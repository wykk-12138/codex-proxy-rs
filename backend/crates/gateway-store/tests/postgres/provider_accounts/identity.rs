//! 验证原生绑定的身份稳定性、凭据原子提交及账号行锁竞争

use gateway_admin::{
    model::{
        client_keys::{DeleteClientKey, NewClientKey},
        provider_credentials::{CredentialRotationCommit, PreparedCredentialRotationFacts},
    },
    ports::store::ClientKeyStore,
};
use gateway_core::{
    account::CredentialIdentityWrite,
    policy::{ClientApiKeyId, CodexAccessBinding},
};
use gateway_store::postgres::PgAdminClientKeyStore;

use super::*;

const ACCOUNT: &str = "acct_codex_identity";
const USER: &str = "codex-user";
const WORKSPACE: &str = "codex-workspace";

fn context() -> MutationContext {
    MutationContext {
        actor: MutationActor::System,
        request_id: "codex-identity".into(),
    }
}

fn key() -> NewClientKey {
    NewClientKey {
        codex_access: Some(CodexAccessBinding {
            enabled: true,
            account_id: ProviderAccountId::new(ACCOUNT).unwrap(),
        }),
        request_profile_overrides: Default::default(),
        id: ClientApiKeyId::new("key_codex_identity").unwrap(),
        name: "Codex identity".into(),
        label: None,
        group_ids: Vec::new(),
        limits: Default::default(),
        budget: Default::default(),
        plaintext: "test-codex-identity".into(),
    }
}

async fn seed(pool: &sqlx::PgPool) {
    let mut account = account(ACCOUNT, USER);
    account.upstream_account_id = Some(WORKSPACE.into());
    PgProviderAccountRepository::new(pool.clone())
        .insert_provider_account(account)
        .await
        .unwrap();
}

fn replacement(user: &str, workspace: &str, revision: u64) -> CredentialRotationCommit {
    CredentialRotationCommit {
        settings: None,
        prepared: PreparedCredentialRotationFacts {
            identity_write: CredentialIdentityWrite::VerifiedReplacement(
                ProviderAccountIdentity::new(user.into(), Some(workspace.into())),
            ),
            account_id: ProviderAccountId::new(ACCOUNT).unwrap(),
            provider_kind: ProviderKind::new("openai").unwrap(),
            expected_credential_revision: gateway_admin::model::Revision::new(revision).unwrap(),
            name: "Replacement".into(),
            email: None,
            plan_type: Some("plus".into()),
            preserve_profile: false,
            preserve_credential_state: false,
            provider_material: ProviderDocument::new(OpaqueProviderData::new(
                json!({"access_token":"replacement-test-only"})
                    .as_object()
                    .unwrap()
                    .clone(),
            )),
            has_refresh_token: false,
            access_token_expires_at: None,
            next_refresh_at: None,
        },
    }
}

async fn persisted(pool: &sqlx::PgPool) -> serde_json::Value {
    sqlx::query_scalar("select jsonb_build_object('account', to_jsonb(a), 'revision', (select config_revision from runtime_settings where id=1), 'audit_count', (select count(*) from admin_audit_events)) from provider_accounts a where a.id=$1")
        .bind(ACCOUNT).fetch_one(pool).await.unwrap()
}

#[tokio::test]
async fn bound_identity_rejects_replacement_and_reauthorization_atomically() {
    let Some(database) = TestDatabase::create("codex_identity_replace").await else {
        return;
    };
    seed(&database.pool).await;
    let keys = PgAdminClientKeyStore::new(database.pool.clone());
    keys.create_client_key(key(), &context()).await.unwrap();
    let store = admin_account_store(&database.pool);
    let before = persisted(&database.pool).await;
    for (user, workspace) in [("other-user", WORKSPACE), (USER, "other-workspace")] {
        assert!(
            store
                .commit_credential_rotation(replacement(user, workspace, 1), &context())
                .await
                .is_err()
        );
        assert_eq!(persisted(&database.pool).await, before);
        let (mut command, authorization_context) =
            admin_adapter::command(admin_adapter::credential("acct_unused", "unused-user"));
        command.pending = PendingAuthorizationMutation::new(
            ProviderKind::new("openai").unwrap(),
            AuthorizationMutationTarget::Reauthorize {
                account_id: ProviderAccountId::new(ACCOUNT).unwrap(),
            },
            AuthorizationOwnerBinding::from_context(&authorization_context),
        );
        command.credential = AuthorizationCredentialCommit::Reauthorize(Box::new(
            replacement(user, workspace, 1).prepared,
        ));
        assert!(
            store
                .commit_authorization(command, &authorization_context)
                .await
                .is_err()
        );
        assert_eq!(persisted(&database.pool).await, before);
    }
    let mut unverified = replacement(USER, WORKSPACE, 1);
    unverified.prepared.identity_write = CredentialIdentityWrite::UnidentifiedReplacement;
    assert!(
        store
            .commit_credential_rotation(unverified, &context())
            .await
            .is_err()
    );
    assert_eq!(persisted(&database.pool).await, before);
    store
        .commit_credential_rotation(replacement(USER, WORKSPACE, 1), &context())
        .await
        .unwrap();
    let repository = PgProviderAccountRepository::new(database.pool.clone());
    let loaded = repository
        .load_current_credential(&ProviderAccountId::new(ACCOUNT).unwrap())
        .await
        .unwrap();
    let refreshed = CredentialCasUpdate::new(
        loaded.account.id().clone(),
        loaded.account.revision(),
        ProviderAccountUpdate {
            account_id: loaded.account.id().clone(),
            name: "refresh".into(),
            email: None,
            plan_type: None,
        },
        PlaintextCredential::new(
            json!({"access_token":"refresh-test-only"})
                .as_object()
                .unwrap()
                .clone(),
        ),
        false,
        None,
        None,
    )
    .unwrap()
    .preserving_profile();
    assert!(matches!(
        repository
            .compare_and_swap_credential(refreshed)
            .await
            .unwrap(),
        CredentialCasOutcome::Updated(_)
    ));
    let account = repository
        .load_current_credential(&ProviderAccountId::new(ACCOUNT).unwrap())
        .await
        .unwrap()
        .account;
    assert_eq!(account.upstream_user_id(), Some(USER));
    assert_eq!(account.upstream_account_id(), Some(WORKSPACE));
    assert!(
        repository
            .delete_provider_accounts_admin(DeleteProviderAccounts {
                scope: ProviderAccountAdminScope {
                    provider_kind: "openai".into()
                },
                account_ids: vec![ACCOUNT.into()],
                audit: audit("codex-delete", "delete", ACCOUNT),
            })
            .await
            .is_err()
    );
    keys.delete_client_key(DeleteClientKey { id: key().id }, &context())
        .await
        .unwrap();
    store
        .commit_credential_rotation(replacement("other-user", WORKSPACE, 3), &context())
        .await
        .unwrap();
    database.close().await;
}

#[tokio::test]
async fn import_uses_new_identity_and_preserves_bound_credentials_on_conflict() {
    let Some(database) = TestDatabase::create("codex_identity_import").await else {
        return;
    };
    seed(&database.pool).await;
    PgAdminClientKeyStore::new(database.pool.clone())
        .create_client_key(key(), &context())
        .await
        .unwrap();
    let repository = PgProviderAccountRepository::new(database.pool.clone());
    let before = persisted(&database.pool).await;
    for (user, workspace) in [("other-user", WORKSPACE), (USER, "other-workspace")] {
        let mut imported = account(ACCOUNT, user);
        imported.upstream_account_id = Some(workspace.into());
        imported.provider_credentials_json = credential_json("import-test-only");
        assert!(
            repository
                .import_provider_accounts(ImportProviderAccounts {
                    settings: None,
                    outbound_proxy: None,
                    scope: ProviderAccountAdminScope {
                        provider_kind: "openai".into()
                    },
                    accounts: vec![imported],
                    audit: audit("codex-import", "import", ACCOUNT),
                })
                .await
                .is_err()
        );
        assert_eq!(persisted(&database.pool).await, before);
    }
    let mut imported = account("acct_import_alias", USER);
    imported.upstream_account_id = Some(WORKSPACE.into());
    imported.provider_credentials_json = credential_json("same-identity-import-test-only");
    let result = repository
        .import_provider_accounts(ImportProviderAccounts {
            settings: None,
            outbound_proxy: None,
            scope: ProviderAccountAdminScope {
                provider_kind: "openai".into(),
            },
            accounts: vec![imported],
            audit: audit("codex-import-same", "import", ACCOUNT),
        })
        .await
        .unwrap();
    assert_eq!(result.account_ids, vec![ACCOUNT]);
    assert_eq!(result.credential_revisions[ACCOUNT].get(), 2);
    database.close().await;
}

#[tokio::test]
async fn binding_committed_first_blocks_a_waiting_identity_replacement() {
    let Some(database) = TestDatabase::create("codex_binding_first").await else {
        return;
    };
    seed(&database.pool).await;
    let keys = PgAdminClientKeyStore::new(database.pool.clone());
    let mut unbound = key();
    unbound.codex_access = None;
    keys.create_client_key(unbound, &context()).await.unwrap();
    let repository = PgProviderAccountRepository::new(database.pool.clone());
    let store = admin_account_store(&database.pool);
    let mut transaction = database.pool.begin().await.unwrap();
    sqlx::query("select id from provider_accounts where id=$1 for update")
        .bind(ACCOUNT)
        .execute(&mut *transaction)
        .await
        .unwrap();
    sqlx::query("insert into client_api_key_codex_bindings (key_id, account_id, enabled) values ($1,$2,true)").bind(key().id.as_str()).bind(ACCOUNT).execute(&mut *transaction).await.unwrap();
    let context = context();
    let pending =
        store.commit_credential_rotation(replacement("other-user", WORKSPACE, 1), &context);
    tokio::pin!(pending);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut pending)
            .await
            .is_err()
    );
    transaction.commit().await.unwrap();
    assert!(pending.await.is_err());
    let account = repository
        .load_current_credential(&ProviderAccountId::new(ACCOUNT).unwrap())
        .await
        .unwrap()
        .account;
    assert_eq!(account.upstream_user_id(), Some(USER));
    assert_eq!(account.revision().get(), 1);
    database.close().await;
}

#[tokio::test]
async fn identity_committed_first_is_rechecked_by_waiting_binding_creation() {
    let Some(database) = TestDatabase::create("codex_identity_first").await else {
        return;
    };
    seed(&database.pool).await;
    let keys = PgAdminClientKeyStore::new(database.pool.clone());
    let mut transaction = database.pool.begin().await.unwrap();
    // 模拟先取得账号锁的身份写入，提交后账号已不具备可绑定的 workspace
    sqlx::query("update provider_accounts set upstream_account_id=null, credential_revision=credential_revision+1 where id=$1").bind(ACCOUNT).execute(&mut *transaction).await.unwrap();
    let context = context();
    let pending = keys.create_client_key(key(), &context);
    tokio::pin!(pending);
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut pending)
            .await
            .is_err()
    );
    transaction.commit().await.unwrap();
    assert!(pending.await.is_err());
    assert!(keys.get_client_key(&key().id).await.unwrap().is_none());
    database.close().await;
}

#[tokio::test]
async fn unidentified_replacement_cannot_lend_old_identity_to_a_new_binding() {
    let Some(database) = TestDatabase::create("codex_unidentified_replacement").await else {
        return;
    };
    seed(&database.pool).await;
    let store = admin_account_store(&database.pool);
    let mut command = replacement(USER, WORKSPACE, 1);
    command.prepared.identity_write = CredentialIdentityWrite::UnidentifiedReplacement;
    store
        .commit_credential_rotation(command, &context())
        .await
        .unwrap();
    let account = PgProviderAccountRepository::new(database.pool.clone())
        .load_current_credential(&ProviderAccountId::new(ACCOUNT).unwrap())
        .await
        .unwrap()
        .account;
    assert!(account.upstream_user_id().is_none());
    assert!(account.upstream_account_id().is_none());
    assert!(
        PgAdminClientKeyStore::new(database.pool.clone())
            .create_client_key(key(), &context())
            .await
            .is_err()
    );
    database.close().await;
}
