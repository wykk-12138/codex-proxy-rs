//! 账号行锁内的凭据身份与 Codex 持久绑定约束

use super::*;

/// 绑定创建与凭据提交共同锁定账号行，锁内再核对 revision、身份和当前绑定
pub(super) async fn validate_credential_identity_in_transaction(
    transaction: &mut Transaction<'_, Postgres>,
    account_id: &str,
    expected_revision: Revision,
    identity_write: &gateway_core::account::CredentialIdentityWrite,
) -> StoreResult<()> {
    use gateway_core::account::CredentialIdentityWrite;
    let current: Option<(i64, Option<String>, Option<String>)> = sqlx::query_as(
        "select credential_revision, upstream_user_id, upstream_account_id
         from provider_accounts where id = $1 for update",
    )
    .bind(account_id)
    .fetch_optional(&mut **transaction)
    .await
    .map_err(|source| postgres_unavailable("lock credential identity", source))?;
    let Some((revision, user, workspace)) = current else {
        return Err(credential_identity_conflict(
            account_id,
            ConflictKind::StaleRevision,
        ));
    };
    if to_u64(revision)? != expected_revision.get() {
        return Err(credential_identity_conflict(
            account_id,
            ConflictKind::StaleRevision,
        ));
    }
    let changed = match identity_write {
        CredentialIdentityWrite::RuntimeMaintenance
        | CredentialIdentityWrite::ConnectionSettings => false,
        CredentialIdentityWrite::VerifiedReplacement(identity) => {
            require_nonempty(ENTITY, "upstream_user_id", identity.upstream_user_id())?;
            if let Some(workspace) = identity.upstream_account_id() {
                require_nonempty(ENTITY, "upstream_account_id", workspace)?;
            }
            user.as_deref() != Some(identity.upstream_user_id())
                || workspace.as_deref() != identity.upstream_account_id()
        }
        CredentialIdentityWrite::UnidentifiedReplacement => true,
    };
    if changed {
        let bound: bool = sqlx::query_scalar(
            "select exists(select 1 from client_api_key_codex_bindings where account_id = $1)",
        )
        .bind(account_id)
        .fetch_one(&mut **transaction)
        .await
        .map_err(|source| postgres_unavailable("read credential identity binding", source))?;
        if bound {
            return Err(credential_identity_conflict(
                account_id,
                ConflictKind::InvalidTransition,
            ));
        }
    }
    Ok(())
}

fn credential_identity_conflict(account_id: &str, kind: ConflictKind) -> StoreError {
    StoreError::Conflict {
        source: None,
        entity: ENTITY,
        id: account_id.to_owned(),
        kind,
    }
}

pub(super) fn map_account_delete_error(source: sqlx::Error) -> StoreError {
    if source.as_database_error().is_some_and(|error| {
        error.is_foreign_key_violation()
            && error.constraint() == Some("client_api_key_codex_bindings_account_id_fkey")
    }) {
        return StoreError::Conflict {
            source: Some(source.into()),
            entity: ENTITY,
            id: "Codex-bound account".to_owned(),
            kind: ConflictKind::InvalidTransition,
        };
    }
    postgres_unavailable("delete provider account", source)
}
