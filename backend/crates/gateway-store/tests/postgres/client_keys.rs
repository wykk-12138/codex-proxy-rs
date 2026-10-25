//! 验证 Client Key 持久化的唯一性、原值保留与搜索脱敏

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use chrono::{TimeZone as _, Utc};
use gateway_admin::{
    model::client_keys::{
        ClientKeyListQuery as AdminClientKeyListQuery, ClientKeyPageSize,
        ClientKeySort as AdminClientKeySort, ClientKeySortField as AdminClientKeySortField,
        SortDirection as AdminSortDirection,
    },
    ports::store::ClientKeyStore as _,
};
use gateway_core::{
    engine::execution::ClientApiKeyUsageSink as _,
    lifecycle::CancellationToken,
    policy::ClientApiKeyId,
    task::{DaemonTask as _, WorkerDaemonContext},
};
use gateway_store::postgres::{
    ClientApiKeyCursor, ClientApiKeyCursorValue, ClientApiKeyListQuery, ClientApiKeyRepository,
    ClientApiKeySort, ClientApiKeySortDirection, ClientApiKeySortField, NewClientApiKey,
    PgAdminClientKeyStore, PgClientApiKeyRepository, PgClientApiKeyUsageSink,
};

use super::TestDatabase;

#[tokio::test]
async fn migrated_keys_persist_exactly_and_keep_short_keys_masked() {
    use gateway_admin::model::{MutationActor, MutationContext, client_keys::NewClientKey};
    use gateway_core::policy::RateLimits;
    use gateway_store::postgres::{PgRuntimeSnapshotRepository, RuntimeSnapshotRepository};

    let Some(database) = TestDatabase::create("migrated_keys").await else {
        return;
    };
    let store = PgAdminClientKeyStore::new(database.pool.clone());
    let context = MutationContext {
        actor: MutationActor::System,
        request_id: "migration-test".to_owned(),
    };
    let long_key: String = (0..160)
        .map(|_| uuid::Uuid::new_v4().simple().to_string())
        .collect();
    let values = [
        "q".to_owned(),
        "abc".to_owned(),
        "sk-legacy/key+value=:!@\\".to_owned(),
        long_key,
    ];
    for (index, key) in values.iter().enumerate() {
        let id = ClientApiKeyId::new(format!("key_migrated_{index}")).unwrap();
        let (_, record) = store
            .create_client_key(
                NewClientKey {
                    codex_access: None,
                    request_profile_overrides: Default::default(),
                    id: id.clone(),
                    name: format!("Migrated {index}"),
                    label: None,
                    group_ids: Vec::new(),
                    limits: RateLimits {
                        max_concurrency: 3,
                        requests_per_minute: 25,
                    },
                    budget: Default::default(),
                    plaintext: key.clone(),
                },
                &context,
            )
            .await
            .unwrap();
        assert_eq!(record.prefix.len(), 10.min(key.len() / 2));
        assert_ne!(record.prefix, *key);
        let revealed = store.reveal_client_key(&id).await.unwrap().unwrap();
        assert_eq!(revealed.expose_for_response(), key);
    }
    let snapshot = PgRuntimeSnapshotRepository::new(database.pool.clone())
        .load_runtime_snapshot()
        .await
        .unwrap();
    assert_eq!(snapshot.client_api_keys.len(), values.len());
    for key in &values {
        let record = snapshot
            .client_api_keys
            .iter()
            .find(|item| item.plaintext_key.expose_for_auth() == key)
            .unwrap();
        assert_eq!(record.limits.max_concurrency, 3);
    }
    database.close().await;
}

#[tokio::test]
async fn duplicate_migrated_keys_conflict_atomically_without_extra_audits() {
    use gateway_admin::{
        model::{MutationActor, MutationContext, client_keys::NewClientKey},
        ports::store::AdminStoreErrorKind,
    };
    use gateway_core::policy::RateLimits;

    let Some(database) = TestDatabase::create("duplicate_keys").await else {
        return;
    };
    let store = PgAdminClientKeyStore::new(database.pool.clone());
    let context = MutationContext {
        actor: MutationActor::System,
        request_id: "duplicate-test".to_owned(),
    };
    let key = "legacy-key-case-sensitive!";
    let command = |id: &str| NewClientKey {
        codex_access: None,
        request_profile_overrides: Default::default(),
        id: ClientApiKeyId::new(id).unwrap(),
        name: id.to_owned(),
        label: None,
        group_ids: Vec::new(),
        limits: RateLimits::unlimited(),
        budget: Default::default(),
        plaintext: key.to_owned(),
    };
    let (first, second) = tokio::join!(
        store.create_client_key(command("key_first"), &context),
        store.create_client_key(command("key_second"), &context)
    );
    assert_ne!(first.is_ok(), second.is_ok());
    let error = first.err().or_else(|| second.err()).unwrap();
    assert_eq!(error.kind(), AdminStoreErrorKind::Conflict);
    assert!(!format!("{error:?}").contains(key));
    let counts: (i64, i64) = sqlx::query_as(
        "select (select count(*) from client_api_keys), (select count(*) from admin_audit_events)",
    )
    .fetch_one(&database.pool)
    .await
    .unwrap();
    assert_eq!(counts, (1, 1));
    let mut different_case = command("key_case_sensitive");
    different_case.plaintext = key.to_uppercase();
    store
        .create_client_key(different_case, &context)
        .await
        .unwrap();
    database.close().await;
}

#[test]
fn generated_client_key_format_remains_valid() {
    let key = NewClientApiKey {
        codex_access: None,
        request_profile_overrides: Default::default(),
        budget: Default::default(),
        id: "key-1".to_owned(),
        name: "default".to_owned(),
        label: None,
        group_ids: Vec::new(),
        key: format!("sk_{}", "a".repeat(43)),
        max_concurrency: 0,
        requests_per_minute: 0,
    };
    assert!(key.validate().is_ok());
}

#[tokio::test]
async fn client_key_names_are_checked_atomically_on_create_and_rename() {
    use gateway_admin::{
        model::{
            MutationActor, MutationContext,
            client_keys::{NewClientKey, UpdateClientKey},
        },
        ports::store::AdminStoreErrorKind,
    };
    use gateway_core::policy::RateLimits;

    let Some(database) = TestDatabase::create("duplicate_key_names").await else {
        return;
    };
    let store = PgAdminClientKeyStore::new(database.pool.clone());
    let context = MutationContext {
        actor: MutationActor::System,
        request_id: "duplicate-name-test".to_owned(),
    };
    let create = |id: &str, name: &str| NewClientKey {
        codex_access: None,
        request_profile_overrides: Default::default(),
        id: ClientApiKeyId::new(id).unwrap(),
        name: name.to_owned(),
        label: None,
        group_ids: Vec::new(),
        limits: RateLimits::unlimited(),
        budget: Default::default(),
        plaintext: format!("synthetic-credential-{id}"),
    };
    let update = |id: ClientApiKeyId, name: &str| UpdateClientKey {
        codex_access: None,
        request_profile_override_updates: Default::default(),
        id,
        name: name.to_owned(),
        label: None,
        group_ids: Vec::new(),
        limits: RateLimits::unlimited(),
        daily_limit_usd: None,
        weekly_limit_usd: None,
    };
    let (first, second) = tokio::join!(
        store.create_client_key(create("key_name_first", "Production"), &context),
        store.create_client_key(create("key_name_second", " production "), &context),
    );
    let (winner, conflict) = match (first, second) {
        (Ok((_, record)), Err(error)) | (Err(error), Ok((_, record))) => (record, error),
        results => panic!("expected one committed create: {results:?}"),
    };
    assert_eq!(conflict.kind(), AdminStoreErrorKind::DuplicateName);
    assert_eq!(winner.name.trim(), winner.name);
    let counts: (i64, i64) = sqlx::query_as(
        "select (select count(*) from client_api_keys), (select count(*) from admin_audit_events)",
    )
    .fetch_one(&database.pool)
    .await
    .unwrap();
    assert_eq!(counts, (1, 1));

    let (revision, _) = store
        .update_client_key(update(winner.id.clone(), "PRODUCTION"), &context)
        .await
        .unwrap();
    let (_, other) = store
        .create_client_key(create("key_name_other", "Staging"), &context)
        .await
        .unwrap();
    let error = store
        .update_client_key(update(other.id.clone(), " production "), &context)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), AdminStoreErrorKind::DuplicateName);
    let unchanged = store.reveal_client_key(&other.id).await.unwrap().unwrap();
    assert_eq!(unchanged.record.name, "Staging");
    let persisted_revision: i64 =
        sqlx::query_scalar("select config_revision from runtime_settings where id = 1")
            .fetch_one(&database.pool)
            .await
            .unwrap();
    assert_eq!(persisted_revision as u64, revision.get() + 1);

    let (first, second) = tokio::join!(
        store.update_client_key(update(winner.id, "Shared"), &context),
        store.update_client_key(update(other.id, "SHARED"), &context),
    );
    assert_ne!(first.is_ok(), second.is_ok());
    assert_eq!(
        first.err().or_else(|| second.err()).unwrap().kind(),
        AdminStoreErrorKind::DuplicateName
    );
    database.close().await;
}

#[tokio::test]
async fn client_key_search_matches_names_and_labels_but_never_credential_values() {
    let Some(database) = TestDatabase::create("key_name_search").await else {
        return;
    };
    let generated = format!("sk_{}", "K".repeat(43));
    for (id, name, key) in [
        ("key_generated", "Production", generated.as_str()),
        ("key_custom", "生产_%专用", "legacy-token+/123"),
    ] {
        sqlx::query(
            "insert into client_api_keys (id, name, label, key, created_at, updated_at)
            values ($1, $2, 'backend label', $3, now(), now())",
        )
        .bind(id)
        .bind(name)
        .bind(key)
        .execute(&database.pool)
        .await
        .unwrap();
    }
    let repository = PgClientApiKeyRepository::new(database.pool.clone());
    for (search, expected) in [
        ("PROD", 1),
        ("生产_%", 1),
        ("生产X", 0),
        ("backend", 2),
        (generated.as_str(), 0),
        (&generated[..10], 0),
        ("legacy-token+/123", 0),
        ("legacy-", 0),
    ] {
        let page = repository
            .list_client_api_keys(ClientApiKeyListQuery {
                cursor: None,
                page_size: 10,
                search: Some(search.to_owned()),
                sort: ClientApiKeySort::default(),
            })
            .await
            .unwrap();
        assert_eq!(page.total, expected, "search: {search}");
        assert_eq!(page.items.len() as u64, expected);
    }
    database.close().await;
}

#[tokio::test]
async fn client_key_list_uses_safe_keyset_search_and_filtered_total() {
    let Some(database) = TestDatabase::create("client_key_page").await else {
        return;
    };
    for (index, label) in ["alpha", "needle", "omega"].into_iter().enumerate() {
        let id = format!("key_page_{index}");
        let key = format!(
            "sk_{}",
            char::from(b'a' + index as u8).to_string().repeat(43)
        );
        sqlx::query(
            "insert into client_api_keys (
               id, name, label, key, enabled, max_concurrency, requests_per_minute,
               created_at, updated_at
             ) values ($1, $2, $2, $3, true, 0, 0,
                       now() - ($4::bigint * interval '1 minute'), now())",
        )
        .bind(id)
        .bind(label)
        .bind(key)
        .bind(index as i64)
        .execute(&database.pool)
        .await
        .expect("seed paged client key");
    }
    let repository = PgClientApiKeyRepository::new(database.pool.clone());
    let first = repository
        .list_client_api_keys(ClientApiKeyListQuery {
            cursor: None,
            page_size: 2,
            search: None,
            sort: ClientApiKeySort::default(),
        })
        .await
        .expect("first client key page");
    assert_eq!(first.total, 3);
    assert_eq!(first.items.len(), 2);
    assert!(first.next_cursor.is_some());
    let second = repository
        .list_client_api_keys(ClientApiKeyListQuery {
            cursor: first.next_cursor,
            page_size: 2,
            search: None,
            sort: ClientApiKeySort::default(),
        })
        .await
        .expect("second client key page");
    assert_eq!(second.total, 3);
    assert_eq!(second.items.len(), 1);

    let searched = repository
        .list_client_api_keys(ClientApiKeyListQuery {
            cursor: None,
            page_size: 10,
            search: Some("needle".to_owned()),
            sort: ClientApiKeySort::default(),
        })
        .await
        .expect("searched client key page");
    assert_eq!(searched.total, 1);
    assert_eq!(searched.items[0].label.as_deref(), Some("needle"));
    assert_eq!(searched.items[0].prefix.len(), 10);
    database.close().await;
}

#[test]
fn client_key_cursor_is_bound_to_one_sort_contract() {
    let created_sort = ClientApiKeySort::default();
    assert!(
        ClientApiKeyListQuery {
            cursor: None,
            page_size: u16::MAX,
            search: None,
            sort: created_sort,
        }
        .validate()
        .is_ok()
    );
    let cursor = ClientApiKeyCursor::new(
        created_sort,
        ClientApiKeyCursorValue::CreatedAt(chrono::Utc::now()),
        "key-cursor",
    )
    .expect("valid cursor");
    let query = ClientApiKeyListQuery {
        cursor: Some(cursor),
        page_size: 10,
        search: None,
        sort: ClientApiKeySort {
            field: ClientApiKeySortField::Name,
            direction: ClientApiKeySortDirection::Asc,
        },
    };
    assert!(query.validate().is_err());
    assert!(
        ClientApiKeyCursor::new(
            created_sort,
            ClientApiKeyCursorValue::Enabled(true),
            "key-cursor",
        )
        .is_err()
    );
}

#[tokio::test]
async fn admin_client_key_adapter_should_preserve_the_full_nonzero_u16_page_size() {
    let Some(database) = TestDatabase::create("admin_client_key_max_page").await else {
        return;
    };
    let page = PgAdminClientKeyStore::new(database.pool.clone())
        .list_client_keys(AdminClientKeyListQuery {
            cursor: None,
            page_size: ClientKeyPageSize::new(u16::MAX).expect("maximum page size"),
            search: None,
            sort: AdminClientKeySort {
                field: AdminClientKeySortField::CreatedAt,
                direction: AdminSortDirection::Desc,
            },
        })
        .await
        .expect("maximum Client Key page size");

    assert_eq!(page.total, 0);
    assert!(page.items.is_empty());
    database.close().await;
}

#[tokio::test]
async fn client_key_database_sort_is_stable_and_keeps_null_last_used_at_last() {
    let Some(database) = TestDatabase::create("client_key_sort").await else {
        return;
    };
    for (index, (id, name, enabled, created_at, last_used_at)) in [
        ("key_sort_a", "Zulu", false, "2026-01-01T00:00:00Z", None),
        (
            "key_sort_b",
            "alpha",
            true,
            "2026-01-02T00:00:00Z",
            Some("2026-01-03T00:00:00Z"),
        ),
        (
            "key_sort_c",
            "Beta",
            false,
            "2026-01-03T00:00:00Z",
            Some("2026-01-01T00:00:00Z"),
        ),
    ]
    .into_iter()
    .enumerate()
    {
        sqlx::query(
            "insert into client_api_keys (
               id, name, key, enabled, max_concurrency, requests_per_minute,
               last_used_at, created_at, updated_at
             ) values ($1, $2, $3, $4, 0, 0, $5::timestamptz, $6::timestamptz,
                       $6::timestamptz)",
        )
        .bind(id)
        .bind(name)
        .bind(format!(
            "sk_{}",
            char::from(b'a' + index as u8).to_string().repeat(43)
        ))
        .bind(enabled)
        .bind(last_used_at)
        .bind(created_at)
        .execute(&database.pool)
        .await
        .expect("seed sorted client key");
    }
    let repository = PgClientApiKeyRepository::new(database.pool.clone());
    for (sort, expected) in [
        (
            ClientApiKeySort {
                field: ClientApiKeySortField::Name,
                direction: ClientApiKeySortDirection::Asc,
            },
            vec!["key_sort_b", "key_sort_c", "key_sort_a"],
        ),
        (
            ClientApiKeySort {
                field: ClientApiKeySortField::Enabled,
                direction: ClientApiKeySortDirection::Desc,
            },
            vec!["key_sort_b", "key_sort_c", "key_sort_a"],
        ),
        (
            ClientApiKeySort {
                field: ClientApiKeySortField::CreatedAt,
                direction: ClientApiKeySortDirection::Desc,
            },
            vec!["key_sort_c", "key_sort_b", "key_sort_a"],
        ),
        (
            ClientApiKeySort {
                field: ClientApiKeySortField::LastUsedAt,
                direction: ClientApiKeySortDirection::Asc,
            },
            vec!["key_sort_c", "key_sort_b", "key_sort_a"],
        ),
        (
            ClientApiKeySort {
                field: ClientApiKeySortField::LastUsedAt,
                direction: ClientApiKeySortDirection::Desc,
            },
            vec!["key_sort_b", "key_sort_c", "key_sort_a"],
        ),
    ] {
        let mut cursor = None;
        let mut ids = Vec::new();
        loop {
            let page = repository
                .list_client_api_keys(ClientApiKeyListQuery {
                    cursor,
                    page_size: 1,
                    search: None,
                    sort,
                })
                .await
                .expect("sorted client key page");
            ids.extend(page.items.into_iter().map(|item| item.id));
            let Some(next) = page.next_cursor else {
                break;
            };
            cursor = Some(next);
        }
        assert_eq!(ids, expected);
    }
    database.close().await;
}

#[tokio::test]
async fn client_key_usage_touch_should_preserve_the_latest_observed_timestamp() {
    let Some(database) = TestDatabase::create("client_key_usage_touch").await else {
        return;
    };
    sqlx::query(
        "insert into client_api_keys (
           id, name, key, enabled, max_concurrency, requests_per_minute,
           created_at, updated_at
         ) values ('key_usage_touch', 'usage', $1, true, 0, 0, now(), now())",
    )
    .bind(format!("sk_{}", "t".repeat(43)))
    .execute(&database.pool)
    .await
    .expect("seed client key");
    let repository = PgClientApiKeyRepository::new(database.pool.clone());
    let first = Utc
        .with_ymd_and_hms(2026, 7, 24, 0, 0, 0)
        .single()
        .expect("first timestamp");
    let latest = Utc
        .with_ymd_and_hms(2026, 7, 24, 0, 0, 1)
        .single()
        .expect("latest timestamp");
    repository
        .touch_client_api_keys(&BTreeMap::from([("key_usage_touch".to_owned(), first)]))
        .await
        .expect("record first API Key use");
    repository
        .touch_client_api_keys(&BTreeMap::from([("key_usage_touch".to_owned(), latest)]))
        .await
        .expect("record latest API Key use");

    let record = repository
        .get_client_api_key("key_usage_touch")
        .await
        .expect("load API Key")
        .expect("client key exists");
    assert_eq!(record.last_used_at, Some(latest));
    database.close().await;
}

#[tokio::test]
async fn client_key_usage_daemon_should_flush_pending_touch_on_shutdown() {
    let Some(database) = TestDatabase::create("client_key_usage_daemon").await else {
        return;
    };
    sqlx::query(
        "insert into client_api_keys (
           id, name, key, enabled, max_concurrency, requests_per_minute,
           created_at, updated_at
         ) values ('key_usage_daemon', 'usage', $1, true, 0, 0, now(), now())",
    )
    .bind(format!("sk_{}", "u".repeat(43)))
    .execute(&database.pool)
    .await
    .expect("seed client key");

    let (sink, writer) =
        PgClientApiKeyUsageSink::with_flush_delay(database.pool.clone(), Duration::from_secs(60));
    let writer = Arc::new(writer);
    let cancellation = CancellationToken::new();
    let task = {
        let writer = Arc::clone(&writer);
        let cancellation = cancellation.clone();
        tokio::spawn(async move { writer.run(WorkerDaemonContext::new(cancellation)).await })
    };
    sink.record_used(&ClientApiKeyId::new("key_usage_daemon").expect("valid client API Key ID"));
    cancellation.cancel();
    task.await
        .expect("join client key usage daemon")
        .expect("stop client key usage daemon");

    let last_used_at: Option<chrono::DateTime<Utc>> = sqlx::query_scalar(
        "select last_used_at from client_api_keys where id = 'key_usage_daemon'",
    )
    .fetch_one(&database.pool)
    .await
    .expect("load client key last-used timestamp");
    assert!(last_used_at.is_some());
    database.close().await;
}

#[tokio::test]
async fn dedicated_reveal_returns_plaintext_without_debug_exposure() {
    let Some(database) = TestDatabase::create("client_key_reveal").await else {
        return;
    };
    let plaintext = format!("sk_{}", "r".repeat(43));
    sqlx::query(
        "insert into client_api_keys (
           id, name, key, enabled, max_concurrency, requests_per_minute,
           created_at, updated_at
         ) values ('key_reveal', 'reveal', $1, true, 1, 2, now(), now())",
    )
    .bind(&plaintext)
    .execute(&database.pool)
    .await
    .expect("seed revealed client key");
    let revealed = PgClientApiKeyRepository::new(database.pool.clone())
        .reveal_client_api_key("key_reveal")
        .await
        .expect("reveal client key")
        .expect("revealed key exists");
    assert_eq!(revealed.key, plaintext);
    assert!(!format!("{revealed:?}").contains(&plaintext));
    database.close().await;
}

#[test]
fn client_key_debug_redacts_plaintext() {
    let secret = format!("sk_{}", "s".repeat(43));
    let key = NewClientApiKey {
        codex_access: None,
        request_profile_overrides: Default::default(),
        budget: Default::default(),
        id: "key-1".to_owned(),
        name: "default".to_owned(),
        label: None,
        group_ids: Vec::new(),
        key: secret.clone(),
        max_concurrency: 0,
        requests_per_minute: 0,
    };
    assert!(!format!("{key:?}").contains(&secret));
}

#[tokio::test]
async fn session_key_status_checks_the_exact_current_enabled_record() {
    let Some(database) = TestDatabase::create("session_key_status").await else {
        return;
    };
    let store = PgAdminClientKeyStore::new(database.pool.clone());
    let key = ClientApiKeyId::new("key-session-test").unwrap();
    assert!(!store.is_enabled(&key).await.unwrap());
    sqlx::query("insert into client_api_keys (id, name, key, enabled, created_at, updated_at) values ($1, 'Session test', 'synthetic-session-status-key', true, now(), now())")
        .bind(key.as_str()).execute(&database.pool).await.unwrap();
    assert!(store.is_enabled(&key).await.unwrap());
    assert!(
        !store
            .is_enabled(&ClientApiKeyId::new("other-key").unwrap())
            .await
            .unwrap()
    );
    sqlx::query("update client_api_keys set enabled = false where id = $1")
        .bind(key.as_str())
        .execute(&database.pool)
        .await
        .unwrap();
    assert!(!store.is_enabled(&key).await.unwrap());
    sqlx::query("delete from client_api_keys where id = $1")
        .bind(key.as_str())
        .execute(&database.pool)
        .await
        .unwrap();
    assert!(!store.is_enabled(&key).await.unwrap());
    database.close().await;
}

#[tokio::test]
async fn key_profile_override_roundtrips_and_explicit_clear_restores_inheritance() {
    use gateway_admin::model::{
        MutationActor, MutationContext,
        client_keys::{NewClientKey, UpdateClientKey},
    };
    use gateway_core::{account::OpaqueProviderData, policy::RateLimits};
    use gateway_store::postgres::{PgRuntimeSnapshotRepository, RuntimeSnapshotRepository};
    let Some(database) = TestDatabase::create("key_profiles").await else {
        return;
    };
    let store = PgAdminClientKeyStore::new(database.pool.clone());
    let context = MutationContext {
        actor: MutationActor::System,
        request_id: "profile-test".to_owned(),
    };
    let profile = OpaqueProviderData::new(
        serde_json::json!({"client":"cli","platform":"linux","versionMode":"latest"})
            .as_object()
            .unwrap()
            .clone(),
    );
    let id = ClientApiKeyId::new("key_profile").unwrap();
    let openai = gateway_core::routing::ProviderKind::new("openai").unwrap();
    let xai = gateway_core::routing::ProviderKind::new("xai").unwrap();
    let (_, record) = store
        .create_client_key(
            NewClientKey {
                codex_access: None,
                id: id.clone(),
                name: "profile".to_owned(),
                label: None,
                group_ids: vec![],
                limits: RateLimits::unlimited(),
                budget: Default::default(),
                plaintext: "synthetic-profile-test-key".to_owned(),
                request_profile_overrides: BTreeMap::from([
                    (openai.clone(), profile.clone()),
                    (xai.clone(), profile.clone()),
                ]),
            },
            &context,
        )
        .await
        .unwrap();
    assert_eq!(
        record.request_profile_overrides.get(&openai),
        Some(&profile)
    );
    assert_eq!(record.request_profile_overrides.get(&xai), Some(&profile));
    let update = |override_value: Option<Option<OpaqueProviderData>>| UpdateClientKey {
        codex_access: None,
        id: id.clone(),
        name: "profile".to_owned(),
        label: None,
        group_ids: vec![],
        limits: RateLimits::unlimited(),
        daily_limit_usd: None,
        weekly_limit_usd: None,
        request_profile_override_updates: override_value
            .map(|profile| BTreeMap::from([(openai.clone(), profile)]))
            .unwrap_or_default(),
    };
    let (_, unchanged) = store
        .update_client_key(update(None), &context)
        .await
        .unwrap();
    assert_eq!(
        unchanged.request_profile_overrides.get(&openai),
        Some(&profile)
    );
    assert_eq!(
        unchanged.request_profile_overrides.get(&xai),
        Some(&profile)
    );
    let snapshot = PgRuntimeSnapshotRepository::new(database.pool.clone())
        .load_runtime_snapshot()
        .await
        .unwrap();
    assert_eq!(
        snapshot.client_api_keys[0].request_profiles.values().next(),
        Some(&profile)
    );
    assert_eq!(
        snapshot.client_api_keys[0].request_profiles.get(&xai),
        Some(&profile)
    );
    let mut clear_xai = update(None);
    clear_xai
        .request_profile_override_updates
        .insert(xai.clone(), None);
    let (_, cleared_xai) = store.update_client_key(clear_xai, &context).await.unwrap();
    assert!(!cleared_xai.request_profile_overrides.contains_key(&xai));
    assert_eq!(
        cleared_xai.request_profile_overrides.get(&openai),
        Some(&profile)
    );
    let (_, cleared) = store
        .update_client_key(update(Some(None)), &context)
        .await
        .unwrap();
    assert!(!cleared.request_profile_overrides.contains_key(&openai));
    let snapshot = PgRuntimeSnapshotRepository::new(database.pool.clone())
        .load_runtime_snapshot()
        .await
        .unwrap();
    assert!(snapshot.client_api_keys[0].request_profiles.is_empty());
    database.close().await;
}

#[tokio::test]
async fn codex_binding_is_persistent_immutable_and_included_in_snapshots() {
    use gateway_admin::model::{
        MutationActor, MutationContext,
        client_keys::{DeleteClientKey, NewClientKey, UpdateClientKey},
    };
    use gateway_core::{
        account::ProviderAccountId,
        policy::{CodexAccessBinding, CodexAccessMode, CodexAccessUpdate},
    };
    use gateway_store::postgres::{
        PgProviderAccountRepository, PgRuntimeSnapshotRepository, ProviderAccountRepository,
        RuntimeSnapshotRepository,
    };
    let Some(database) = TestDatabase::create("codex_key_binding").await else {
        return;
    };
    let accounts = PgProviderAccountRepository::new(database.pool.clone());
    for id in ["acct_codex_a", "acct_codex_b"] {
        let mut account = super::provider_accounts::account(id, id);
        account.upstream_account_id = Some(format!("workspace-{id}"));
        accounts.insert_provider_account(account).await.unwrap();
    }
    let keys = PgAdminClientKeyStore::new(database.pool.clone());
    let context = MutationContext {
        actor: MutationActor::System,
        request_id: "codex-key".into(),
    };
    let id = ClientApiKeyId::new("key_codex").unwrap();
    let (_, record) = keys
        .create_client_key(
            NewClientKey {
                codex_access: None,
                request_profile_overrides: Default::default(),
                id: id.clone(),
                name: "Codex".into(),
                label: None,
                group_ids: Vec::new(),
                limits: Default::default(),
                budget: Default::default(),
                plaintext: "test-codex-key".into(),
            },
            &context,
        )
        .await
        .unwrap();
    assert!(record.codex_access.is_none());
    let binding = CodexAccessBinding {
        enabled: true,
        mode: CodexAccessMode::Fixed,
        account_id: ProviderAccountId::new("acct_codex_a").unwrap(),
    };
    // 更新输入省略 mode 时保留既有模式，旧客户端不会被误解为切回 fixed
    let update_binding = |binding: &CodexAccessBinding| CodexAccessUpdate {
        enabled: binding.enabled,
        mode: Some(binding.mode),
        account_id: binding.account_id.clone(),
    };
    let update = |codex_access| UpdateClientKey {
        codex_access,
        request_profile_override_updates: Default::default(),
        id: id.clone(),
        name: "Codex".into(),
        label: None,
        group_ids: Vec::new(),
        limits: Default::default(),
        daily_limit_usd: None,
        weekly_limit_usd: None,
    };
    keys.update_client_key(update(Some(update_binding(&binding))), &context)
        .await
        .unwrap();
    keys.update_client_key(update(None), &context)
        .await
        .unwrap();
    let record = keys.get_client_key(&id).await.unwrap().unwrap();
    assert_eq!(record.codex_access, Some(binding.clone()));
    let snapshot = PgRuntimeSnapshotRepository::new(database.pool.clone())
        .load_runtime_snapshot()
        .await
        .unwrap();
    assert_eq!(
        snapshot
            .client_api_keys
            .iter()
            .find(|key| key.id == id)
            .unwrap()
            .codex_access,
        Some(binding.clone())
    );
    let disabled = CodexAccessBinding {
        enabled: false,
        ..binding.clone()
    };
    keys.update_client_key(update(Some(update_binding(&disabled))), &context)
        .await
        .unwrap();
    let before: (i64, i64) = sqlx::query_as("select config_revision, (select count(*) from admin_audit_events) from runtime_settings where id=1").fetch_one(&database.pool).await.unwrap();
    assert!(
        keys.update_client_key(
            update(Some(CodexAccessUpdate {
                enabled: true,
                mode: Some(CodexAccessMode::Fixed),
                account_id: ProviderAccountId::new("acct_codex_b").unwrap()
            })),
            &context
        )
        .await
        .is_err()
    );
    let after: (i64, i64) = sqlx::query_as("select config_revision, (select count(*) from admin_audit_events) from runtime_settings where id=1").fetch_one(&database.pool).await.unwrap();
    assert_eq!(before, after);
    assert_eq!(
        keys.get_client_key(&id)
            .await
            .unwrap()
            .unwrap()
            .codex_access,
        Some(disabled)
    );
    keys.delete_client_key(DeleteClientKey { id }, &context)
        .await
        .unwrap();
    let count: i64 = sqlx::query_scalar("select count(*) from client_api_key_codex_bindings")
        .fetch_one(&database.pool)
        .await
        .unwrap();
    assert_eq!(count, 0);
    database.close().await;
}

#[tokio::test]
async fn codex_binding_checks_scope_on_creation_and_keeps_existing_switch_editable() {
    use gateway_admin::model::{
        MutationActor, MutationContext,
        client_keys::{NewClientKey, UpdateClientKey},
    };
    use gateway_core::{
        account::ProviderAccountId,
        policy::{CodexAccessBinding, CodexAccessMode, CodexAccessUpdate},
        routing::AccountGroupId,
    };
    use gateway_store::postgres::{PgProviderAccountRepository, ProviderAccountRepository};
    let Some(database) = TestDatabase::create("codex_key_scope").await else {
        return;
    };
    let repository = PgProviderAccountRepository::new(database.pool.clone());
    for (id, provider, authentication) in [
        ("acct_codex_oauth", "openai", "oauth"),
        ("acct_codex_api", "openai", "api_key"),
        ("acct_codex_xai", "xai", "oauth"),
    ] {
        let mut account = super::provider_accounts::account(id, id);
        account.provider_kind = provider.into();
        account.authentication_kind = authentication.into();
        account.upstream_account_id = Some(format!("workspace-{id}"));
        repository.insert_provider_account(account).await.unwrap();
    }
    let group_id = "grp_00000000000000000000000000000004";
    sqlx::query("insert into account_groups (id,name,enabled,created_at,updated_at,color) values ($1,'Codex scope',false,now(),now(),'#FFFFFFFF')").bind(group_id).execute(&database.pool).await.unwrap();
    sqlx::query("insert into account_group_accounts (account_group_id,provider_account_id,created_at) values ($1,'acct_codex_oauth',now())").bind(group_id).execute(&database.pool).await.unwrap();
    let keys = PgAdminClientKeyStore::new(database.pool.clone());
    let context = MutationContext {
        actor: MutationActor::System,
        request_id: "codex-scope".into(),
    };
    let command = |account: &str, group_ids| NewClientKey {
        codex_access: Some(CodexAccessBinding {
            enabled: true,
            mode: CodexAccessMode::Fixed,
            account_id: ProviderAccountId::new(account).unwrap(),
        }),
        request_profile_overrides: Default::default(),
        id: ClientApiKeyId::new("key_scope").unwrap(),
        name: "Codex scope".into(),
        label: None,
        group_ids,
        limits: Default::default(),
        budget: Default::default(),
        plaintext: "test-codex-scope".into(),
    };
    for account in ["acct_missing", "acct_codex_api", "acct_codex_xai"] {
        assert!(
            keys.create_client_key(command(account, Vec::new()), &context)
                .await
                .is_err()
        );
    }
    assert!(
        keys.create_client_key(
            command(
                "acct_codex_oauth",
                vec![AccountGroupId::new(group_id).unwrap()]
            ),
            &context
        )
        .await
        .is_err()
    );
    sqlx::query("update account_groups set enabled=true where id=$1")
        .bind(group_id)
        .execute(&database.pool)
        .await
        .unwrap();
    keys.create_client_key(
        command(
            "acct_codex_oauth",
            vec![AccountGroupId::new(group_id).unwrap()],
        ),
        &context,
    )
    .await
    .unwrap();
    for scope_change in [
        "update account_groups set enabled=false where id=$1",
        "delete from account_group_accounts where account_group_id=$1",
    ] {
        sqlx::query(scope_change)
            .bind(group_id)
            .execute(&database.pool)
            .await
            .unwrap();
        // 同值保存、停用和重新启用都不重新建立关系，失效范围不能阻止管理操作
        for enabled in [true, false, true] {
            let expected = CodexAccessBinding {
                enabled,
                mode: CodexAccessMode::Fixed,
                account_id: ProviderAccountId::new("acct_codex_oauth").unwrap(),
            };
            let (_, saved) = keys
                .update_client_key(
                    UpdateClientKey {
                        codex_access: Some(CodexAccessUpdate {
                            enabled: expected.enabled,
                            mode: Some(expected.mode),
                            account_id: expected.account_id.clone(),
                        }),
                        request_profile_override_updates: Default::default(),
                        id: ClientApiKeyId::new("key_scope").unwrap(),
                        name: "Codex scope".into(),
                        label: None,
                        group_ids: vec![AccountGroupId::new(group_id).unwrap()],
                        limits: Default::default(),
                        daily_limit_usd: None,
                        weekly_limit_usd: None,
                    },
                    &context,
                )
                .await
                .unwrap();
            assert_eq!(saved.codex_access, Some(expected));
        }
        sqlx::query("update account_groups set enabled=true where id=$1")
            .bind(group_id)
            .execute(&database.pool)
            .await
            .unwrap();
    }
    database.close().await;
}

#[tokio::test]
async fn codex_session_owner_claims_once_and_keeps_identity_references() {
    use gateway_admin::model::{
        MutationActor, MutationContext,
        client_keys::{DeleteClientKey, NewClientKey, UpdateClientKey},
    };
    use gateway_core::{
        account::{ProviderAccountId, ProviderAccountStore as _},
        policy::{CodexAccessBinding, CodexAccessMode, CodexAccessUpdate},
        provider_ports::{
            CodexSessionClaim, CodexSessionClaimOutcome, CodexSessionOwnershipPort as _,
            ProviderSessionAffinityKey,
        },
    };
    use gateway_store::postgres::{
        PgCodexSessionOwnership, PgProviderAccountRepository, ProviderAccountRepository,
    };
    let Some(database) = TestDatabase::create("codex_session_owner").await else {
        return;
    };
    let repository = PgProviderAccountRepository::new(database.pool.clone());
    for (id, authentication, enabled) in [
        ("acct_session_a", "oauth", true),
        ("acct_session_b", "oauth", true),
        ("acct_session_disabled", "oauth", false),
        ("acct_session_api", "api_key", true),
    ] {
        let mut account = super::provider_accounts::account(id, id);
        account.authentication_kind = authentication.into();
        account.enabled = enabled;
        account.upstream_account_id = Some(format!("workspace-{id}"));
        repository.insert_provider_account(account).await.unwrap();
    }
    let revision = |id: &str| {
        let repository = repository.clone();
        let id = id.to_owned();
        async move {
            repository
                .load_current_credential(&ProviderAccountId::new(id).unwrap())
                .await
                .unwrap()
                .account
                .revision()
                .get()
        }
    };
    let keys = PgAdminClientKeyStore::new(database.pool.clone());
    let context = MutationContext {
        actor: MutationActor::System,
        request_id: "codex-session".into(),
    };
    let key_id = ClientApiKeyId::new("key_codex_session").unwrap();
    keys.create_client_key(
        NewClientKey {
            codex_access: Some(CodexAccessBinding {
                enabled: true,
                mode: CodexAccessMode::SessionPool,
                account_id: ProviderAccountId::new("acct_session_a").unwrap(),
            }),
            request_profile_overrides: Default::default(),
            id: key_id.clone(),
            name: "Codex session".into(),
            label: None,
            group_ids: Vec::new(),
            limits: Default::default(),
            budget: Default::default(),
            plaintext: "test-codex-session".into(),
        },
        &context,
    )
    .await
    .unwrap();
    let ownership = PgCodexSessionOwnership::new(database.pool.clone());
    let session = |key: &str| ProviderSessionAffinityKey::try_new(key).unwrap();
    let claim = |session_key: &ProviderSessionAffinityKey, account: &str, revision: u64| {
        CodexSessionClaim {
            key_id: key_id.clone(),
            session_key: session_key.clone(),
            account_id: ProviderAccountId::new(account).unwrap(),
            credential_revision: revision,
        }
    };
    let owner_a = ProviderAccountId::new("acct_session_a").unwrap();
    let session_a = session("session-a");
    let claim_a = claim(
        &session_a,
        "acct_session_a",
        revision("acct_session_a").await,
    );
    assert_eq!(
        ownership.claim(&claim_a).await.unwrap(),
        CodexSessionClaimOutcome::Owned
    );
    assert_eq!(
        ownership.load(&key_id, &session_a).await.unwrap(),
        Some(owner_a.clone())
    );
    // 同号重复认领是幂等复用，不新增归属行
    assert_eq!(
        ownership.claim(&claim_a).await.unwrap(),
        CodexSessionClaimOutcome::Owned
    );
    let rows: i64 = sqlx::query_scalar("select count(*) from client_api_key_codex_sessions")
        .fetch_one(&database.pool)
        .await
        .unwrap();
    assert_eq!(rows, 1);
    // 异号认领不覆盖，只报告真实 owner
    let conflict = claim(
        &session_a,
        "acct_session_b",
        revision("acct_session_b").await,
    );
    assert_eq!(
        ownership.claim(&conflict).await.unwrap(),
        CodexSessionClaimOutcome::ConflictingOwner(owner_a)
    );
    // 停用、非 OAuth 与版本变化的账号都不能承载原生会话
    let unavailable = claim(
        &session("session-disabled"),
        "acct_session_disabled",
        revision("acct_session_disabled").await,
    );
    assert_eq!(
        ownership.claim(&unavailable).await.unwrap(),
        CodexSessionClaimOutcome::Unavailable
    );
    let unavailable = claim(
        &session("session-api"),
        "acct_session_api",
        revision("acct_session_api").await,
    );
    assert_eq!(
        ownership.claim(&unavailable).await.unwrap(),
        CodexSessionClaimOutcome::Unavailable
    );
    let session_stale = session("session-stale");
    let stale = claim(
        &session_stale,
        "acct_session_a",
        revision("acct_session_a").await + 1,
    );
    assert_eq!(
        ownership.claim(&stale).await.unwrap(),
        CodexSessionClaimOutcome::IdentityChanged
    );
    assert_eq!(ownership.load(&key_id, &session_stale).await.unwrap(), None);
    // 模式与入口账号一样建立后不可原地切换；省略 mode 保留既有会话池模式
    let update = |mode: Option<CodexAccessMode>| UpdateClientKey {
        codex_access: Some(CodexAccessUpdate {
            enabled: true,
            mode,
            account_id: ProviderAccountId::new("acct_session_a").unwrap(),
        }),
        request_profile_override_updates: Default::default(),
        id: key_id.clone(),
        name: "Codex session".into(),
        label: None,
        group_ids: Vec::new(),
        limits: Default::default(),
        daily_limit_usd: None,
        weekly_limit_usd: None,
    };
    assert!(
        keys.update_client_key(update(Some(CodexAccessMode::Fixed)), &context)
            .await
            .is_err(),
        "会话池模式不能被原地切回固定账号"
    );
    keys.update_client_key(update(None), &context)
        .await
        .unwrap();
    let record = keys.get_client_key(&key_id).await.unwrap().unwrap();
    assert_eq!(
        record.codex_access.unwrap().mode,
        CodexAccessMode::SessionPool
    );
    // 删除 Key 时归属行随外键级联清理
    keys.delete_client_key(DeleteClientKey { id: key_id }, &context)
        .await
        .unwrap();
    let rows: i64 = sqlx::query_scalar("select count(*) from client_api_key_codex_sessions")
        .fetch_one(&database.pool)
        .await
        .unwrap();
    assert_eq!(rows, 0);
    database.close().await;
}

#[tokio::test]
async fn codex_session_owner_concurrent_first_claims_keep_one_winner() {
    use gateway_admin::model::{MutationActor, MutationContext, client_keys::NewClientKey};
    use gateway_core::{
        account::{ProviderAccountId, ProviderAccountStore as _},
        policy::{CodexAccessBinding, CodexAccessMode},
        provider_ports::{
            CodexSessionClaim, CodexSessionClaimOutcome, CodexSessionOwnershipPort as _,
            ProviderSessionAffinityKey,
        },
    };
    use gateway_store::postgres::{
        PgCodexSessionOwnership, PgProviderAccountRepository, ProviderAccountRepository,
    };
    let Some(database) = TestDatabase::create("codex_session_concurrent").await else {
        return;
    };
    let repository = PgProviderAccountRepository::new(database.pool.clone());
    for id in ["acct_session_a", "acct_session_b"] {
        let mut account = super::provider_accounts::account(id, id);
        account.authentication_kind = "oauth".into();
        account.upstream_account_id = Some(format!("workspace-{id}"));
        repository.insert_provider_account(account).await.unwrap();
    }
    let keys = PgAdminClientKeyStore::new(database.pool.clone());
    let context = MutationContext {
        actor: MutationActor::System,
        request_id: "codex-session-concurrent".into(),
    };
    let key_id = ClientApiKeyId::new("key_codex_session_concurrent").unwrap();
    keys.create_client_key(
        NewClientKey {
            codex_access: Some(CodexAccessBinding {
                enabled: true,
                mode: CodexAccessMode::SessionPool,
                account_id: ProviderAccountId::new("acct_session_a").unwrap(),
            }),
            request_profile_overrides: Default::default(),
            id: key_id.clone(),
            name: "Codex session concurrent".into(),
            label: None,
            group_ids: Vec::new(),
            limits: Default::default(),
            budget: Default::default(),
            plaintext: "test-codex-session-concurrent".into(),
        },
        &context,
    )
    .await
    .unwrap();
    let revision = |id: &str| {
        let repository = repository.clone();
        let id = id.to_owned();
        async move {
            repository
                .load_current_credential(&ProviderAccountId::new(id).unwrap())
                .await
                .unwrap()
                .account
                .revision()
                .get()
        }
    };
    let ownership = PgCodexSessionOwnership::new(database.pool.clone());
    let session_key = ProviderSessionAffinityKey::try_new("session-shared").unwrap();
    let claim = |account: &str, revision: u64| CodexSessionClaim {
        key_id: key_id.clone(),
        session_key: session_key.clone(),
        account_id: ProviderAccountId::new(account).unwrap(),
        credential_revision: revision,
    };
    let claim_a = claim("acct_session_a", revision("acct_session_a").await);
    let claim_b = claim("acct_session_b", revision("acct_session_b").await);
    // 两连接池会同时被两个认领事务占满，观察连接单独连到同一隔离 schema
    let observer = database.observer_pool().await;
    let mut holding = observer.begin().await.unwrap();
    sqlx::query("lock table client_api_key_codex_sessions in exclusive mode")
        .execute(&mut *holding)
        .await
        .unwrap();
    let (first, second, ()) = tokio::join!(
        ownership.claim(&claim_a),
        ownership.claim(&claim_b),
        async {
            // 两个事务都读过归属后才放开插入，才能覆盖插入冲突后读回赢家的分支
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let waiting: i64 = sqlx::query_scalar(
                        "select count(*) from pg_locks
                         where database = (select oid from pg_database where datname = current_database())
                           and relation = 'client_api_key_codex_sessions'::regclass
                           and mode = 'RowExclusiveLock'
                           and not granted",
                    )
                    .fetch_one(&mut *holding)
                    .await
                    .unwrap();
                    if waiting == 2 {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("两个认领事务都要先越过初次归属查询才放开插入");
            holding.commit().await.unwrap();
        }
    );
    let (winner, owner) = match (first.unwrap(), second.unwrap()) {
        (CodexSessionClaimOutcome::Owned, CodexSessionClaimOutcome::ConflictingOwner(owner)) => {
            ("acct_session_a", owner)
        }
        (CodexSessionClaimOutcome::ConflictingOwner(owner), CodexSessionClaimOutcome::Owned) => {
            ("acct_session_b", owner)
        }
        (first, second) => panic!("并发首绑必须只有一个赢家: {first:?} {second:?}"),
    };
    assert_eq!(owner.as_str(), winner);
    // 后到的认领只报告真实赢家，归属行不被覆盖
    assert_eq!(
        ownership.load(&key_id, &session_key).await.unwrap(),
        Some(ProviderAccountId::new(winner).unwrap())
    );
    observer.close().await;
    database.close().await;
}
