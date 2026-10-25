//! 验证原生会话状态、history 条目与 notes 操作日志的 PostgreSQL 持久化合同
//!
//! 覆盖来源判定、代次 CAS 发布、Key 隔离、顺序稳定、原对象恢复与 Key 删除级联

use gateway_admin::{
    model::{
        MutationActor, MutationContext,
        client_keys::{DeleteClientKey, NewClientKey},
    },
    ports::store::ClientKeyStore as _,
};
use gateway_core::{
    account::{ProviderAccountId, ProviderAccountStore as _},
    policy::ClientApiKeyId,
    provider_ports::{
        CodexHistoryItem, CodexHistoryPendingInput, CodexHistoryRequest, CodexHistoryResponse,
        CodexHistoryRole, CodexHistoryWindow, CodexNativeWindowId, CodexNotesCompletion,
        CodexNotesGeneration, CodexNotesOperation, CodexNotesOperationKind,
        CodexNotesOperationStatus, CodexNotesPublish, CodexNotesPublishOutcome, CodexNotesReplay,
        CodexNotesReplica, CodexSessionClaim, CodexSessionClaimOutcome,
        CodexSessionContentPort as _, CodexSessionOrigin, CodexSessionOwnershipPort as _,
        CodexSessionStatePort as _, NewCodexNotesOperation, ProviderSessionAffinityKey,
        ProviderStoreErrorKind,
    },
};
use gateway_store::postgres::{
    PgAdminClientKeyStore, PgCodexSessionOwnership, PgCodexSessionState,
    PgProviderAccountRepository, ProviderAccountRepository,
};

use super::TestDatabase;

const CONTEXT_WINDOW_ID: &str = "0193f0b7-1d2a-7c31-9b0e-2a3f5c6d7e80";

#[tokio::test]
async fn claimed_sessions_stay_unclassified_until_origin_is_resolved_once() {
    let Some(database) = TestDatabase::create("codex_state_origin").await else {
        return;
    };
    let key_id = create_key(&database, "key_state_origin").await;
    let revision = insert_oauth_account(&database, "acct_state_a").await;
    let owner = ProviderAccountId::new("acct_state_a").unwrap();
    let session = session_key("session-origin");
    let ownership = PgCodexSessionOwnership::new(database.pool.clone());
    let state = PgCodexSessionState::new(database.pool.clone());

    assert_eq!(state.load_session(&key_id, &session).await.unwrap(), None);
    assert_eq!(
        ownership
            .claim(&CodexSessionClaim {
                key_id: key_id.clone(),
                session_key: session.clone(),
                account_id: owner.clone(),
                credential_revision: revision,
            })
            .await
            .unwrap(),
        CodexSessionClaimOutcome::Owned
    );
    let claimed = state
        .load_session(&key_id, &session)
        .await
        .unwrap()
        .unwrap();
    // 新认领只说明归属，采集来源仍未判定
    assert_eq!(claimed.origin, CodexSessionOrigin::Unclassified);
    assert_eq!(claimed.owner, owner);
    assert_eq!(claimed.generation, 0);
    assert_eq!(claimed.notes, CodexNotesReplica::native(owner.clone()));
    assert_eq!(claimed.notes_replay, None);

    let resolved = state
        .resolve_session_origin(&key_id, &session, CodexSessionOrigin::Managed)
        .await
        .unwrap();
    assert_eq!(resolved.origin, CodexSessionOrigin::Managed);
    // 首次判定固定；后到的相反判定不覆盖既有事实
    let kept = state
        .resolve_session_origin(&key_id, &session, CodexSessionOrigin::Legacy)
        .await
        .unwrap();
    assert_eq!(kept.origin, CodexSessionOrigin::Managed);
    // 未判定本身不是可写入的判定结果
    assert_eq!(
        state
            .resolve_session_origin(&key_id, &session, CodexSessionOrigin::Unclassified)
            .await
            .expect_err("unclassified is not a resolution")
            .kind(),
        ProviderStoreErrorKind::InvalidData
    );
    // 未认领会话不能判定来源
    assert_eq!(
        state
            .resolve_session_origin(
                &key_id,
                &session_key("session-unclaimed"),
                CodexSessionOrigin::Managed,
            )
            .await
            .expect_err("unclaimed session")
            .kind(),
        ProviderStoreErrorKind::InvalidData
    );

    // 迁移前的既有归属行按 legacy 处理，不因新列出现而改变 owner 读取
    sqlx::query(
        "insert into client_api_key_codex_sessions (key_id, session_key, account_id)
         values ($1, $2, $3)",
    )
    .bind(key_id.as_str())
    .bind("session-existing")
    .bind(owner.as_str())
    .execute(&database.pool)
    .await
    .unwrap();
    let existing = state
        .load_session(&key_id, &session_key("session-existing"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(existing.origin, CodexSessionOrigin::Legacy);
    assert_eq!(existing.owner, owner);
    assert_eq!(existing.generation, 0);
    assert_eq!(
        ownership
            .load(&key_id, &session_key("session-existing"))
            .await
            .unwrap(),
        Some(owner)
    );
    database.close().await;
}

#[tokio::test]
async fn publish_notes_replay_switches_owner_and_rebuilt_domain_by_generation() {
    let Some(database) = TestDatabase::create("codex_state_publish").await else {
        return;
    };
    let key_id = create_key(&database, "key_state_publish").await;
    let revision_a = insert_oauth_account(&database, "acct_publish_a").await;
    let revision_b = insert_oauth_account(&database, "acct_publish_b").await;
    let owner_a = ProviderAccountId::new("acct_publish_a").unwrap();
    let owner_b = ProviderAccountId::new("acct_publish_b").unwrap();
    let session = session_key("session-publish");
    let state = PgCodexSessionState::new(database.pool.clone());
    claim(&database, &key_id, &session, &owner_a, revision_a).await;
    state
        .resolve_session_origin(&key_id, &session, CodexSessionOrigin::Managed)
        .await
        .unwrap();

    // 两次正常写入在冻结前完整终结，全部进入重放目标
    for (kind, request, response) in [
        (
            CodexNotesOperationKind::WriteFile,
            r#"{"context":{"session_id":"s","current_agent_name":"/root"},"ciphertext":"one"}"#,
            r#"{"result":"ok","ciphertext":"resp-one"}"#,
        ),
        (
            CodexNotesOperationKind::AppendToFile,
            r#"{"context":{"session_id":"s","current_agent_name":"/root"},"ciphertext":"two"}"#,
            r#"{"result":"ok","ciphertext":"resp-two"}"#,
        ),
    ] {
        record_operation(
            &database,
            &key_id,
            &session,
            CodexNotesReplica::native(owner_a.clone()),
            kind,
            request,
            response,
        )
        .await;
    }

    let generation = CodexNotesGeneration::generate();
    let replica = CodexNotesReplica::rebuilt(owner_b.clone(), generation.clone());
    state
        .save_notes_replay(
            &key_id,
            &session,
            &CodexNotesReplay::try_new(replica.clone(), 2, 2).unwrap(),
        )
        .await
        .unwrap();
    let pending = state
        .load_session(&key_id, &session)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pending.owner, owner_a);
    assert_eq!(pending.notes_replay.as_ref().unwrap().replica, replica);
    // 独立连接读到已提交的迁移进度，等价于普通重启后的重新装配
    let observer = database.observer_pool().await;
    let restarted = PgCodexSessionState::new(observer.clone())
        .load_session(&key_id, &session)
        .await
        .unwrap()
        .unwrap();
    let replayed = restarted.notes_replay.expect("persisted replay progress");
    assert_eq!(replayed.replica, replica);
    assert_eq!(replayed.frozen_through_seq, 2);
    assert_eq!(replayed.replayed_through_seq, 2);

    // 原生域不是切换目标；owner 与副本落点必须属于同一目标账号
    assert_eq!(
        state
            .publish_notes_replay(&CodexNotesPublish {
                key_id: key_id.clone(),
                session_key: session.clone(),
                expected_generation: 0,
                owner: owner_b.clone(),
                credential_revision: revision_b,
                replica: CodexNotesReplica::native(owner_b.clone()),
                through_seq: 2,
            })
            .await
            .expect_err("native replica is not a switch target")
            .kind(),
        ProviderStoreErrorKind::InvalidData
    );
    assert_eq!(
        state
            .publish_notes_replay(&CodexNotesPublish {
                key_id: key_id.clone(),
                session_key: session.clone(),
                expected_generation: 0,
                owner: owner_b.clone(),
                credential_revision: revision_b,
                replica: CodexNotesReplica::rebuilt(
                    owner_a.clone(),
                    CodexNotesGeneration::generate(),
                ),
                through_seq: 2,
            })
            .await
            .expect_err("replica account must match the owner")
            .kind(),
        ProviderStoreErrorKind::InvalidData
    );

    let published = state
        .publish_notes_replay(&CodexNotesPublish {
            key_id: key_id.clone(),
            session_key: session.clone(),
            expected_generation: 0,
            owner: owner_b.clone(),
            credential_revision: revision_b,
            replica: replica.clone(),
            through_seq: 2,
        })
        .await
        .unwrap();
    let published = match published {
        CodexNotesPublishOutcome::Published(state) => state,
        other => panic!("expected published state, got {other:?}"),
    };
    assert_eq!(published.owner, owner_b);
    assert_eq!(published.generation, 1);
    assert_eq!(published.notes, replica);
    assert_eq!(published.notes_replay, None);
    assert_eq!(published.origin, CodexSessionOrigin::Managed);
    let restarted = PgCodexSessionState::new(observer.clone())
        .load_session(&key_id, &session)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(restarted.owner, owner_b);
    assert_eq!(restarted.generation, 1);
    assert_eq!(restarted.notes, replica);
    assert_eq!(restarted.notes_replay, None);
    observer.close().await;
    // 归属读取跟随新 owner，既有 owner 端口行为不变
    assert_eq!(
        PgCodexSessionOwnership::new(database.pool.clone())
            .load(&key_id, &session)
            .await
            .unwrap(),
        Some(owner_b.clone())
    );

    // 用旧代次再次发布读回最新状态，不重复切换
    let stale = state
        .publish_notes_replay(&CodexNotesPublish {
            key_id: key_id.clone(),
            session_key: session.clone(),
            expected_generation: 0,
            owner: owner_b.clone(),
            credential_revision: revision_b,
            replica: replica.clone(),
            through_seq: 2,
        })
        .await
        .unwrap();
    let latest = match stale {
        CodexNotesPublishOutcome::GenerationChanged(state) => state,
        other => panic!("expected generation change, got {other:?}"),
    };
    assert_eq!(latest.generation, 1);
    assert_eq!(latest.owner, owner_b);

    // 凭据版本变化与不可承载账号都不能用旧身份发布
    assert_eq!(
        state
            .publish_notes_replay(&CodexNotesPublish {
                key_id: key_id.clone(),
                session_key: session.clone(),
                expected_generation: 1,
                owner: owner_a.clone(),
                credential_revision: revision_a + 1,
                replica: CodexNotesReplica::rebuilt(
                    owner_a.clone(),
                    CodexNotesGeneration::generate(),
                ),
                through_seq: 2,
            })
            .await
            .unwrap(),
        CodexNotesPublishOutcome::IdentityChanged
    );
    insert_api_key_account(&database, "acct_publish_api").await;
    assert_eq!(
        state
            .publish_notes_replay(&CodexNotesPublish {
                key_id: key_id.clone(),
                session_key: session.clone(),
                expected_generation: 1,
                owner: ProviderAccountId::new("acct_publish_api").unwrap(),
                credential_revision: 0,
                replica: CodexNotesReplica::rebuilt(
                    ProviderAccountId::new("acct_publish_api").unwrap(),
                    CodexNotesGeneration::generate(),
                ),
                through_seq: 2,
            })
            .await
            .unwrap(),
        CodexNotesPublishOutcome::Unavailable
    );
    database.close().await;
}

#[tokio::test]
async fn publish_requires_a_completed_replay_target_and_terminal_log() {
    let Some(database) = TestDatabase::create("codex_state_publish_pending").await else {
        return;
    };
    let key_id = create_key(&database, "key_state_publish_pending").await;
    let revision = insert_oauth_account(&database, "acct_pending_a").await;
    let owner = ProviderAccountId::new("acct_pending_a").unwrap();
    let target = ProviderAccountId::new("acct_pending_b").unwrap();
    let target_revision = insert_oauth_account(&database, "acct_pending_b").await;
    let state = PgCodexSessionState::new(database.pool.clone());

    // 没有保存过重建目标就不能发布：日志终态不等于已在新域同步
    let unrebuilt = session_key("session-unrebuilt");
    claim(&database, &key_id, &unrebuilt, &owner, revision).await;
    let replica = CodexNotesReplica::rebuilt(target.clone(), CodexNotesGeneration::generate());
    assert_eq!(
        state
            .publish_notes_replay(&publish_to(
                &key_id,
                &unrebuilt,
                &target,
                target_revision,
                &replica,
                0,
            ))
            .await
            .unwrap(),
        CodexNotesPublishOutcome::ReplayNotReady
    );

    // 保存的目标账号或域代次与本次发布不同，同样不能发布
    state
        .save_notes_replay(
            &key_id,
            &unrebuilt,
            &CodexNotesReplay::try_new(replica.clone(), 0, 0).unwrap(),
        )
        .await
        .unwrap();
    let other_replica =
        CodexNotesReplica::rebuilt(target.clone(), CodexNotesGeneration::generate());
    assert_eq!(
        state
            .publish_notes_replay(&publish_to(
                &key_id,
                &unrebuilt,
                &target,
                target_revision,
                &other_replica,
                0,
            ))
            .await
            .unwrap(),
        CodexNotesPublishOutcome::ReplayNotReady
    );
    // 空日志用已保存的 through=0 目标发布，新域从零开始
    let published = state
        .publish_notes_replay(&publish_to(
            &key_id,
            &unrebuilt,
            &target,
            target_revision,
            &replica,
            0,
        ))
        .await
        .unwrap();
    assert!(matches!(published, CodexNotesPublishOutcome::Published(_)));

    // 冻结目标尚未重放到冻结位置时不能发布
    let session = session_key("session-unfinished");
    claim(&database, &key_id, &session, &owner, revision).await;
    let operation = append_operation(
        &database,
        &key_id,
        &session,
        CodexNotesReplica::native(owner.clone()),
        CodexNotesOperationKind::AppendToFile,
        r#"{"ciphertext":"pending"}"#,
    )
    .await;
    state
        .save_notes_replay(
            &key_id,
            &session,
            &CodexNotesReplay::try_new(replica.clone(), operation.seq, 0).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        state
            .publish_notes_replay(&publish_to(
                &key_id,
                &session,
                &target,
                target_revision,
                &replica,
                operation.seq,
            ))
            .await
            .unwrap(),
        CodexNotesPublishOutcome::ReplayNotReady
    );

    // 未终结操作不能被跳过，即使已声称重放完成
    state
        .save_notes_replay(
            &key_id,
            &session,
            &CodexNotesReplay::try_new(replica.clone(), operation.seq, operation.seq).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        state
            .publish_notes_replay(&publish_to(
                &key_id,
                &session,
                &target,
                target_revision,
                &replica,
                operation.seq,
            ))
            .await
            .unwrap(),
        CodexNotesPublishOutcome::ReplayNotReady
    );

    // 尝试已开始但未终结同样阻止发布
    state
        .start_notes_operation(&key_id, &session, operation.seq)
        .await
        .unwrap();
    assert_eq!(
        state
            .publish_notes_replay(&publish_to(
                &key_id,
                &session,
                &target,
                target_revision,
                &replica,
                operation.seq,
            ))
            .await
            .unwrap(),
        CodexNotesPublishOutcome::ReplayNotReady
    );

    // 冻结位置之后的任何操作都还没进入重放目标
    let beyond = session_key("session-beyond");
    claim(&database, &key_id, &beyond, &owner, revision).await;
    record_operation(
        &database,
        &key_id,
        &beyond,
        CodexNotesReplica::native(owner.clone()),
        CodexNotesOperationKind::WriteFile,
        r#"{"ciphertext":"first"}"#,
        r#"{"result":"ok"}"#,
    )
    .await;
    record_operation(
        &database,
        &key_id,
        &beyond,
        CodexNotesReplica::native(owner.clone()),
        CodexNotesOperationKind::AppendToFile,
        r#"{"ciphertext":"second"}"#,
        r#"{"result":"ok"}"#,
    )
    .await;
    state
        .save_notes_replay(
            &key_id,
            &beyond,
            &CodexNotesReplay::try_new(replica.clone(), 1, 1).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        state
            .publish_notes_replay(&publish_to(
                &key_id,
                &beyond,
                &target,
                target_revision,
                &replica,
                2,
            ))
            .await
            .unwrap(),
        CodexNotesPublishOutcome::ReplayNotReady
    );
    database.close().await;
}

#[tokio::test]
async fn history_items_keep_original_objects_order_and_response_anchors() {
    let Some(database) = TestDatabase::create("codex_state_history").await else {
        return;
    };
    let key_id = create_key(&database, "key_state_history").await;
    let other_key_id = create_key(&database, "key_state_history_other").await;
    let session = session_key("session-shared");
    let store = PgCodexSessionState::new(database.pool.clone());
    let window = history_window("thread-history", 1);

    let user_raw = r#"{"type":"message","role":"user","content":[{"type":"input_text","text":"hello  \n保留空白"}]}"#;
    store
        .commit_history_request(&CodexHistoryRequest {
            key_id: key_id.clone(),
            session_key: session.clone(),
            window: window.clone(),
            previous_response_id: None,
            items: vec![
                history_item(
                    "item_user",
                    Some(CodexHistoryRole::User),
                    "message",
                    user_raw,
                    Some("hello  \n保留空白"),
                ),
                history_item(
                    "item_reasoning",
                    None,
                    "reasoning",
                    r#"{"type":"reasoning","encrypted_content":"opaque-cipher"}"#,
                    None,
                ),
            ],
        })
        .await
        .unwrap();
    store
        .commit_history_response(&CodexHistoryResponse {
            key_id: key_id.clone(),
            session_key: session.clone(),
            response_id: "resp_1".to_owned(),
            window: window.clone(),
            previous_response_id: None,
            input: vec!["item_user".to_owned(), "item_reasoning".to_owned()],
            items: vec![
                history_item(
                    "item_assistant",
                    Some(CodexHistoryRole::Assistant),
                    "message",
                    r#"{"type":"message","role":"assistant"}"#,
                    Some("answer"),
                ),
                history_item(
                    "item_call",
                    Some(CodexHistoryRole::Tool),
                    "function_call",
                    r#"{"type":"function_call","name":"shell"}"#,
                    None,
                ),
            ],
        })
        .await
        .unwrap();
    // 同一 response 的后续批次追加在既有条目之后
    store
        .commit_history_response(&CodexHistoryResponse {
            key_id: key_id.clone(),
            session_key: session.clone(),
            response_id: "resp_1".to_owned(),
            window: window.clone(),
            previous_response_id: None,
            input: vec!["item_later".to_owned()],
            items: vec![history_item(
                "item_output",
                Some(CodexHistoryRole::Tool),
                "function_call_output",
                r#"{"type":"function_call_output","output":"done"}"#,
                Some("done"),
            )],
        })
        .await
        .unwrap();
    // 零输出响应也保存锚；预热与空输出正常响应靠它续接
    store
        .commit_history_response(&CodexHistoryResponse {
            key_id: key_id.clone(),
            session_key: session.clone(),
            response_id: "resp_warmup".to_owned(),
            window: window.clone(),
            previous_response_id: None,
            input: Vec::new(),
            items: Vec::new(),
        })
        .await
        .unwrap();

    let items = store.load_history_items(&key_id, &session).await.unwrap();
    assert_eq!(
        items.iter().map(|item| item.ordinal).collect::<Vec<_>>(),
        vec![0, 1, 2, 3, 4]
    );
    assert_eq!(items[0].item.item_id, "item_user");
    assert_eq!(items[0].item.role, Some(CodexHistoryRole::User));
    // 原对象逐字节保留，不做 JSON 重写
    assert_eq!(items[0].item.raw, user_raw);
    assert_eq!(items[1].item.role, None);
    assert_eq!(items[1].item.text, None);
    assert_eq!(items[2].item.item_id, "item_assistant");
    assert_eq!(items[4].item.item_id, "item_output");
    assert!(items.iter().all(|item| item.window_id == window.window_id));
    // 语义身份随原对象保存，读回后仍参与同位置比较
    assert_eq!(items[0].item.identity, "identity-item_user");
    assert_eq!(items[1].item.identity, "identity-item_reasoning");

    let windows = store.load_history_windows(&key_id, &session).await.unwrap();
    assert_eq!(windows.len(), 1);
    assert_eq!(windows[0].window, window);
    assert_eq!(windows[0].item_count, 5);

    let anchor = store
        .load_history_response(&key_id, &session, "resp_1")
        .await
        .unwrap()
        .expect("saved response");
    assert_eq!(anchor.response_id, "resp_1");
    assert_eq!(anchor.window, window);
    assert_eq!(anchor.previous_response_id, None);
    // 逻辑输入只在首次锚定时写入；同一 response 的后续批次不改写当时输入
    assert_eq!(anchor.input, vec!["item_user", "item_reasoning"]);
    // 输出引用按批次追加，保持产出顺序
    assert_eq!(
        anchor.output,
        vec!["item_assistant", "item_call", "item_output"]
    );
    let warmup = store
        .load_history_response(&key_id, &session, "resp_warmup")
        .await
        .unwrap()
        .expect("empty response anchor");
    assert_eq!(warmup.window, window);
    assert!(warmup.input.is_empty());
    assert!(warmup.output.is_empty());
    assert_eq!(
        store
            .load_history_response(&key_id, &session, "resp_missing")
            .await
            .unwrap(),
        None
    );
    // 无父请求按该窗口最近锚定的响应还原当前逻辑上下文
    let latest = store
        .load_latest_history_response(&key_id, &session, &window.window_id)
        .await
        .unwrap()
        .expect("latest response anchor");
    assert_eq!(latest.response_id, "resp_warmup");
    assert!(latest.input.is_empty());
    assert!(latest.output.is_empty());
    assert_eq!(
        store
            .load_latest_history_response(
                &key_id,
                &session,
                &gateway_core::provider_ports::CodexNativeWindowId::try_new("thread-other:0")
                    .unwrap(),
            )
            .await
            .unwrap(),
        None
    );
    // 窗口条目查询只返回该窗口档案
    let window_items = store
        .load_history_window_items(&key_id, &session, &window.window_id)
        .await
        .unwrap();
    assert_eq!(window_items.len(), 5);
    assert!(
        window_items
            .iter()
            .all(|item| item.window_id == window.window_id)
    );

    // 相同正文是两次真实输入，不按正文去重
    store
        .commit_history_request(&CodexHistoryRequest {
            key_id: key_id.clone(),
            session_key: session.clone(),
            window: window.clone(),
            previous_response_id: Some("resp_1".to_owned()),
            items: vec![history_item(
                "item_user_repeat",
                Some(CodexHistoryRole::User),
                "message",
                user_raw,
                Some("hello  \n保留空白"),
            )],
        })
        .await
        .unwrap();
    let repeated = store.load_history_items(&key_id, &session).await.unwrap();
    assert_eq!(repeated.len(), 6);
    assert_eq!(repeated[5].item.raw, repeated[0].item.raw);
    // 同一窗口内同一稳定 ID 的重复提交是幂等 no-op
    store
        .commit_history_request(&CodexHistoryRequest {
            key_id: key_id.clone(),
            session_key: session.clone(),
            window: window.clone(),
            previous_response_id: None,
            items: vec![history_item(
                "item_user",
                Some(CodexHistoryRole::User),
                "message",
                user_raw,
                Some("hello  \n保留空白"),
            )],
        })
        .await
        .unwrap();
    assert_eq!(
        store
            .load_history_items(&key_id, &session)
            .await
            .unwrap()
            .len(),
        6
    );

    // 同一 response 不能改挂到另一个逻辑窗口
    assert_eq!(
        store
            .commit_history_response(&CodexHistoryResponse {
                key_id: key_id.clone(),
                session_key: session.clone(),
                response_id: "resp_1".to_owned(),
                window: history_window("thread-history", 2),
                previous_response_id: None,
                input: Vec::new(),
                items: Vec::new(),
            })
            .await
            .expect_err("response window conflict")
            .kind(),
        ProviderStoreErrorKind::Conflict
    );

    // Key 与逻辑会话都参与隔离；其他 Key 的同名会话看不到这些条目
    assert!(
        store
            .load_history_items(&other_key_id, &session)
            .await
            .unwrap()
            .is_empty()
    );
    store
        .commit_history_request(&CodexHistoryRequest {
            key_id: other_key_id.clone(),
            session_key: session.clone(),
            window: window.clone(),
            previous_response_id: None,
            items: vec![history_item(
                "item_other_key",
                Some(CodexHistoryRole::User),
                "message",
                r#"{"type":"message"}"#,
                Some("other"),
            )],
        })
        .await
        .unwrap();
    let other = store
        .load_history_items(&other_key_id, &session)
        .await
        .unwrap();
    assert_eq!(other.len(), 1);
    assert_eq!(other[0].item.item_id, "item_other_key");
    assert_eq!(other[0].ordinal, 0);
    assert_eq!(
        store
            .load_history_items(&key_id, &session)
            .await
            .unwrap()
            .len(),
        6
    );
    database.close().await;
}

/// 发送前逻辑输入快照按窗口保存：同一窗口只保留最新一份，Key 与会话隔离
#[tokio::test]
async fn pending_history_input_keeps_the_latest_logical_input_per_window() {
    let Some(database) = TestDatabase::create("codex_state_pending").await else {
        return;
    };
    let key_id = create_key(&database, "key_state_pending").await;
    let other_key_id = create_key(&database, "key_state_pending_other").await;
    let session = session_key("session-pending");
    let store = PgCodexSessionState::new(database.pool.clone());
    let window = history_window("thread-pending", 0);
    assert_eq!(
        store
            .load_history_pending_input(&key_id, &session, &window.window_id)
            .await
            .unwrap(),
        None
    );

    store
        .save_history_pending_input(
            &key_id,
            &session,
            &CodexHistoryPendingInput {
                window: window.clone(),
                previous_response_id: None,
                input: vec!["cprs:thread-pending:0:0".to_owned()],
            },
        )
        .await
        .unwrap();
    let first = store
        .load_history_pending_input(&key_id, &session, &window.window_id)
        .await
        .unwrap()
        .expect("sending-side snapshot");
    assert_eq!(first.window, window);
    assert_eq!(first.previous_response_id, None);
    assert_eq!(first.input, vec!["cprs:thread-pending:0:0".to_owned()]);

    // 同一窗口的更新覆盖旧快照：只有最近一次发送前请求才是可对齐的尾部
    store
        .save_history_pending_input(
            &key_id,
            &session,
            &CodexHistoryPendingInput {
                window: window.clone(),
                previous_response_id: Some("resp_pending".to_owned()),
                input: vec![
                    "cprs:thread-pending:0:0".to_owned(),
                    "cprs:thread-pending:0:1".to_owned(),
                ],
            },
        )
        .await
        .unwrap();
    let updated = store
        .load_history_pending_input(&key_id, &session, &window.window_id)
        .await
        .unwrap()
        .expect("updated snapshot");
    assert_eq!(
        updated.previous_response_id.as_deref(),
        Some("resp_pending")
    );
    assert_eq!(
        updated.input,
        vec![
            "cprs:thread-pending:0:0".to_owned(),
            "cprs:thread-pending:0:1".to_owned()
        ]
    );

    // 其它窗口与其它 Key 看不到这份快照
    assert_eq!(
        store
            .load_history_pending_input(
                &key_id,
                &session,
                &gateway_core::provider_ports::CodexNativeWindowId::try_new("thread-pending:1")
                    .unwrap(),
            )
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        store
            .load_history_pending_input(&other_key_id, &session, &window.window_id)
            .await
            .unwrap(),
        None
    );
    database.close().await;
}

#[tokio::test]
async fn notes_operations_allocate_sequence_and_record_original_outcomes() {
    let Some(database) = TestDatabase::create("codex_state_notes").await else {
        return;
    };
    let key_id = create_key(&database, "key_state_notes").await;
    let other_key_id = create_key(&database, "key_state_notes_other").await;
    let owner = ProviderAccountId::new("acct_notes_a").unwrap();
    insert_oauth_account(&database, "acct_notes_a").await;
    let session = session_key("session-notes");
    let store = PgCodexSessionState::new(database.pool.clone());
    let request = r#"{"context":{"session_id":"session-notes","current_agent_name":"/root"},"encrypted":"opaque"}"#;

    // history 与 notes 共用会话游标行；history 先写入时首条 notes 仍必须从 seq 1 开始
    store
        .commit_history_request(&CodexHistoryRequest {
            key_id: key_id.clone(),
            session_key: session.clone(),
            window: history_window("thread-notes", 0),
            previous_response_id: None,
            items: vec![history_item(
                "item_before_notes",
                Some(CodexHistoryRole::User),
                "message",
                r#"{"type":"message"}"#,
                Some("before notes"),
            )],
        })
        .await
        .unwrap();

    let first = append_operation(
        &database,
        &key_id,
        &session,
        CodexNotesReplica::native(owner.clone()),
        CodexNotesOperationKind::WriteFile,
        request,
    )
    .await;
    assert_eq!(first.seq, 1);
    assert_eq!(first.status, CodexNotesOperationStatus::Unsent);
    assert_eq!(first.request, request);
    assert_eq!(first.response, None);
    let second = append_operation(
        &database,
        &key_id,
        &session,
        CodexNotesReplica::native(owner.clone()),
        CodexNotesOperationKind::AppendToFile,
        request,
    )
    .await;
    // 正文相同的两次真实写入各占一个 seq
    assert_eq!(second.seq, 2);

    let started = store
        .start_notes_operation(&key_id, &session, first.seq)
        .await
        .unwrap();
    assert_eq!(started.status, CodexNotesOperationStatus::InFlight);
    let completed = store
        .complete_notes_operation(&CodexNotesCompletion {
            key_id: key_id.clone(),
            session_key: session.clone(),
            seq: first.seq,
            status: CodexNotesOperationStatus::Responded,
            response: Some(r#"{"result":"ok","cipher":"opaque"}"#.to_owned()),
            response_status: Some(200),
        })
        .await
        .unwrap();
    assert_eq!(completed.status, CodexNotesOperationStatus::Responded);
    assert_eq!(
        completed.response.as_deref(),
        Some(r#"{"result":"ok","cipher":"opaque"}"#)
    );
    assert_eq!(completed.response_status, Some(200));

    // 终结观察不可重开，未开始的写入也不能被再次开始
    assert_eq!(
        store
            .complete_notes_operation(&CodexNotesCompletion {
                key_id: key_id.clone(),
                session_key: session.clone(),
                seq: first.seq,
                status: CodexNotesOperationStatus::Unknown,
                response: None,
                response_status: None,
            })
            .await
            .expect_err("terminal operation cannot be reopened")
            .kind(),
        ProviderStoreErrorKind::Conflict
    );
    assert_eq!(
        store
            .start_notes_operation(&key_id, &session, first.seq)
            .await
            .expect_err("responded operation is not startable")
            .kind(),
        ProviderStoreErrorKind::Conflict
    );
    assert_eq!(
        store
            .start_notes_operation(&key_id, &session, 99)
            .await
            .expect_err("unknown seq")
            .kind(),
        ProviderStoreErrorKind::InvalidData
    );
    // 完整响应标记必须带原始响应
    assert_eq!(
        store
            .complete_notes_operation(&CodexNotesCompletion {
                key_id: key_id.clone(),
                session_key: session.clone(),
                seq: second.seq,
                status: CodexNotesOperationStatus::Responded,
                response: None,
                response_status: Some(200),
            })
            .await
            .expect_err("responded requires the original response")
            .kind(),
        ProviderStoreErrorKind::InvalidData
    );
    // 发送前明确失败直接终结，不进入尝试开始状态
    let failed = store
        .complete_notes_operation(&CodexNotesCompletion {
            key_id: key_id.clone(),
            session_key: session.clone(),
            seq: second.seq,
            status: CodexNotesOperationStatus::Failed,
            response: None,
            response_status: None,
        })
        .await
        .unwrap();
    assert_eq!(failed.status, CodexNotesOperationStatus::Failed);

    let replica = CodexNotesReplica::rebuilt(owner, CodexNotesGeneration::generate());
    let third = append_operation(
        &database,
        &key_id,
        &session,
        replica.clone(),
        CodexNotesOperationKind::AppendToFile,
        request,
    )
    .await;
    assert_eq!(third.seq, 3);
    assert_eq!(third.replica, replica);

    let operations = store
        .load_notes_operations(&key_id, &session)
        .await
        .unwrap();
    assert_eq!(
        operations
            .iter()
            .map(|operation| operation.seq)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(
        operations
            .iter()
            .map(|operation| operation.status)
            .collect::<Vec<_>>(),
        vec![
            CodexNotesOperationStatus::Responded,
            CodexNotesOperationStatus::Failed,
            CodexNotesOperationStatus::Unsent,
        ]
    );
    // Key 隔离：同名会话的其他 Key 没有操作日志
    assert!(
        store
            .load_notes_operations(&other_key_id, &session)
            .await
            .unwrap()
            .is_empty()
    );
    database.close().await;
}

#[tokio::test]
async fn deleting_key_cascades_native_state_history_and_notes() {
    let Some(database) = TestDatabase::create("codex_state_cascade").await else {
        return;
    };
    let key_id = create_key(&database, "key_state_cascade").await;
    let owner = ProviderAccountId::new("acct_cascade_a").unwrap();
    let revision = insert_oauth_account(&database, "acct_cascade_a").await;
    let session = session_key("session-cascade");
    let store = PgCodexSessionState::new(database.pool.clone());
    claim(&database, &key_id, &session, &owner, revision).await;
    store
        .save_history_pending_input(
            &key_id,
            &session,
            &CodexHistoryPendingInput {
                window: history_window("thread-cascade", 1),
                previous_response_id: None,
                input: vec!["item_cascade".to_owned()],
            },
        )
        .await
        .unwrap();
    store
        .commit_history_request(&CodexHistoryRequest {
            key_id: key_id.clone(),
            session_key: session.clone(),
            window: history_window("thread-cascade", 1),
            previous_response_id: None,
            items: vec![history_item(
                "item_cascade",
                Some(CodexHistoryRole::User),
                "message",
                r#"{"type":"message"}"#,
                Some("text"),
            )],
        })
        .await
        .unwrap();
    store
        .commit_history_response(&CodexHistoryResponse {
            key_id: key_id.clone(),
            session_key: session.clone(),
            response_id: "resp_cascade".to_owned(),
            window: history_window("thread-cascade", 1),
            previous_response_id: None,
            input: vec!["item_cascade".to_owned()],
            items: vec![history_item(
                "item_cascade_response",
                Some(CodexHistoryRole::Assistant),
                "message",
                r#"{"type":"message"}"#,
                Some("answer"),
            )],
        })
        .await
        .unwrap();
    append_operation(
        &database,
        &key_id,
        &session,
        CodexNotesReplica::native(owner),
        CodexNotesOperationKind::WriteFile,
        r#"{"ciphertext":"cascade"}"#,
    )
    .await;

    PgAdminClientKeyStore::new(database.pool.clone())
        .delete_client_key(
            DeleteClientKey { id: key_id.clone() },
            &MutationContext {
                actor: MutationActor::System,
                request_id: "delete-native-state".to_owned(),
            },
        )
        .await
        .unwrap();
    for table in [
        "client_api_key_codex_sessions",
        "client_api_key_codex_session_counters",
        "client_api_key_codex_history_items",
        "client_api_key_codex_history_responses",
        "client_api_key_codex_history_response_items",
        "client_api_key_codex_history_pending_inputs",
        "client_api_key_codex_notes_operations",
    ] {
        let rows: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "select count(*) from {table} where key_id = $1"
        )))
        .bind(key_id.as_str())
        .fetch_one(&database.pool)
        .await
        .unwrap();
        assert_eq!(rows, 0, "{table}");
    }
    database.close().await;
}

async fn create_key(database: &TestDatabase, id: &str) -> ClientApiKeyId {
    let key_id = ClientApiKeyId::new(id).unwrap();
    PgAdminClientKeyStore::new(database.pool.clone())
        .create_client_key(
            NewClientKey {
                codex_access: None,
                request_profile_overrides: Default::default(),
                id: key_id.clone(),
                name: id.to_owned(),
                label: None,
                group_ids: Vec::new(),
                limits: Default::default(),
                budget: Default::default(),
                plaintext: format!("{id}-plaintext"),
            },
            &MutationContext {
                actor: MutationActor::System,
                request_id: id.to_owned(),
            },
        )
        .await
        .unwrap();
    key_id
}

async fn insert_oauth_account(database: &TestDatabase, id: &str) -> u64 {
    let repository = PgProviderAccountRepository::new(database.pool.clone());
    let mut account = super::provider_accounts::account(id, id);
    account.authentication_kind = "oauth".into();
    account.upstream_account_id = Some(format!("workspace-{id}"));
    repository.insert_provider_account(account).await.unwrap();
    revision(&database.pool, id).await
}

async fn insert_api_key_account(database: &TestDatabase, id: &str) {
    let repository = PgProviderAccountRepository::new(database.pool.clone());
    let mut account = super::provider_accounts::account(id, id);
    account.authentication_kind = "api_key".into();
    repository.insert_provider_account(account).await.unwrap();
}

async fn revision(pool: &sqlx::PgPool, id: &str) -> u64 {
    PgProviderAccountRepository::new(pool.clone())
        .load_current_credential(&ProviderAccountId::new(id).unwrap())
        .await
        .unwrap()
        .account
        .revision()
        .get()
}

async fn claim(
    database: &TestDatabase,
    key_id: &ClientApiKeyId,
    session: &ProviderSessionAffinityKey,
    owner: &ProviderAccountId,
    credential_revision: u64,
) {
    let outcome = PgCodexSessionOwnership::new(database.pool.clone())
        .claim(&CodexSessionClaim {
            key_id: key_id.clone(),
            session_key: session.clone(),
            account_id: owner.clone(),
            credential_revision,
        })
        .await
        .unwrap();
    assert_eq!(outcome, CodexSessionClaimOutcome::Owned);
}

async fn append_operation(
    database: &TestDatabase,
    key_id: &ClientApiKeyId,
    session: &ProviderSessionAffinityKey,
    replica: CodexNotesReplica,
    kind: CodexNotesOperationKind,
    request: &str,
) -> CodexNotesOperation {
    PgCodexSessionState::new(database.pool.clone())
        .append_notes_operation(&NewCodexNotesOperation {
            key_id: key_id.clone(),
            session_key: session.clone(),
            replica,
            agent_name: "/root".to_owned(),
            kind,
            request: request.to_owned(),
        })
        .await
        .unwrap()
}

async fn record_operation(
    database: &TestDatabase,
    key_id: &ClientApiKeyId,
    session: &ProviderSessionAffinityKey,
    replica: CodexNotesReplica,
    kind: CodexNotesOperationKind,
    request: &str,
    response: &str,
) -> CodexNotesOperation {
    let store = PgCodexSessionState::new(database.pool.clone());
    let operation = append_operation(database, key_id, session, replica, kind, request).await;
    store
        .start_notes_operation(key_id, session, operation.seq)
        .await
        .unwrap();
    store
        .complete_notes_operation(&CodexNotesCompletion {
            key_id: key_id.clone(),
            session_key: session.clone(),
            seq: operation.seq,
            status: CodexNotesOperationStatus::Responded,
            response: Some(response.to_owned()),
            response_status: Some(200),
        })
        .await
        .unwrap()
}

fn publish_to(
    key_id: &ClientApiKeyId,
    session: &ProviderSessionAffinityKey,
    target: &ProviderAccountId,
    credential_revision: u64,
    replica: &CodexNotesReplica,
    through_seq: u64,
) -> CodexNotesPublish {
    CodexNotesPublish {
        key_id: key_id.clone(),
        session_key: session.clone(),
        expected_generation: 0,
        owner: target.clone(),
        credential_revision,
        replica: replica.clone(),
        through_seq,
    }
}

fn session_key(value: &str) -> ProviderSessionAffinityKey {
    ProviderSessionAffinityKey::try_new(value).unwrap()
}

fn history_window(thread: &str, number: u64) -> CodexHistoryWindow {
    CodexHistoryWindow {
        window_id: CodexNativeWindowId::try_new(format!("{thread}:{number}")).unwrap(),
        agent_name: Some("/root".to_owned()),
        context_window_id: Some(CONTEXT_WINDOW_ID.to_owned()),
    }
}

fn history_item(
    item_id: &str,
    role: Option<CodexHistoryRole>,
    content_type: &str,
    raw: &str,
    text: Option<&str>,
) -> CodexHistoryItem {
    CodexHistoryItem {
        item_id: item_id.to_owned(),
        role,
        tool_namespace: None,
        tool_name: None,
        content_type: content_type.to_owned(),
        raw: raw.to_owned(),
        text: text.map(ToOwned::to_owned),
        identity: format!("identity-{item_id}"),
    }
}
