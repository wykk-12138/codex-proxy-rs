//! 验证原生 notes 的会话内 seq、写入日志、冻结重建目标、发布前进度与互斥范围

use std::sync::{Arc, Mutex};

use futures::{FutureExt as _, executor::block_on, future::BoxFuture};
use gateway_core::{
    account::ProviderAccountId,
    engine::codex_notes::{
        CodexNotesDomain, CodexNotesError, CodexNotesRebuild, CodexNotesService,
    },
    policy::ClientApiKeyId,
    provider_ports::{
        CodexHistoryPendingInput, CodexHistoryRequest, CodexHistoryResponse,
        CodexHistoryResponseAnchor, CodexHistoryStoredItem, CodexHistoryStoredWindow,
        CodexNativeWindowId, CodexNotesCompletion, CodexNotesGeneration, CodexNotesOperation,
        CodexNotesOperationKind, CodexNotesOperationStatus, CodexNotesPublish,
        CodexNotesPublishOutcome, CodexNotesReplay, CodexNotesReplica, CodexSessionContentPort,
        CodexSessionOrigin, CodexSessionState, CodexSessionStatePort, NewCodexNotesOperation,
        ProviderSessionAffinityKey, ProviderStoreError, ProviderStoreErrorKind,
    },
};

fn key() -> ClientApiKeyId {
    ClientApiKeyId::new("key_codex_notes").expect("client key id")
}

fn session() -> ProviderSessionAffinityKey {
    ProviderSessionAffinityKey::try_new("session_codex_notes").expect("session key")
}

fn other_session() -> ProviderSessionAffinityKey {
    ProviderSessionAffinityKey::try_new("session_codex_notes_other").expect("session key")
}

fn account() -> ProviderAccountId {
    ProviderAccountId::new("acct_codex_notes").expect("account id")
}

fn target() -> ProviderAccountId {
    ProviderAccountId::new("acct_codex_notes_target").expect("account id")
}

fn unavailable(operation: &'static str) -> ProviderStoreError {
    ProviderStoreError::new(ProviderStoreErrorKind::Unavailable, operation)
}

fn state(origin: CodexSessionOrigin, replica: CodexNotesReplica) -> CodexSessionState {
    CodexSessionState {
        origin,
        owner: account(),
        generation: 4,
        notes: replica,
        notes_replay: None,
    }
}

/// 内存事实：会话状态、已保存的重建进度、按 seq 排列的操作与发布记录
#[derive(Default)]
struct MemoryNotesStore {
    state: Mutex<Option<CodexSessionState>>,
    operations: Mutex<Vec<CodexNotesOperation>>,
    publishes: Mutex<Vec<CodexNotesPublish>>,
}

impl MemoryNotesStore {
    fn with_state(state: CodexSessionState) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(Some(state)),
            ..Self::default()
        })
    }

    fn record(
        &self,
        kind: CodexNotesOperationKind,
        status: CodexNotesOperationStatus,
        request: &str,
    ) {
        let mut operations = self.operations.lock().expect("operations lock");
        let seq = operations.len() as u64 + 1;
        operations.push(CodexNotesOperation {
            seq,
            replica: CodexNotesReplica::native(account()),
            agent_name: "/root".to_owned(),
            kind,
            status,
            request: request.to_owned(),
            response: status
                .needs_replay()
                .then(|| r#"{"result":"ok","ciphertext":"resp"}"#.to_owned()),
            response_status: status.needs_replay().then_some(200),
        });
    }

    fn operations(&self) -> Vec<CodexNotesOperation> {
        self.operations.lock().expect("operations lock").clone()
    }

    fn replay(&self) -> Option<CodexNotesReplay> {
        self.state
            .lock()
            .expect("state lock")
            .as_ref()
            .and_then(|state| state.notes_replay.clone())
    }

    fn publishes(&self) -> Vec<CodexNotesPublish> {
        self.publishes.lock().expect("publishes lock").clone()
    }
}

impl CodexSessionStatePort for MemoryNotesStore {
    fn load_session<'a>(
        &'a self,
        _key_id: &'a ClientApiKeyId,
        _session_key: &'a ProviderSessionAffinityKey,
    ) -> BoxFuture<'a, Result<Option<CodexSessionState>, ProviderStoreError>> {
        let state = self.state.lock().expect("state lock").clone();
        Box::pin(async move { Ok(state) })
    }

    fn resolve_session_origin<'a>(
        &'a self,
        _key_id: &'a ClientApiKeyId,
        _session_key: &'a ProviderSessionAffinityKey,
        origin: CodexSessionOrigin,
    ) -> BoxFuture<'a, Result<CodexSessionState, ProviderStoreError>> {
        let mut current = self.state.lock().expect("state lock");
        let Some(state) = current.as_mut() else {
            return Box::pin(async { Err(unavailable("resolve Codex session origin")) });
        };
        if state.origin == CodexSessionOrigin::Unclassified {
            state.origin = origin;
        }
        let state = state.clone();
        Box::pin(async move { Ok(state) })
    }

    fn save_notes_replay<'a>(
        &'a self,
        _key_id: &'a ClientApiKeyId,
        _session_key: &'a ProviderSessionAffinityKey,
        replay: &'a CodexNotesReplay,
    ) -> BoxFuture<'a, Result<(), ProviderStoreError>> {
        let mut current = self.state.lock().expect("state lock");
        let Some(state) = current.as_mut() else {
            return Box::pin(async { Err(unavailable("save Codex notes replay")) });
        };
        state.notes_replay = Some(replay.clone());
        Box::pin(async { Ok(()) })
    }

    fn publish_notes_replay<'a>(
        &'a self,
        publish: &'a CodexNotesPublish,
    ) -> BoxFuture<'a, Result<CodexNotesPublishOutcome, ProviderStoreError>> {
        self.publishes
            .lock()
            .expect("publishes lock")
            .push(publish.clone());
        let mut current = self.state.lock().expect("state lock");
        let Some(state) = current.as_mut() else {
            return Box::pin(async { Err(unavailable("publish Codex notes replay")) });
        };
        if state.generation != publish.expected_generation {
            let state = state.clone();
            return Box::pin(async move { Ok(CodexNotesPublishOutcome::GenerationChanged(state)) });
        }
        state.owner = publish.owner.clone();
        state.notes = publish.replica.clone();
        state.generation = state.generation.saturating_add(1);
        state.notes_replay = None;
        let state = state.clone();
        Box::pin(async move { Ok(CodexNotesPublishOutcome::Published(state)) })
    }
}

impl CodexSessionContentPort for MemoryNotesStore {
    fn commit_history_request<'a>(
        &'a self,
        _record: &'a CodexHistoryRequest,
    ) -> BoxFuture<'a, Result<(), ProviderStoreError>> {
        Box::pin(async { Err(unavailable("commit Codex history request")) })
    }

    fn commit_history_response<'a>(
        &'a self,
        _record: &'a CodexHistoryResponse,
    ) -> BoxFuture<'a, Result<(), ProviderStoreError>> {
        Box::pin(async { Err(unavailable("commit Codex history response")) })
    }

    fn load_history_response<'a>(
        &'a self,
        _key_id: &'a ClientApiKeyId,
        _session_key: &'a ProviderSessionAffinityKey,
        _response_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<CodexHistoryResponseAnchor>, ProviderStoreError>> {
        Box::pin(async { Err(unavailable("load Codex history response")) })
    }

    fn load_latest_history_response<'a>(
        &'a self,
        _key_id: &'a ClientApiKeyId,
        _session_key: &'a ProviderSessionAffinityKey,
        _window_id: &'a CodexNativeWindowId,
    ) -> BoxFuture<'a, Result<Option<CodexHistoryResponseAnchor>, ProviderStoreError>> {
        Box::pin(async { Ok(None) })
    }

    fn load_history_windows<'a>(
        &'a self,
        _key_id: &'a ClientApiKeyId,
        _session_key: &'a ProviderSessionAffinityKey,
    ) -> BoxFuture<'a, Result<Vec<CodexHistoryStoredWindow>, ProviderStoreError>> {
        Box::pin(async { Err(unavailable("load Codex history windows")) })
    }

    fn load_history_items<'a>(
        &'a self,
        _key_id: &'a ClientApiKeyId,
        _session_key: &'a ProviderSessionAffinityKey,
    ) -> BoxFuture<'a, Result<Vec<CodexHistoryStoredItem>, ProviderStoreError>> {
        Box::pin(async { Err(unavailable("load Codex history items")) })
    }

    fn load_history_window_items<'a>(
        &'a self,
        _key_id: &'a ClientApiKeyId,
        _session_key: &'a ProviderSessionAffinityKey,
        _window_id: &'a CodexNativeWindowId,
    ) -> BoxFuture<'a, Result<Vec<CodexHistoryStoredItem>, ProviderStoreError>> {
        Box::pin(async { Err(unavailable("load Codex history window items")) })
    }

    // 本用例集只验证 notes 日志与发布，不跑 history 采集；发送前快照按存储合同如实失败
    fn save_history_pending_input<'a>(
        &'a self,
        _key_id: &'a ClientApiKeyId,
        _session_key: &'a ProviderSessionAffinityKey,
        _pending: &'a CodexHistoryPendingInput,
    ) -> BoxFuture<'a, Result<(), ProviderStoreError>> {
        Box::pin(async { Err(unavailable("save Codex history pending input")) })
    }

    fn load_history_pending_input<'a>(
        &'a self,
        _key_id: &'a ClientApiKeyId,
        _session_key: &'a ProviderSessionAffinityKey,
        _window_id: &'a CodexNativeWindowId,
    ) -> BoxFuture<'a, Result<Option<CodexHistoryPendingInput>, ProviderStoreError>> {
        Box::pin(async { Ok(None) })
    }

    fn append_notes_operation<'a>(
        &'a self,
        operation: &'a NewCodexNotesOperation,
    ) -> BoxFuture<'a, Result<CodexNotesOperation, ProviderStoreError>> {
        let mut operations = self.operations.lock().expect("operations lock");
        let seq = operations.len() as u64 + 1;
        let recorded = CodexNotesOperation {
            seq,
            replica: operation.replica.clone(),
            agent_name: operation.agent_name.clone(),
            kind: operation.kind,
            status: CodexNotesOperationStatus::Unsent,
            request: operation.request.clone(),
            response: None,
            response_status: None,
        };
        operations.push(recorded.clone());
        Box::pin(async move { Ok(recorded) })
    }

    fn start_notes_operation<'a>(
        &'a self,
        _key_id: &'a ClientApiKeyId,
        _session_key: &'a ProviderSessionAffinityKey,
        seq: u64,
    ) -> BoxFuture<'a, Result<CodexNotesOperation, ProviderStoreError>> {
        let mut operations = self.operations.lock().expect("operations lock");
        let Some(operation) = operations.iter_mut().find(|operation| operation.seq == seq) else {
            return Box::pin(async { Err(unavailable("start Codex notes operation")) });
        };
        operation.status = CodexNotesOperationStatus::InFlight;
        let operation = operation.clone();
        Box::pin(async move { Ok(operation) })
    }

    fn complete_notes_operation<'a>(
        &'a self,
        completion: &'a CodexNotesCompletion,
    ) -> BoxFuture<'a, Result<CodexNotesOperation, ProviderStoreError>> {
        let mut operations = self.operations.lock().expect("operations lock");
        let Some(operation) = operations
            .iter_mut()
            .find(|operation| operation.seq == completion.seq)
        else {
            return Box::pin(async { Err(unavailable("complete Codex notes operation")) });
        };
        operation.status = completion.status;
        operation.response.clone_from(&completion.response);
        operation.response_status = completion.response_status;
        let operation = operation.clone();
        Box::pin(async move { Ok(operation) })
    }

    fn load_notes_operations<'a>(
        &'a self,
        _key_id: &'a ClientApiKeyId,
        _session_key: &'a ProviderSessionAffinityKey,
    ) -> BoxFuture<'a, Result<Vec<CodexNotesOperation>, ProviderStoreError>> {
        Box::pin(async move { Ok(self.operations()) })
    }
}

fn service(store: &Arc<MemoryNotesStore>) -> CodexNotesService {
    CodexNotesService::new(store.clone(), store.clone(), true)
}

fn managed_store() -> Arc<MemoryNotesStore> {
    MemoryNotesStore::with_state(state(
        CodexSessionOrigin::Managed,
        CodexNotesReplica::native(account()),
    ))
}

#[test]
fn recorded_writes_keep_sequence_and_original_ciphertext() {
    let store = managed_store();
    let service = service(&store);
    let write = r#"{"context":{"session_id":"s","current_agent_name":"/root"},"path":"/root/a.md","text":"cipher-write"}"#;
    let append = r#"{"context":{"session_id":"s","current_agent_name":"/root"},"path":"/root/a.md","text":"cipher-append"}"#;
    let first = block_on(service.append_operation(
        &key(),
        &session(),
        CodexNotesReplica::native(account()),
        CodexNotesOperationKind::WriteFile,
        "/root".to_owned(),
        write.to_owned(),
    ))
    .expect("record write");
    let second = block_on(service.append_operation(
        &key(),
        &session(),
        CodexNotesReplica::native(account()),
        CodexNotesOperationKind::AppendToFile,
        "/root".to_owned(),
        append.to_owned(),
    ))
    .expect("record append");
    assert_eq!((first.seq, second.seq), (1, 2));
    assert_eq!(first.status, CodexNotesOperationStatus::Unsent);
    // 记录只保存原请求，不在本地改写或解密密文字段
    assert_eq!(second.request, append);
    let started =
        block_on(service.start_operation(&key(), &session(), second.seq)).expect("start append");
    assert_eq!(started.status, CodexNotesOperationStatus::InFlight);
    block_on(service.complete_operation(&CodexNotesCompletion {
        key_id: key(),
        session_key: session(),
        seq: second.seq,
        status: CodexNotesOperationStatus::Responded,
        response: Some(r#"{"ciphertext":"resp"}"#.to_owned()),
        response_status: Some(200),
    }))
    .expect("complete append");
    let operations = block_on(service.operations(&key(), &session())).expect("operations");
    assert_eq!(operations.len(), 2);
    assert_eq!(operations[1].status, CodexNotesOperationStatus::Responded);
}

#[test]
fn identical_content_twice_is_two_real_operations() {
    let store = managed_store();
    let service = service(&store);
    let request = r#"{"context":{"session_id":"s","current_agent_name":"/root"},"path":"/root/a.md","text":"same"}"#;
    for _ in 0..2 {
        block_on(service.append_operation(
            &key(),
            &session(),
            CodexNotesReplica::native(account()),
            CodexNotesOperationKind::AppendToFile,
            "/root".to_owned(),
            request.to_owned(),
        ))
        .expect("record append");
    }
    assert_eq!(store.operations().len(), 2);
}

#[test]
fn rebuild_freezes_terminal_log_and_replays_responded_operations_in_order() {
    let store = managed_store();
    let service = service(&store);
    store.record(
        CodexNotesOperationKind::WriteFile,
        CodexNotesOperationStatus::Responded,
        r#"{"text":"one"}"#,
    );
    store.record(
        CodexNotesOperationKind::AppendToFile,
        CodexNotesOperationStatus::Failed,
        r#"{"text":"two"}"#,
    );
    store.record(
        CodexNotesOperationKind::AppendToFile,
        CodexNotesOperationStatus::Responded,
        r#"{"text":"three"}"#,
    );
    let rebuild = block_on(service.begin_rebuild(&key(), &session(), &target())).expect("rebuild");
    assert_eq!(rebuild.through_seq, 3);
    assert_eq!(rebuild.expected_generation, 4);
    assert_eq!(
        rebuild
            .steps
            .iter()
            .map(|step| step.seq)
            .collect::<Vec<_>>(),
        [1, 3]
    );
    // 冻结目标与零进度已经持久；目标域是目标账号的自建新域
    let replay = store.replay().expect("saved replay target");
    assert_eq!(replay.replica.account(), &target());
    assert!(replay.replica.generation().is_some());
    assert_eq!(
        (replay.frozen_through_seq, replay.replayed_through_seq),
        (3, 0)
    );
    assert_eq!(replay.replica, rebuild.replica());
}

#[test]
fn failed_tail_is_covered_by_the_last_replayed_step() {
    let store = managed_store();
    let service = service(&store);
    store.record(
        CodexNotesOperationKind::WriteFile,
        CodexNotesOperationStatus::Responded,
        r#"{"text":"one"}"#,
    );
    store.record(
        CodexNotesOperationKind::AppendToFile,
        CodexNotesOperationStatus::Failed,
        r#"{"text":"two"}"#,
    );
    let mut rebuild =
        block_on(service.begin_rebuild(&key(), &session(), &target())).expect("rebuild");
    assert_eq!(rebuild.through_seq, 2);
    assert_eq!(
        rebuild
            .steps
            .iter()
            .map(|step| step.seq)
            .collect::<Vec<_>>(),
        [1]
    );
    assert!(!rebuild.is_replayed());
    block_on(service.advance_rebuild(&key(), &session(), &mut rebuild, 1)).expect("advance");
    // 末尾 Failed 没有待发送内容，重放完最后一步即到达冻结位置
    assert_eq!(rebuild.replayed_through_seq, 2);
    assert!(rebuild.is_replayed());
    assert_eq!(
        store.replay().expect("saved progress").replayed_through_seq,
        2
    );
    assert!(matches!(
        block_on(service.publish_rebuild(&key(), &session(), &rebuild, 9)),
        Ok(CodexNotesPublishOutcome::Published(_))
    ));
}

#[test]
fn rebuild_rejects_unsettled_operations_without_saving_target() {
    for status in [
        CodexNotesOperationStatus::Unsent,
        CodexNotesOperationStatus::InFlight,
        CodexNotesOperationStatus::Unknown,
    ] {
        let store = managed_store();
        let service = service(&store);
        store.record(
            CodexNotesOperationKind::WriteFile,
            status,
            r#"{"text":"one"}"#,
        );
        let error = block_on(service.begin_rebuild(&key(), &session(), &target()))
            .expect_err("unsettled operation blocks rebuild");
        assert!(matches!(
            error,
            CodexNotesError::OperationUnsettled { seq: 1 }
        ));
        assert!(store.replay().is_none());
    }
}

#[test]
fn rebuild_progress_must_follow_the_frozen_order() {
    let store = managed_store();
    let service = service(&store);
    for text in ["one", "two"] {
        store.record(
            CodexNotesOperationKind::AppendToFile,
            CodexNotesOperationStatus::Responded,
            text,
        );
    }
    let mut rebuild =
        block_on(service.begin_rebuild(&key(), &session(), &target())).expect("rebuild");
    assert!(matches!(
        block_on(service.advance_rebuild(&key(), &session(), &mut rebuild, 2)),
        Err(CodexNotesError::ReplayOutOfOrder)
    ));
    block_on(service.advance_rebuild(&key(), &session(), &mut rebuild, 1))
        .expect("advance first step");
    assert_eq!(
        store.replay().expect("saved progress").replayed_through_seq,
        1
    );
    block_on(service.advance_rebuild(&key(), &session(), &mut rebuild, 2))
        .expect("advance second step");
    assert_eq!(rebuild.replayed_through_seq, 2);
    assert!(rebuild.is_replayed());
}

#[test]
fn publish_requires_complete_replay_and_the_observed_generation() {
    let store = managed_store();
    let service = service(&store);
    store.record(
        CodexNotesOperationKind::WriteFile,
        CodexNotesOperationStatus::Responded,
        r#"{"text":"one"}"#,
    );
    let rebuild = block_on(service.begin_rebuild(&key(), &session(), &target())).expect("rebuild");
    assert!(matches!(
        block_on(service.publish_rebuild(&key(), &session(), &rebuild, 7)),
        Err(CodexNotesError::ReplayNotComplete)
    ));
    assert!(store.publishes().is_empty());

    let mut rebuild = rebuild;
    block_on(service.advance_rebuild(&key(), &session(), &mut rebuild, 1)).expect("advance");
    let published =
        block_on(service.publish_rebuild(&key(), &session(), &rebuild, 7)).expect("publish");
    let published = match published {
        CodexNotesPublishOutcome::Published(state) => state,
        other => panic!("expected published state, got {other:?}"),
    };
    assert_eq!(published.owner, target());
    assert_eq!(published.notes, rebuild.replica());
    assert_eq!(published.generation, 5);
    let publish = store.publishes().pop().expect("publish input");
    assert_eq!(publish.expected_generation, 4);
    assert_eq!(publish.credential_revision, 7);
    assert_eq!(publish.through_seq, 1);
}

#[test]
fn legacy_sessions_keep_the_native_domain_and_are_never_rebuilt() {
    let store = MemoryNotesStore::with_state(state(
        CodexSessionOrigin::Legacy,
        CodexNotesReplica::native(account()),
    ));
    let service = service(&store);
    let domain = block_on(service.domain(&key(), &session()))
        .expect("domain")
        .expect("claimed session");
    assert!(!domain.records_operations());
    assert_eq!(
        domain.outbound_session_id("client-session"),
        "client-session"
    );
    assert!(matches!(
        block_on(service.begin_rebuild(&key(), &session(), &target())),
        Err(CodexNotesError::LegacySession)
    ));
    assert!(store.replay().is_none());
}

#[test]
fn outbound_session_id_follows_the_published_replica() {
    let generation = CodexNotesGeneration::generate();
    let replica = CodexNotesReplica::rebuilt(account(), generation.clone());
    let domain = CodexNotesDomain {
        origin: CodexSessionOrigin::Managed,
        replica,
    };
    assert_eq!(
        domain.outbound_session_id("client-session"),
        generation.expose_to_store()
    );
    assert!(domain.records_operations());
    let unclassified = CodexNotesDomain {
        origin: CodexSessionOrigin::Unclassified,
        replica: CodexNotesReplica::native(account()),
    };
    // 未判定会话的先行写入也要记录，原生域仍沿用客户端会话 ID
    assert!(unclassified.records_operations());
    assert_eq!(
        unclassified.outbound_session_id("client-session"),
        "client-session"
    );
}

#[test]
fn session_guard_is_exclusive_per_key_and_session() {
    let store = managed_store();
    let service = Arc::new(service(&store));
    let first = block_on(service.lock_session(&key(), &session()));
    // 同一会话的第二个写方必须等待，不同会话互不阻塞
    assert!(
        service
            .lock_session(&key(), &session())
            .now_or_never()
            .is_none()
    );
    drop(block_on(service.lock_session(&key(), &other_session())));
    drop(first);
    assert!(
        service
            .lock_session(&key(), &session())
            .now_or_never()
            .is_some()
    );
}

#[test]
fn missing_session_reports_absent() {
    let store = Arc::new(MemoryNotesStore::default());
    let service = service(&store);
    assert!(
        block_on(service.domain(&key(), &session()))
            .expect("domain")
            .is_none()
    );
    assert!(matches!(
        block_on(service.begin_rebuild(&key(), &session(), &target())),
        Err(CodexNotesError::SessionAbsent)
    ));
}

#[test]
fn empty_log_rebuild_publishes_through_zero() {
    let store = managed_store();
    let service = service(&store);
    let rebuild: CodexNotesRebuild =
        block_on(service.begin_rebuild(&key(), &session(), &target())).expect("rebuild");
    assert_eq!(rebuild.through_seq, 0);
    assert!(rebuild.steps.is_empty());
    assert!(rebuild.is_replayed());
    let published = block_on(service.publish_rebuild(&key(), &session(), &rebuild, 3))
        .expect("publish empty replica");
    assert!(matches!(published, CodexNotesPublishOutcome::Published(_)));
}

#[test]
fn disabled_admission_keeps_managed_domains_and_fixes_new_ones_as_legacy() {
    // 已有 managed 会话继续使用已发布域，关闭开关不影响本地 notes 与切号
    let managed = managed_store();
    let service = CodexNotesService::new(managed.clone(), managed, false);
    let domain = block_on(service.domain(&key(), &session()))
        .expect("managed domain")
        .expect("claimed session");
    assert_eq!(domain.origin, CodexSessionOrigin::Managed);
    assert!(domain.records_operations());

    // 新会话在首次辅助请求就固定为 legacy：不记录新操作，也不进入迁移
    let fresh = MemoryNotesStore::with_state(state(
        CodexSessionOrigin::Unclassified,
        CodexNotesReplica::native(account()),
    ));
    let service = CodexNotesService::new(fresh.clone(), fresh.clone(), false);
    let domain = block_on(service.domain(&key(), &session()))
        .expect("unclassified domain")
        .expect("claimed session");
    assert_eq!(domain.origin, CodexSessionOrigin::Legacy);
    assert!(!domain.records_operations());
    assert_eq!(
        fresh
            .state
            .lock()
            .expect("state lock")
            .as_ref()
            .map(|state| state.origin),
        Some(CodexSessionOrigin::Legacy)
    );
}
