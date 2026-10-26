//! 验证原生 notes 六端点的已发布域映射、写入日志真实语义与异常不重试

use std::{
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::SystemTime,
};

use bytes::Bytes;
use futures::{SinkExt as _, StreamExt as _, future::BoxFuture};
use gateway_core::engine::ContinuationAttempt;
use gateway_core::{
    account::{
        ProviderAccountId, ProviderAccountStore as _, QuotaAccessChange, QuotaAccessState,
        QuotaEvidence, QuotaState,
    },
    engine::{
        codex::{CodexAuxiliaryOperation, CodexAuxiliaryRequest},
        codex_history::CodexHistoryService,
        codex_notes::{CodexNotesService, CodexNotesSessionExpectation},
        execution::ExecutionService as _,
        middleware::{MiddlewareHeader, MiddlewareResponse},
        provider::Provider as _,
    },
    error::ProviderErrorKind,
    lifecycle::CancellationToken,
    operation::{GenerateRequest, Operation, ProtocolPayload, ProviderSessionState},
    policy::{ClientApiKeyId, CodexAccessMode},
    provider_ports::{
        CodexHistoryItem, CodexHistoryPendingInput, CodexHistoryRequest, CodexHistoryResponse,
        CodexHistoryResponseAnchor, CodexHistoryRole, CodexHistoryStoredItem,
        CodexHistoryStoredWindow, CodexHistoryWindow, CodexHistoryWindowMetadata,
        CodexNativeWindowId, CodexNotesCompletion, CodexNotesGeneration, CodexNotesOperation,
        CodexNotesOperationKind, CodexNotesOperationStatus, CodexNotesPublish,
        CodexNotesPublishOutcome, CodexNotesReplay, CodexNotesReplica, CodexSessionContentPort,
        CodexSessionOrigin, CodexSessionState, CodexSessionStatePort, NewCodexNotesOperation,
        ProviderSessionAffinityKey, ProviderStoreError, ProviderStoreErrorKind,
    },
    task::{WorkerKind, WorkerRunnable},
    upstream::UpstreamSendState,
};
use serde_json::{Map, Value, json};
use tokio::{
    io::{AsyncReadExt as _, AsyncWriteExt as _},
    net::TcpListener,
    sync::Notify,
};
use tokio_tungstenite::tungstenite::Message;
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

use super::contract::{
    generate_with_session_context, planned_request, pool_context, provider_and_notes_delivery_task,
    provider_with_codex_sessions_and_native_notes,
    provider_with_codex_sessions_native_history_and_notes, spawn_notes_delivery,
};
use super::native::{
    native_service_with_mode, native_service_with_parallel_admissions, prepare, seed,
    seed_workspace,
};
use super::native_history::{CAPTURED_SSE, pool_turn_request, pool_turn_request_with_input};
use crate::support::{
    GATE_WAIT_TIMEOUT, Gate, MemoryAccountStore, MemoryCodexSessions, TestLeaseCoordinator,
};
use crate::transport::accept_codex_test_websocket;

const ACCOUNT: &str = "acct_native_bound";
const SESSION: &str = "session-native";
const REPLAYED: &str = r#"{"ciphertext":"resp"}"#;
const MIGRATE_SESSION: &str = "session-native-migrate";
const OWNER_A: &str = "acct_affinity_switch_a";
const OWNER_B: &str = "acct_affinity_switch_b";

fn unavailable(operation: &'static str) -> ProviderStoreError {
    ProviderStoreError::new(ProviderStoreErrorKind::Unavailable, operation)
}

fn account() -> ProviderAccountId {
    ProviderAccountId::new(ACCOUNT).expect("account id")
}

/// 额度恢复用例的用户输入：不带 ID，客户端只能靠位置与语义身份对齐
fn quota_input() -> Value {
    json!([{
        "type": "message",
        "role": "user",
        "content": [{"type": "input_text", "text": "hello"}]
    }])
}

fn state(origin: CodexSessionOrigin, replica: CodexNotesReplica) -> CodexSessionState {
    CodexSessionState {
        origin,
        owner: account(),
        generation: 1,
        notes: replica,
        notes_replay: None,
    }
}

/// 只回答会话状态、notes 日志、切换发布与 history 事实的内存存储；Key 由同一测试会话固定
///
/// history 部分按 Store 合同的可见语义实现：条目按提交顺序追加、同稳定 ID 幂等、
/// 响应锚保存当时的逻辑输入，供切号后的真实模型请求继续对齐
#[derive(Default)]
struct MemoryNotesStore {
    state: Mutex<Option<CodexSessionState>>,
    operations: Mutex<Vec<CodexNotesOperation>>,
    publishes: Mutex<Vec<CodexNotesPublish>>,
    history_items: Mutex<Vec<CodexHistoryStoredItem>>,
    history_anchors: Mutex<Vec<CodexHistoryResponseAnchor>>,
    history_pending: Mutex<Vec<CodexHistoryPendingInput>>,
    /// 未认领会话：认领之后才出现会话状态，用于验证 notes 先于模型请求到达
    /// 以及另一请求在认领期间按既有来源判定固定来源
    claiming: Mutex<Option<(Arc<MemoryCodexSessions>, CodexSessionOrigin)>>,
    /// 交付生命周期用例的确定性闸门与故障注入；常规用例保持未设置
    gates: Mutex<GateSet>,
    start_failure: Mutex<Option<StartFailure>>,
    complete_fails_after_commit: AtomicBool,
    completions: Arc<CompletionCounter>,
}

/// 可挂接闸门的持久阶段
#[derive(Clone, Copy, PartialEq, Eq)]
enum NotesStage {
    Append,
    Start,
    Complete,
}

/// 可挂接的持久阶段闸门：工作进入后停住，测试再决定取消、丢弃或放行
#[derive(Default)]
struct GateSet {
    append: Option<Arc<Gate>>,
    start: Option<Arc<Gate>>,
    complete: Option<Arc<Gate>>,
}

/// 结束观察的到达计数：测试据此等待真实落库，而不是按时间推测
#[derive(Default)]
struct CompletionCounter {
    completed: AtomicUsize,
    signal: Notify,
}

impl CompletionCounter {
    /// 等待真实落库到达；超时同样只用于定位测试驱动错误
    async fn wait(&self, count: usize) {
        tokio::time::timeout(GATE_WAIT_TIMEOUT, async {
            loop {
                let notified = self.signal.notified();
                if self.completed.load(Ordering::SeqCst) >= count {
                    return;
                }
                notified.await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "completion wait timed out: expected {count}, observed {}",
                self.completed.load(Ordering::SeqCst)
            )
        });
    }

    fn record(&self) {
        self.completed.fetch_add(1, Ordering::SeqCst);
        self.signal.notify_waiters();
    }
}

/// `start` 的错误注入：区分“可能已提交但响应丢失”和“确定没有开始”
#[derive(Clone, Copy)]
enum StartFailure {
    /// 先把状态推进到 `InFlight` 再返回错误
    Committed,
    /// 不改状态直接返回错误
    NotCommitted,
}

impl MemoryNotesStore {
    fn with_state(state: CodexSessionState) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(Some(state)),
            ..Self::default()
        })
    }

    /// 会话尚未认领：状态行只在归属认领之后出现，与真实 Store 的认领建行一致
    fn claiming(sessions: Arc<MemoryCodexSessions>, origin: CodexSessionOrigin) -> Arc<Self> {
        Arc::new(Self {
            claiming: Mutex::new(Some((sessions, origin))),
            ..Self::default()
        })
    }

    /// 模拟另一请求按既有来源判定把已存在的会话固定成给定来源
    fn resolve_session_as(&self, origin: CodexSessionOrigin) {
        if let Some(state) = self.state.lock().expect("state lock").as_mut() {
            state.origin = origin;
        }
    }

    /// 模拟另一请求在认领期间按既有来源判定把该会话固定成给定来源
    fn resolve_unclaimed_as(&self, origin: CodexSessionOrigin) {
        if let Some((_, current)) = self.claiming.lock().expect("claiming lock").as_mut() {
            *current = origin;
        }
    }

    fn gate(&self, stage: NotesStage) -> Option<Arc<Gate>> {
        let gates = self.gates.lock().expect("gates lock");
        match stage {
            NotesStage::Append => gates.append.clone(),
            NotesStage::Start => gates.start.clone(),
            NotesStage::Complete => gates.complete.clone(),
        }
    }

    fn attach_gate(&self, stage: NotesStage, gate: &Arc<Gate>) {
        let mut gates = self.gates.lock().expect("gates lock");
        match stage {
            NotesStage::Append => gates.append = Some(Arc::clone(gate)),
            NotesStage::Start => gates.start = Some(Arc::clone(gate)),
            NotesStage::Complete => gates.complete = Some(Arc::clone(gate)),
        }
    }

    async fn enter(&self, stage: NotesStage) {
        if let Some(gate) = self.gate(stage) {
            gate.enter().await;
        }
    }

    fn completions(&self) -> &Arc<CompletionCounter> {
        &self.completions
    }

    fn fail_start(&self, failure: StartFailure) {
        *self.start_failure.lock().expect("start failure lock") = Some(failure);
    }

    fn complete_fails_after_commit(&self) {
        self.complete_fails_after_commit
            .store(true, Ordering::SeqCst);
    }

    fn status(&self, seq: u64) -> Option<CodexNotesOperationStatus> {
        self.operations()
            .into_iter()
            .find(|operation| operation.seq == seq)
            .map(|operation| operation.status)
    }

    fn history_item_ids(&self) -> Vec<String> {
        self.history_items
            .lock()
            .expect("history items lock")
            .iter()
            .map(|item| item.item.item_id.clone())
            .collect()
    }

    /// 预置一条已持久条目，模拟切换前 A 已完整采集的一轮上下文
    fn seed_history_item(&self, window: &CodexHistoryWindow, item: CodexHistoryItem) {
        let mut items = self.history_items.lock().expect("history items lock");
        let ordinal = items.len() as u64;
        items.push(CodexHistoryStoredItem {
            window_id: window.window_id.clone(),
            ordinal,
            item,
        });
    }

    fn history_anchor(&self, response_id: &str) -> Option<CodexHistoryResponseAnchor> {
        self.history_anchors
            .lock()
            .expect("history anchors lock")
            .iter()
            .find(|anchor| anchor.response_id == response_id)
            .cloned()
    }

    /// 当前窗口的发送前逻辑输入快照
    fn history_pending(&self, window_value: &str) -> Option<CodexHistoryPendingInput> {
        self.history_pending
            .lock()
            .expect("history pending lock")
            .iter()
            .rev()
            .find(|pending| pending.window.window_id.expose_to_store() == window_value)
            .cloned()
    }

    /// 预置一条已锚定的父响应，模拟切换前 A 已完整采集的一轮
    fn seed_history_response(
        &self,
        window: CodexHistoryWindow,
        response_id: &str,
        input: &[&str],
        output: &[&str],
    ) {
        self.history_anchors
            .lock()
            .expect("history anchors lock")
            .push(CodexHistoryResponseAnchor {
                response_id: response_id.to_owned(),
                window,
                previous_response_id: None,
                input: input.iter().map(|value| (*value).to_owned()).collect(),
                output: output.iter().map(|value| (*value).to_owned()).collect(),
            });
    }

    fn operations(&self) -> Vec<CodexNotesOperation> {
        self.operations.lock().expect("operations lock").clone()
    }

    fn publishes(&self) -> Vec<CodexNotesPublish> {
        self.publishes.lock().expect("publishes lock").clone()
    }

    fn owner(&self) -> Option<ProviderAccountId> {
        self.state
            .lock()
            .expect("state lock")
            .as_ref()
            .map(|state| state.owner.clone())
    }

    fn notes(&self) -> Option<CodexNotesReplica> {
        self.state
            .lock()
            .expect("state lock")
            .as_ref()
            .map(|state| state.notes.clone())
    }

    /// 预置一条已完整响应的历史写入；重放只拾取这类终态
    fn seed_responded(&self, kind: CodexNotesOperationKind, request: &str) {
        let mut operations = self.operations.lock().expect("operations lock");
        let seq = operations.len() as u64 + 1;
        operations.push(CodexNotesOperation {
            seq,
            replica: CodexNotesReplica::native(account()),
            agent_name: "/root".to_owned(),
            kind,
            status: CodexNotesOperationStatus::Responded,
            request: request.to_owned(),
            response: Some(REPLAYED.to_owned()),
            response_status: Some(200),
        });
    }
}

impl CodexSessionStatePort for MemoryNotesStore {
    fn load_session<'a>(
        &'a self,
        key_id: &'a ClientApiKeyId,
        session_key: &'a ProviderSessionAffinityKey,
    ) -> BoxFuture<'a, Result<Option<CodexSessionState>, ProviderStoreError>> {
        let state = self.state.lock().expect("state lock").clone();
        let claiming = self.claiming.lock().expect("claiming lock").clone();
        Box::pin(async move {
            if let Some(state) = state {
                return Ok(Some(state));
            }
            let Some((sessions, origin)) = claiming else {
                return Ok(None);
            };
            // 未认领就没有状态行；首绑成功之后才按认领的 owner 建立会话事实
            let Some(owner) = sessions.owner_of(key_id, session_key) else {
                return Ok(None);
            };
            let state = CodexSessionState {
                origin,
                notes: CodexNotesReplica::native(owner.clone()),
                owner,
                generation: 4,
                notes_replay: None,
            };
            *self.state.lock().expect("state lock") = Some(state.clone());
            Ok(Some(state))
        })
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

    /// 与 Store 合同一致：只发布已保存并完成重放的冻结目标
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
        let replay_ready = state.notes_replay.as_ref().is_some_and(|replay| {
            replay.replica == publish.replica
                && replay.frozen_through_seq == publish.through_seq
                && replay.replayed_through_seq == publish.through_seq
        });
        if !replay_ready {
            return Box::pin(async { Ok(CodexNotesPublishOutcome::ReplayNotReady) });
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
        record: &'a CodexHistoryRequest,
    ) -> BoxFuture<'a, Result<(), ProviderStoreError>> {
        let mut items = self.history_items.lock().expect("history items lock");
        for item in &record.items {
            // 同一窗口内同一稳定 ID 的重复提交是幂等 no-op
            if items
                .iter()
                .any(|stored| stored.item.item_id == item.item_id)
            {
                continue;
            }
            let ordinal = items.len() as u64;
            items.push(CodexHistoryStoredItem {
                window_id: record.window.window_id.clone(),
                ordinal,
                item: item.clone(),
            });
        }
        Box::pin(async { Ok(()) })
    }

    fn commit_history_response<'a>(
        &'a self,
        record: &'a CodexHistoryResponse,
    ) -> BoxFuture<'a, Result<(), ProviderStoreError>> {
        let mut items = self.history_items.lock().expect("history items lock");
        for item in &record.items {
            if items
                .iter()
                .any(|stored| stored.item.item_id == item.item_id)
            {
                continue;
            }
            let ordinal = items.len() as u64;
            items.push(CodexHistoryStoredItem {
                window_id: record.window.window_id.clone(),
                ordinal,
                item: item.clone(),
            });
        }
        drop(items);
        let mut anchors = self.history_anchors.lock().expect("history anchors lock");
        match anchors
            .iter_mut()
            .find(|anchor| anchor.response_id == record.response_id)
        {
            Some(anchor) => anchor
                .output
                .extend(record.items.iter().map(|item| item.item_id.clone())),
            None => anchors.push(CodexHistoryResponseAnchor {
                response_id: record.response_id.clone(),
                window: record.window.clone(),
                previous_response_id: record.previous_response_id.clone(),
                input: record.input.clone(),
                output: record
                    .items
                    .iter()
                    .map(|item| item.item_id.clone())
                    .collect(),
            }),
        }
        Box::pin(async { Ok(()) })
    }

    fn load_history_response<'a>(
        &'a self,
        _key_id: &'a ClientApiKeyId,
        _session_key: &'a ProviderSessionAffinityKey,
        response_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<CodexHistoryResponseAnchor>, ProviderStoreError>> {
        let anchor = self
            .history_anchors
            .lock()
            .expect("history anchors lock")
            .iter()
            .find(|anchor| anchor.response_id == response_id)
            .cloned();
        Box::pin(async move { Ok(anchor) })
    }

    fn load_latest_history_response<'a>(
        &'a self,
        _key_id: &'a ClientApiKeyId,
        _session_key: &'a ProviderSessionAffinityKey,
        window_id: &'a CodexNativeWindowId,
    ) -> BoxFuture<'a, Result<Option<CodexHistoryResponseAnchor>, ProviderStoreError>> {
        let anchor = self
            .history_anchors
            .lock()
            .expect("history anchors lock")
            .iter()
            .rfind(|anchor| anchor.window.window_id == *window_id)
            .cloned();
        Box::pin(async move { Ok(anchor) })
    }

    fn load_history_windows<'a>(
        &'a self,
        _key_id: &'a ClientApiKeyId,
        _session_key: &'a ProviderSessionAffinityKey,
    ) -> BoxFuture<'a, Result<Vec<CodexHistoryStoredWindow>, ProviderStoreError>> {
        let items = self
            .history_items
            .lock()
            .expect("history items lock")
            .clone();
        Box::pin(async move {
            let mut windows: Vec<CodexHistoryStoredWindow> = Vec::new();
            for item in items {
                match windows
                    .iter_mut()
                    .find(|window| window.window_id == item.window_id)
                {
                    Some(window) => window.item_count += 1,
                    None => windows.push(CodexHistoryStoredWindow {
                        window_id: item.window_id.clone(),
                        item_count: 1,
                        metadata: vec![CodexHistoryWindowMetadata {
                            agent_name: Some("/root".to_owned()),
                            context_window_id: None,
                        }],
                    }),
                }
            }
            Ok(windows)
        })
    }

    fn load_history_items<'a>(
        &'a self,
        _key_id: &'a ClientApiKeyId,
        _session_key: &'a ProviderSessionAffinityKey,
    ) -> BoxFuture<'a, Result<Vec<CodexHistoryStoredItem>, ProviderStoreError>> {
        let items = self
            .history_items
            .lock()
            .expect("history items lock")
            .clone();
        Box::pin(async move { Ok(items) })
    }

    fn load_history_window_items<'a>(
        &'a self,
        _key_id: &'a ClientApiKeyId,
        _session_key: &'a ProviderSessionAffinityKey,
        window_id: &'a CodexNativeWindowId,
    ) -> BoxFuture<'a, Result<Vec<CodexHistoryStoredItem>, ProviderStoreError>> {
        let items = self
            .history_items
            .lock()
            .expect("history items lock")
            .iter()
            .filter(|item| item.window_id == *window_id)
            .cloned()
            .collect::<Vec<_>>();
        Box::pin(async move { Ok(items) })
    }

    fn save_history_pending_input<'a>(
        &'a self,
        _key_id: &'a ClientApiKeyId,
        _session_key: &'a ProviderSessionAffinityKey,
        pending: &'a CodexHistoryPendingInput,
    ) -> BoxFuture<'a, Result<(), ProviderStoreError>> {
        let mut slots = self.history_pending.lock().expect("history pending lock");
        // 同一窗口只保留最近一次发送前请求的快照
        slots.retain(|slot| slot.window.window_id != pending.window.window_id);
        slots.push(pending.clone());
        Box::pin(async { Ok(()) })
    }

    fn load_history_pending_input<'a>(
        &'a self,
        _key_id: &'a ClientApiKeyId,
        _session_key: &'a ProviderSessionAffinityKey,
        window_id: &'a CodexNativeWindowId,
    ) -> BoxFuture<'a, Result<Option<CodexHistoryPendingInput>, ProviderStoreError>> {
        let pending = self
            .history_pending
            .lock()
            .expect("history pending lock")
            .iter()
            .find(|pending| pending.window.window_id == *window_id)
            .cloned();
        Box::pin(async move { Ok(pending) })
    }

    fn append_notes_operation<'a>(
        &'a self,
        operation: &'a NewCodexNotesOperation,
    ) -> BoxFuture<'a, Result<CodexNotesOperation, ProviderStoreError>> {
        Box::pin(async move {
            self.enter(NotesStage::Append).await;
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
            Ok(recorded)
        })
    }

    fn start_notes_operation<'a>(
        &'a self,
        _key_id: &'a ClientApiKeyId,
        _session_key: &'a ProviderSessionAffinityKey,
        seq: u64,
    ) -> BoxFuture<'a, Result<CodexNotesOperation, ProviderStoreError>> {
        Box::pin(async move {
            self.enter(NotesStage::Start).await;
            let failure = *self.start_failure.lock().expect("start failure lock");
            let mut operations = self.operations.lock().expect("operations lock");
            let Some(operation) = operations.iter_mut().find(|operation| operation.seq == seq)
            else {
                return Err(unavailable("start Codex notes operation"));
            };
            match failure {
                // 开始事实已经提交，但调用方只看到错误：读回必须是 InFlight
                Some(StartFailure::Committed) => {
                    operation.status = CodexNotesOperationStatus::InFlight;
                    Err(unavailable("start Codex notes operation"))
                }
                Some(StartFailure::NotCommitted) => Err(unavailable("start Codex notes operation")),
                None => {
                    operation.status = CodexNotesOperationStatus::InFlight;
                    Ok(operation.clone())
                }
            }
        })
    }

    fn complete_notes_operation<'a>(
        &'a self,
        completion: &'a CodexNotesCompletion,
    ) -> BoxFuture<'a, Result<CodexNotesOperation, ProviderStoreError>> {
        Box::pin(async move {
            self.enter(NotesStage::Complete).await;
            let mut operations = self.operations.lock().expect("operations lock");
            let Some(operation) = operations
                .iter_mut()
                .find(|operation| operation.seq == completion.seq)
            else {
                return Err(unavailable("complete Codex notes operation"));
            };
            operation.status = completion.status;
            operation.response.clone_from(&completion.response);
            operation.response_status = completion.response_status;
            let operation = operation.clone();
            drop(operations);
            self.completions().record();
            if self.complete_fails_after_commit.load(Ordering::SeqCst) {
                // 提交已经生效，但调用方只看到持久失败：不得据此重写终态
                return Err(unavailable("complete Codex notes operation"));
            }
            Ok(operation)
        })
    }

    fn load_notes_operations<'a>(
        &'a self,
        _key_id: &'a ClientApiKeyId,
        _session_key: &'a ProviderSessionAffinityKey,
    ) -> BoxFuture<'a, Result<Vec<CodexNotesOperation>, ProviderStoreError>> {
        Box::pin(async move { Ok(self.operations()) })
    }
}

fn notes_service(store: &Arc<MemoryNotesStore>) -> Arc<CodexNotesService> {
    notes_service_with_admission(store, true)
}

/// 关闭托管准入时，新会话按既有准入规则固定为 legacy
fn notes_service_with_admission(
    store: &Arc<MemoryNotesStore>,
    managed_admission: bool,
) -> Arc<CodexNotesService> {
    Arc::new(CodexNotesService::new(
        store.clone(),
        store.clone(),
        managed_admission,
    ))
}

fn history_service(store: &Arc<MemoryNotesStore>) -> Arc<CodexHistoryService> {
    Arc::new(CodexHistoryService::new(store.clone(), store.clone(), true))
}

fn history_window(thread: &str) -> CodexHistoryWindow {
    CodexHistoryWindow {
        window_id: CodexNativeWindowId::try_new(format!("{thread}:0")).expect("window id"),
        agent_name: Some("/root".to_owned()),
        context_window_id: None,
    }
}

fn json_from(raw: &str) -> Value {
    serde_json::from_str(raw).expect("raw item json")
}

/// 带官方 turn metadata 的模型请求；同一会话标识保证池亲和与既有用例一致
fn turn_operation(
    session: &str,
    window_id: &str,
    previous_response_id: Option<&str>,
    continued_owner: Option<&str>,
    input: &[&str],
    use_websocket: bool,
) -> Operation {
    let turn_metadata = json!({
        "window_id": window_id,
        "history_initialization": "new",
        "request_kind": "turn",
        "agent_name": "/root",
    })
    .to_string();
    let mut body = Map::from_iter([
        ("model".to_owned(), json!("gpt-5.4")),
        (
            "input".to_owned(),
            json!(input.iter().map(|item| json_from(item)).collect::<Vec<_>>()),
        ),
        (
            "client_metadata".to_owned(),
            json!({"session_id": session, "x-codex-turn-metadata": turn_metadata}),
        ),
    ]);
    if let Some(previous_response_id) = previous_response_id {
        body.insert(
            "previous_response_id".to_owned(),
            json!(previous_response_id),
        );
    }
    let request = GenerateRequest::from_protocol_payload(
        ProtocolPayload::json_object("openai", body)
            .expect("openai payload")
            .with_context(Map::from_iter([(
                "use_websocket".to_owned(),
                json!(use_websocket),
            )])),
    );
    let Some(owner) = continued_owner else {
        return Operation::Generate(request);
    };
    // 续接的持久状态属于 A；请求仍按同一会话标识派生池亲和
    let state = ProviderSessionState::new(
        "openai",
        Map::from_iter([
            ("account_id".to_owned(), json!(owner)),
            ("conversation_id".to_owned(), json!(session)),
            ("continuation_scope".to_owned(), json!("persisted")),
        ]),
    )
    .expect("provider session state");
    Operation::Generate(request.with_provider_session_state(state))
}

/// 等待测试上游任务正常结束；客户端断言失败也要让 panic 传到测试结果
async fn timeout_server(server: tokio::task::JoinHandle<()>) {
    tokio::time::timeout(std::time::Duration::from_secs(10), server)
        .await
        .expect("test upstream finished")
        .expect("test upstream task");
}

fn history_item(id: &str, role: &str, text: &str) -> CodexHistoryItem {
    CodexHistoryItem {
        item_id: id.to_owned(),
        role: CodexHistoryRole::from_wire(role),
        tool_namespace: None,
        tool_name: None,
        content_type: "message".to_owned(),
        identity: format!("{role}:{text}"),
        raw: json!({"type": "message", "id": id, "role": role}).to_string(),
        text: Some(text.to_owned()),
    }
}

fn notes_request(operation: CodexAuxiliaryOperation, body: &str) -> CodexAuxiliaryRequest {
    CodexAuxiliaryRequest {
        operation,
        query: None,
        headers: vec![MiddlewareHeader::new(
            "x-openai-encrypted-tool-arguments",
            Bytes::from_static(b"true"),
        )],
        body: Bytes::from(body.to_owned()),
    }
}

/// 官方 notes 写调用正文；密文字段原样带给上游与日志
fn write_body(text: &str) -> String {
    json!({
        "context": {"session_id": SESSION, "current_agent_name": "/root"},
        "path": "/root/notes.md",
        "text": text,
    })
    .to_string()
}

async fn read_body(response: MiddlewareResponse) -> (u16, Vec<u8>) {
    let (_, status, _, mut body, _) = response.into_parts();
    let mut bytes = Vec::new();
    while let Some(frame) = body.next_frame().await.expect("notes body frame") {
        bytes.extend_from_slice(frame.bytes());
    }
    (status, bytes)
}

fn upstream_body(request: &wiremock::Request) -> Value {
    serde_json::from_slice(&request.body).expect("upstream json body")
}

/// 模型请求正文按官方压缩发送；辅助请求仍是明文 JSON
fn model_body(request: &wiremock::Request) -> Value {
    let decoded = zstd::stream::decode_all(request.body.as_slice()).expect("zstd request body");
    serde_json::from_slice(&decoded).expect("model json body")
}

#[tokio::test]
async fn managed_writes_are_recorded_with_original_ciphertext_before_delivery() {
    let server = MockServer::start().await;
    for route in ["write_file", "append_to_file"] {
        Mock::given(method("POST"))
            .and(path(format!("/backend-api/codex/alpha/notes/v2/{route}")))
            .respond_with(ResponseTemplate::new(200).set_body_string(REPLAYED))
            .mount(&server)
            .await;
    }
    let store = Arc::new(MemoryAccountStore::default());
    seed(&store, ACCOUNT, true, false).await;
    let sessions = Arc::new(MemoryCodexSessions::default());
    let notes = MemoryNotesStore::with_state(state(
        CodexSessionOrigin::Managed,
        CodexNotesReplica::native(account()),
    ));
    let provider = provider_with_codex_sessions_and_native_notes(
        &store,
        sessions,
        format!("{}/backend-api", server.uri()),
        notes_service(&notes),
    );
    let (service, _) = native_service_with_mode(provider, CodexAccessMode::SessionPool);

    let write = write_body("cipher-write");
    let append = write_body("cipher-append");
    for (operation, body) in [
        (CodexAuxiliaryOperation::NotesWriteFile, write.as_str()),
        (CodexAuxiliaryOperation::NotesAppendToFile, append.as_str()),
        // 同一正文再次 append 是第二次真实变更，不按正文去重
        (CodexAuxiliaryOperation::NotesAppendToFile, append.as_str()),
    ] {
        let response = service
            .codex_auxiliary(prepare(&service).await, notes_request(operation, body))
            .await
            .expect("recorded notes write");
        // 交付前已完成落库；读取正文之前日志里已是真实观察
        let recorded = notes.operations();
        assert_eq!(
            recorded.last().expect("operation").status,
            CodexNotesOperationStatus::Responded
        );
        let (status, delivered) = read_body(response).await;
        assert_eq!(status, 200);
        assert_eq!(delivered, REPLAYED.as_bytes());
    }

    let operations = notes.operations();
    assert_eq!(
        operations
            .iter()
            .map(|operation| (operation.seq, operation.kind))
            .collect::<Vec<_>>(),
        [
            (1, CodexNotesOperationKind::WriteFile),
            (2, CodexNotesOperationKind::AppendToFile),
            (3, CodexNotesOperationKind::AppendToFile),
        ]
    );
    // 日志保存原请求与真实响应；不本地改写密文
    assert_eq!(operations[0].request, write);
    assert_eq!(operations[1].request, append);
    assert_eq!(operations[1].response.as_deref(), Some(REPLAYED));
    assert_eq!(operations[1].response_status, Some(200));

    let requests = server.received_requests().await.expect("received requests");
    assert_eq!(requests.len(), 3);
    // 原生域保持客户端原始字节与官方加密标记
    assert_eq!(requests[0].body, write.as_bytes());
    assert_eq!(
        requests[0].headers["x-openai-encrypted-tool-arguments"],
        "true"
    );
    assert_eq!(
        requests[0].headers["chatgpt-account-id"],
        "workspace_native"
    );
}

#[tokio::test]
async fn rebuilt_replica_maps_outbound_session_id_and_keeps_business_parameters() {
    let server = MockServer::start().await;
    for route in ["read_file", "write_file"] {
        Mock::given(method("POST"))
            .and(path(format!("/backend-api/codex/alpha/notes/v2/{route}")))
            .respond_with(ResponseTemplate::new(200).set_body_string(REPLAYED))
            .mount(&server)
            .await;
    }
    let store = Arc::new(MemoryAccountStore::default());
    seed(&store, ACCOUNT, true, false).await;
    let sessions = Arc::new(MemoryCodexSessions::default());
    let generation = CodexNotesGeneration::generate();
    let replica = CodexNotesReplica::rebuilt(account(), generation.clone());
    let notes = MemoryNotesStore::with_state(state(CodexSessionOrigin::Managed, replica.clone()));
    let provider = provider_with_codex_sessions_and_native_notes(
        &store,
        sessions,
        format!("{}/backend-api", server.uri()),
        notes_service(&notes),
    );
    let (service, _) = native_service_with_mode(provider, CodexAccessMode::SessionPool);

    let read = json!({
        "context": {"session_id": SESSION, "current_agent_name": "/root"},
        "path": "/root/notes.md",
    })
    .to_string();
    service
        .codex_auxiliary(
            prepare(&service).await,
            notes_request(CodexAuxiliaryOperation::NotesReadFile, &read),
        )
        .await
        .expect("rebuilt domain read");
    // 读类不产生日志，只使用已发布副本域
    assert!(notes.operations().is_empty());

    let write = write_body("cipher-write");
    service
        .codex_auxiliary(
            prepare(&service).await,
            notes_request(CodexAuxiliaryOperation::NotesWriteFile, &write),
        )
        .await
        .expect("rebuilt domain write");

    let requests = server.received_requests().await.expect("received requests");
    assert_eq!(requests.len(), 2);
    for request in &requests {
        let body = upstream_body(request);
        assert_eq!(
            body["context"]["session_id"],
            json!(generation.expose_to_store())
        );
        assert_eq!(body["context"]["current_agent_name"], json!("/root"));
        assert_eq!(body["path"], json!("/root/notes.md"));
    }
    assert_eq!(upstream_body(&requests[1])["text"], json!("cipher-write"));
    // 日志保存客户端原请求，不把出站域改写写回原文
    let operations = notes.operations();
    assert_eq!(operations.len(), 1);
    assert_eq!(operations[0].request, write);
    assert_eq!(operations[0].replica, replica);
}

#[tokio::test]
async fn rejected_write_is_forwarded_once_and_recorded_as_failed() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/backend-api/codex/alpha/notes/v2/write_file"))
        .respond_with(
            ResponseTemplate::new(503)
                .set_body_bytes(b"upstream busy".to_vec())
                .insert_header("content-type", "text/plain"),
        )
        .expect(1)
        .mount(&server)
        .await;
    let store = Arc::new(MemoryAccountStore::default());
    seed(&store, ACCOUNT, true, false).await;
    let sessions = Arc::new(MemoryCodexSessions::default());
    let notes = MemoryNotesStore::with_state(state(
        CodexSessionOrigin::Managed,
        CodexNotesReplica::native(account()),
    ));
    let provider = provider_with_codex_sessions_and_native_notes(
        &store,
        sessions,
        format!("{}/backend-api", server.uri()),
        notes_service(&notes),
    );
    let (service, _) = native_service_with_mode(provider, CodexAccessMode::SessionPool);
    let response = service
        .codex_auxiliary(
            prepare(&service).await,
            notes_request(
                CodexAuxiliaryOperation::NotesWriteFile,
                &write_body("cipher"),
            ),
        )
        .await
        .expect("non-success response is delivered as-is");
    let (status, body) = read_body(response).await;
    assert_eq!(
        (status, body.as_slice()),
        (503, b"upstream busy".as_slice())
    );
    let operations = notes.operations();
    assert_eq!(operations.len(), 1);
    assert_eq!(operations[0].status, CodexNotesOperationStatus::Failed);
    assert_eq!(operations[0].response_status, Some(503));
    assert_eq!(operations[0].response.as_deref(), Some("upstream busy"));
    assert!(!operations[0].status.needs_replay());
}

#[tokio::test]
async fn legacy_session_write_keeps_native_domain_without_log() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/backend-api/codex/alpha/notes/v2/write_file"))
        .respond_with(ResponseTemplate::new(200).set_body_string(REPLAYED))
        .expect(1)
        .mount(&server)
        .await;
    let store = Arc::new(MemoryAccountStore::default());
    seed(&store, ACCOUNT, true, false).await;
    let sessions = Arc::new(MemoryCodexSessions::default());
    let notes = MemoryNotesStore::with_state(state(
        CodexSessionOrigin::Legacy,
        CodexNotesReplica::native(account()),
    ));
    let provider = provider_with_codex_sessions_and_native_notes(
        &store,
        sessions,
        format!("{}/backend-api", server.uri()),
        notes_service(&notes),
    );
    let (service, _) = native_service_with_mode(provider, CodexAccessMode::SessionPool);
    let write = write_body("cipher");
    service
        .codex_auxiliary(
            prepare(&service).await,
            notes_request(CodexAuxiliaryOperation::NotesWriteFile, &write),
        )
        .await
        .expect("legacy write stays upstream");
    assert!(notes.operations().is_empty());
    let requests = server.received_requests().await.expect("received requests");
    assert_eq!(
        upstream_body(&requests[0])["context"]["session_id"],
        json!(SESSION)
    );
}

#[tokio::test]
async fn unclassified_session_records_the_first_write() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/backend-api/codex/alpha/notes/v2/append_to_file"))
        .respond_with(ResponseTemplate::new(200).set_body_string(REPLAYED))
        .mount(&server)
        .await;
    let store = Arc::new(MemoryAccountStore::default());
    seed(&store, ACCOUNT, true, false).await;
    let sessions = Arc::new(MemoryCodexSessions::default());
    let notes = MemoryNotesStore::with_state(state(
        CodexSessionOrigin::Unclassified,
        CodexNotesReplica::native(account()),
    ));
    let provider = provider_with_codex_sessions_and_native_notes(
        &store,
        sessions,
        format!("{}/backend-api", server.uri()),
        notes_service(&notes),
    );
    let (service, _) = native_service_with_mode(provider, CodexAccessMode::SessionPool);
    service
        .codex_auxiliary(
            prepare(&service).await,
            notes_request(
                CodexAuxiliaryOperation::NotesAppendToFile,
                &write_body("cipher"),
            ),
        )
        .await
        .expect("unclassified write");
    let operations = notes.operations();
    assert_eq!(operations.len(), 1);
    assert_eq!(operations[0].status, CodexNotesOperationStatus::Responded);
}

#[tokio::test]
async fn transport_failure_records_unknown_without_retry() {
    let store = Arc::new(MemoryAccountStore::default());
    seed(&store, ACCOUNT, true, false).await;
    let sessions = Arc::new(MemoryCodexSessions::default());
    let notes = MemoryNotesStore::with_state(state(
        CodexSessionOrigin::Managed,
        CodexNotesReplica::native(account()),
    ));
    // 不可达出口：发送结果不明，不能重试也不能当成失败跳过
    let provider = provider_with_codex_sessions_and_native_notes(
        &store,
        sessions.clone(),
        "http://127.0.0.1:9/backend-api".to_owned(),
        notes_service(&notes),
    );
    sessions.seed_owner(
        &ClientApiKeyId::new("key_native_test").expect("client key"),
        &auxiliary_session_key(&provider, SESSION),
        account(),
    );
    let (service, _) = native_service_with_mode(provider, CodexAccessMode::SessionPool);
    let _error = service
        .codex_auxiliary(
            prepare(&service).await,
            notes_request(
                CodexAuxiliaryOperation::NotesWriteFile,
                &write_body("cipher"),
            ),
        )
        .await
        .expect_err("transport failure is returned to the client");
    let operations = notes.operations();
    assert_eq!(operations.len(), 1);
    assert_eq!(operations[0].status, CodexNotesOperationStatus::Unknown);
    assert!(operations[0].response.is_none());
}

/// 与辅助请求共用的会话键派生；模型请求与辅助请求必须命中同一键
fn auxiliary_session_key(
    provider: &provider_openai::CodexProvider,
    session: &str,
) -> ProviderSessionAffinityKey {
    use gateway_core::engine::provider::Provider as _;
    let operation = Operation::Generate(GenerateRequest::from_protocol_payload(
        ProtocolPayload::json_object(
            "openai",
            json!({"model":"gpt-5.4","input":"hello","session_id":session})
                .as_object()
                .expect("json object")
                .clone(),
        )
        .expect("payload"),
    ));
    let observation = provider.request_observation(
        &operation,
        &ClientApiKeyId::new("key_native_test").expect("client key"),
    );
    ProviderSessionAffinityKey::try_new(
        observation
            .continuation
            .affinity_hash
            .expect("session affinity hash"),
    )
    .expect("session key")
}

/// 与模型 attempt 共用的池会话键；owner 预置与选号必须命中同一键
fn pool_session_key(
    provider: &provider_openai::CodexProvider,
    session: &str,
) -> ProviderSessionAffinityKey {
    let observation = provider.request_observation(
        &Operation::Generate(generate_with_session_context(session, None, None)),
        &ClientApiKeyId::new("key_openai_contract").expect("client key"),
    );
    ProviderSessionAffinityKey::try_new(
        observation
            .continuation
            .affinity_hash
            .expect("session affinity hash"),
    )
    .expect("session key")
}

/// 把账号标成确切额度耗尽；这是唯一允许已托管会话迁移的不可用状态
async fn exhaust(store: &MemoryAccountStore, id: &str) {
    let account = store.account(id).expect("seeded account");
    store
        .apply_quota_access(QuotaAccessChange {
            account_id: account.id().clone(),
            expected_revision: account.revision(),
            state: QuotaState::exhausted(QuotaEvidence::UsageLimitReached, SystemTime::now(), None),
        })
        .await
        .expect("exhaust quota");
}

fn migration_state(origin: CodexSessionOrigin, owner: &str) -> CodexSessionState {
    let owner = ProviderAccountId::new(owner).expect("owner account");
    CodexSessionState {
        origin,
        notes: CodexNotesReplica::native(owner.clone()),
        owner,
        generation: 3,
        notes_replay: None,
    }
}

fn migration_log(notes: &MemoryNotesStore) {
    notes.seed_responded(
        CodexNotesOperationKind::WriteFile,
        r#"{"context":{"session_id":"session-native-migrate","current_agent_name":"/root"},"path":"/root/notes.md","text":"cipher-write"}"#,
    );
    notes.seed_responded(
        CodexNotesOperationKind::AppendToFile,
        r#"{"context":{"session_id":"session-native-migrate","current_agent_name":"/root"},"path":"/root/notes.md","text":"cipher-append"}"#,
    );
}

#[tokio::test]
async fn quota_exhausted_owner_migrates_after_notes_replay_before_the_model_request() {
    let server = MockServer::start().await;
    for route in ["write_file", "append_to_file"] {
        Mock::given(method("POST"))
            .and(path(format!("/backend-api/codex/alpha/notes/v2/{route}")))
            .respond_with(ResponseTemplate::new(200).set_body_string(REPLAYED))
            .mount(&server)
            .await;
    }
    Mock::given(method("POST"))
        .and(path("/backend-api/codex/responses"))
        .respond_with(ResponseTemplate::new(200).set_body_string(CAPTURED_SSE))
        .mount(&server)
        .await;
    let store = Arc::new(MemoryAccountStore::default());
    seed_workspace(&store, OWNER_A, true, false, &format!("chatgpt-{OWNER_A}")).await;
    seed_workspace(&store, OWNER_B, true, false, &format!("chatgpt-{OWNER_B}")).await;
    exhaust(&store, OWNER_A).await;
    let sessions = Arc::new(MemoryCodexSessions::default());
    let notes = MemoryNotesStore::with_state(migration_state(CodexSessionOrigin::Managed, OWNER_A));
    migration_log(&notes);
    let provider = provider_with_codex_sessions_and_native_notes(
        &store,
        sessions.clone(),
        format!("{}/backend-api", server.uri()),
        notes_service(&notes),
    );
    let owner_a = ProviderAccountId::new(OWNER_A).expect("owner");
    let key_id = ClientApiKeyId::new("key_openai_contract").expect("client key");
    sessions.seed_owner(
        &key_id,
        &pool_session_key(&provider, MIGRATE_SESSION),
        owner_a.clone(),
    );

    let mut stream = provider
        .execute(
            planned_request(
                "openai",
                pool_turn_request(MIGRATE_SESSION, "thread-native-migrate:0", "new"),
            ),
            pool_context("req_native_migrate", CancellationToken::new()),
        )
        .await
        .expect("migrated request");
    assert_eq!(
        stream.metadata().provider_account_id().as_str(),
        OWNER_B,
        "model request must use the new owner"
    );
    while let Some(event) = stream.next().await {
        event.expect("upstream stream");
    }

    // 选号与发布阶段都不提前认领：持久归属只由 notes 发布事务改写
    assert!(sessions.claims().is_empty());
    assert_eq!(
        notes.owner(),
        Some(ProviderAccountId::new(OWNER_B).expect("owner b"))
    );
    let publishes = notes.publishes();
    assert_eq!(publishes.len(), 1);
    assert_eq!(publishes[0].owner.as_str(), OWNER_B);
    assert_eq!(publishes[0].through_seq, 2);
    // 读凭据使用的域与发布后的副本必须属于同一个新账号
    let replica = notes.notes().expect("published notes replica");
    assert_eq!(replica.account().as_str(), OWNER_B);
    assert_eq!(replica, publishes[0].replica);
    let generation = publishes[0]
        .replica
        .generation()
        .expect("rebuilt generation")
        .expose_to_store()
        .to_owned();

    // 请求顺序必须是：按序重放全部 notes 写 → 发布后的 B 模型请求
    let requests = server.received_requests().await.expect("received requests");
    assert_eq!(requests.len(), 3);
    assert!(requests[0].url.path().ends_with("/notes/v2/write_file"));
    assert!(requests[1].url.path().ends_with("/notes/v2/append_to_file"));
    assert!(requests[2].url.path().ends_with("/codex/responses"));
    for request in &requests {
        assert_eq!(
            request.headers["chatgpt-account-id"],
            format!("chatgpt-{OWNER_B}")
        );
    }
    for request in &requests[..2] {
        let body = upstream_body(request);
        assert_eq!(body["context"]["session_id"], json!(generation));
        assert_eq!(body["path"], json!("/root/notes.md"));
    }
}

#[tokio::test]
async fn failed_notes_replay_keeps_the_old_owner_and_never_sends_the_model_request() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/backend-api/codex/alpha/notes/v2/write_file"))
        .respond_with(ResponseTemplate::new(500).set_body_string("replay failed"))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/backend-api/codex/alpha/notes/v2/append_to_file"))
        .respond_with(ResponseTemplate::new(200).set_body_string(REPLAYED))
        .mount(&server)
        .await;
    let store = Arc::new(MemoryAccountStore::default());
    seed_workspace(&store, OWNER_A, true, false, &format!("chatgpt-{OWNER_A}")).await;
    seed_workspace(&store, OWNER_B, true, false, &format!("chatgpt-{OWNER_B}")).await;
    exhaust(&store, OWNER_A).await;
    let sessions = Arc::new(MemoryCodexSessions::default());
    let notes = MemoryNotesStore::with_state(migration_state(CodexSessionOrigin::Managed, OWNER_A));
    migration_log(&notes);
    let provider = provider_with_codex_sessions_and_native_notes(
        &store,
        sessions.clone(),
        format!("{}/backend-api", server.uri()),
        notes_service(&notes),
    );
    let owner_a = ProviderAccountId::new(OWNER_A).expect("owner");
    sessions.seed_owner(
        &ClientApiKeyId::new("key_openai_contract").expect("client key"),
        &pool_session_key(&provider, MIGRATE_SESSION),
        owner_a.clone(),
    );

    let error = provider
        .execute(
            planned_request(
                "openai",
                pool_turn_request(MIGRATE_SESSION, "thread-native-migrate:0", "new"),
            ),
            pool_context("req_native_migrate_failed", CancellationToken::new()),
        )
        .await
        .err()
        .expect("replay rejection must stop the migration");
    assert_eq!(error.send_state(), UpstreamSendState::NotSent);
    // 未发布：owner、域与归属都保持在旧账号，模型请求完全不发送
    assert_eq!(notes.owner(), Some(owner_a));
    assert!(notes.publishes().is_empty());
    assert!(sessions.claims().is_empty());
    let requests = server.received_requests().await.expect("received requests");
    assert_eq!(requests.len(), 1);
    assert!(requests[0].url.path().ends_with("/notes/v2/write_file"));
}

#[tokio::test]
async fn legacy_session_keeps_its_owner_even_when_the_account_is_exhausted() {
    let server = MockServer::start().await;
    let store = Arc::new(MemoryAccountStore::default());
    seed_workspace(&store, OWNER_A, true, false, &format!("chatgpt-{OWNER_A}")).await;
    seed_workspace(&store, OWNER_B, true, false, &format!("chatgpt-{OWNER_B}")).await;
    exhaust(&store, OWNER_A).await;
    let sessions = Arc::new(MemoryCodexSessions::default());
    let notes = MemoryNotesStore::with_state(migration_state(CodexSessionOrigin::Legacy, OWNER_A));
    let provider = provider_with_codex_sessions_and_native_notes(
        &store,
        sessions.clone(),
        format!("{}/backend-api", server.uri()),
        notes_service(&notes),
    );
    sessions.seed_owner(
        &ClientApiKeyId::new("key_openai_contract").expect("client key"),
        &pool_session_key(&provider, MIGRATE_SESSION),
        ProviderAccountId::new(OWNER_A).expect("owner"),
    );

    let error = provider
        .execute(
            planned_request(
                "openai",
                pool_turn_request(MIGRATE_SESSION, "thread-native-migrate:0", "new"),
            ),
            pool_context("req_native_legacy", CancellationToken::new()),
        )
        .await
        .err()
        .expect("legacy sessions keep the permanent owner");
    assert_eq!(error.kind(), ProviderErrorKind::QuotaExhausted);
    assert!(notes.publishes().is_empty());
    assert!(
        server
            .received_requests()
            .await
            .expect("received requests")
            .is_empty()
    );
}

#[tokio::test]
async fn writes_after_the_first_migration_replay_back_to_the_restored_owner() {
    let server = MockServer::start().await;
    for route in ["write_file", "append_to_file"] {
        Mock::given(method("POST"))
            .and(path(format!("/backend-api/codex/alpha/notes/v2/{route}")))
            .respond_with(ResponseTemplate::new(200).set_body_string(REPLAYED))
            .mount(&server)
            .await;
    }
    Mock::given(method("POST"))
        .and(path("/backend-api/codex/responses"))
        .respond_with(ResponseTemplate::new(200).set_body_string(CAPTURED_SSE))
        .mount(&server)
        .await;
    let store = Arc::new(MemoryAccountStore::default());
    seed_workspace(&store, OWNER_A, true, false, &format!("chatgpt-{OWNER_A}")).await;
    seed_workspace(&store, OWNER_B, true, false, &format!("chatgpt-{OWNER_B}")).await;
    exhaust(&store, OWNER_A).await;
    let sessions = Arc::new(MemoryCodexSessions::default());
    let notes = MemoryNotesStore::with_state(migration_state(CodexSessionOrigin::Managed, OWNER_A));
    migration_log(&notes);
    let provider = provider_with_codex_sessions_and_native_notes(
        &store,
        sessions.clone(),
        format!("{}/backend-api", server.uri()),
        notes_service(&notes),
    );
    let key_id = ClientApiKeyId::new("key_openai_contract").expect("client key");
    let owner_a = ProviderAccountId::new(OWNER_A).expect("owner a");
    let owner_b = ProviderAccountId::new(OWNER_B).expect("owner b");
    let session_key = pool_session_key(&provider, MIGRATE_SESSION);
    sessions.seed_owner(&key_id, &session_key, owner_a.clone());

    // 第一次迁移：A 额度耗尽，按序重放既有两条写入后切到 B
    let mut first = provider
        .clone()
        .execute(
            planned_request(
                "openai",
                pool_turn_request(MIGRATE_SESSION, "thread-native-migrate:0", "new"),
            ),
            pool_context("req_native_migrate_first", CancellationToken::new()),
        )
        .await
        .expect("first migration");
    while let Some(event) = first.next().await {
        event.expect("first upstream stream");
    }
    assert_eq!(notes.owner(), Some(owner_b.clone()));
    // 归属与 notes 域在生产中同属一行；内存存储分开模拟，发布后归属端口也应读到新 owner
    sessions.seed_owner(&key_id, &session_key, owner_b.clone());

    // B 上新增一次真实写入；写路径本身由前面的托管写测试覆盖，这里只补上已终结的日志事实
    notes.seed_responded(
        CodexNotesOperationKind::AppendToFile,
        r#"{"context":{"session_id":"session-native-migrate","current_agent_name":"/root"},"path":"/root/notes.md","text":"cipher-after-migration"}"#,
    );
    let operations = notes.operations();
    assert_eq!(operations.len(), 3);
    assert_eq!(operations[2].seq, 3);
    assert_eq!(operations[2].status, CodexNotesOperationStatus::Responded);

    // B 也耗尽、A 恢复：再次切换时必须把三条写入按序重放到 A 的新域
    exhaust(&store, OWNER_B).await;
    let account_a = store.account(OWNER_A).expect("account a");
    store
        .apply_quota_access(QuotaAccessChange {
            account_id: account_a.id().clone(),
            expected_revision: account_a.revision(),
            state: QuotaState::allowed(SystemTime::now()),
        })
        .await
        .expect("restore quota a");
    let mut second = provider
        .execute(
            planned_request(
                "openai",
                pool_turn_request(MIGRATE_SESSION, "thread-native-migrate:0", "new"),
            ),
            pool_context("req_native_migrate_second", CancellationToken::new()),
        )
        .await
        .expect("second migration");
    assert_eq!(second.metadata().provider_account_id(), &owner_a);
    while let Some(event) = second.next().await {
        event.expect("second upstream stream");
    }
    assert_eq!(notes.owner(), Some(owner_a.clone()));
    let publishes = notes.publishes();
    assert_eq!(publishes.len(), 2);
    let generation = publishes[1]
        .replica
        .generation()
        .expect("second rebuilt generation")
        .expose_to_store()
        .to_owned();
    assert_eq!(publishes[1].through_seq, 3);

    let requests = server.received_requests().await.expect("received requests");
    // 第一次迁移 2 次重放 + B 模型；第二次迁移 3 次重放 + A 模型
    assert_eq!(requests.len(), 7);
    let second_replay = &requests[3..6];
    for request in second_replay {
        assert_eq!(
            request.headers["chatgpt-account-id"],
            format!("chatgpt-{OWNER_A}")
        );
        let body = upstream_body(request);
        assert_eq!(body["context"]["session_id"], json!(generation));
        assert_eq!(body["context"]["current_agent_name"], json!("/root"));
    }
    assert!(requests[6].url.path().ends_with("/codex/responses"));
    assert_eq!(
        requests[6].headers["chatgpt-account-id"],
        format!("chatgpt-{OWNER_A}")
    );
}

/// 真实上游额度拒绝后的完整恢复：反馈 → 客户端全量重发 → 新 owner 的 notes 域与模型请求
///
/// 第一次模型请求由 A 的 mock 上游以官方 429 拒绝（不是预置额度状态）；
/// 网关记录额度事实并把 quota 反馈交给客户端。客户端按既有规则全量重发后，
/// 选号逃离 A、notes 按序重建并发布到 B，随后 B 才收到模型请求；
/// 用户输入不带 ID，history 条目只在发送前保存一次，不因重发重复入库
#[tokio::test]
async fn upstream_quota_rejection_then_client_replay_migrates_history_and_notes() {
    const QUOTA_WINDOW: &str = "thread-native-quota:0";
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/backend-api/codex/responses"))
        .respond_with(ResponseTemplate::new(429).set_body_json(json!({
            "error": {
                "type": "usage_limit_reached",
                "message": "usage limit reached",
                "resets_at": 1_900_000_000u64
            }
        })))
        // 只有第一次请求由 A 收到真实额度拒绝；重发必须落到下一个应答
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;
    for route in ["write_file", "append_to_file"] {
        Mock::given(method("POST"))
            .and(path(format!("/backend-api/codex/alpha/notes/v2/{route}")))
            .respond_with(ResponseTemplate::new(200).set_body_string(REPLAYED))
            .expect(1)
            .mount(&server)
            .await;
    }
    Mock::given(method("POST"))
        .and(path("/backend-api/codex/responses"))
        .respond_with(ResponseTemplate::new(200).set_body_string(CAPTURED_SSE))
        .expect(1)
        .mount(&server)
        .await;

    let store = Arc::new(MemoryAccountStore::default());
    seed_workspace(&store, OWNER_A, true, false, &format!("chatgpt-{OWNER_A}")).await;
    seed_workspace(&store, OWNER_B, true, false, &format!("chatgpt-{OWNER_B}")).await;
    let sessions = Arc::new(MemoryCodexSessions::default());
    let notes = MemoryNotesStore::with_state(migration_state(CodexSessionOrigin::Managed, OWNER_A));
    migration_log(&notes);
    let provider = provider_with_codex_sessions_native_history_and_notes(
        &store,
        sessions.clone(),
        format!("{}/backend-api", server.uri()),
        history_service(&notes),
        notes_service(&notes),
    );
    let owner_a = ProviderAccountId::new(OWNER_A).expect("owner");
    let key_id = ClientApiKeyId::new("key_openai_contract").expect("client key");
    sessions.seed_owner(
        &key_id,
        &pool_session_key(&provider, MIGRATE_SESSION),
        owner_a.clone(),
    );

    let mut stream = provider
        .clone()
        .execute(
            planned_request(
                "openai",
                pool_turn_request_with_input(MIGRATE_SESSION, QUOTA_WINDOW, "new", quota_input()),
            ),
            pool_context("req_native_quota_first", CancellationToken::new()),
        )
        .await
        .expect("provider stream for the quota rejection");
    let error = loop {
        match stream.next().await {
            Some(Ok(_)) => {}
            Some(Err(error)) => break error,
            None => panic!("usage limit rejection must surface a typed failure"),
        }
    };
    drop(stream);
    assert_eq!(error.kind(), ProviderErrorKind::QuotaExhausted);
    assert!(error.replay_is_safe());
    // 额度事实来自真实上游响应，后续选号据此逃离 A
    assert_eq!(
        store.account(OWNER_A).expect("account a").quota().access(),
        QuotaAccessState::Exhausted
    );
    // 失败之前没有发布，也没有改写 owner 与域
    assert!(notes.publishes().is_empty());
    assert_eq!(notes.owner(), Some(owner_a.clone()));
    assert_eq!(notes.notes(), Some(CodexNotesReplica::native(owner_a)));
    // 输入在发送前已保存；失败 attempt 不产生回答条目
    assert_eq!(
        notes.history_item_ids(),
        vec!["cprs:thread-native-quota:0:0".to_owned()]
    );
    assert!(notes.history_anchor("resp_native_1").is_none());
    // 失败 attempt 的发送前逻辑输入快照仍在，供客户端全量重发对齐
    assert_eq!(
        notes
            .history_pending(QUOTA_WINDOW)
            .expect("sending-side input snapshot")
            .input,
        vec!["cprs:thread-native-quota:0:0".to_owned()]
    );

    // 客户端全量重发：同一逻辑会话，同一个完整输入
    let mut stream = provider
        .clone()
        .execute(
            planned_request(
                "openai",
                pool_turn_request_with_input(MIGRATE_SESSION, QUOTA_WINDOW, "new", quota_input()),
            ),
            pool_context("req_native_quota_replay", CancellationToken::new()),
        )
        .await
        .expect("replayed provider stream");
    assert_eq!(
        stream.metadata().provider_account_id().as_str(),
        OWNER_B,
        "client replay must escape the quota exhausted owner"
    );
    while let Some(event) = stream.next().await {
        event.expect("upstream stream");
    }

    // notes 先按 seq 重放到 B 的自建域并发布，之后才是 B 的模型请求
    let publishes = notes.publishes();
    assert_eq!(publishes.len(), 1);
    assert_eq!(publishes[0].owner.as_str(), OWNER_B);
    assert_eq!(publishes[0].through_seq, 2);
    let replica = notes.notes().expect("published replica");
    assert_eq!(replica.account().as_str(), OWNER_B);
    assert_eq!(replica, publishes[0].replica);
    assert_eq!(
        notes.owner(),
        Some(ProviderAccountId::new(OWNER_B).expect("owner b"))
    );
    let requests = server.received_requests().await.expect("received requests");
    assert_eq!(requests.len(), 4);
    assert!(requests[0].url.path().ends_with("/codex/responses"));
    assert_eq!(
        requests[0].headers["chatgpt-account-id"],
        format!("chatgpt-{OWNER_A}")
    );
    assert!(requests[1].url.path().ends_with("/notes/v2/write_file"));
    assert!(requests[2].url.path().ends_with("/notes/v2/append_to_file"));
    assert!(requests[3].url.path().ends_with("/codex/responses"));
    for request in &requests[1..] {
        assert_eq!(
            request.headers["chatgpt-account-id"],
            format!("chatgpt-{OWNER_B}")
        );
    }
    // 重发的模型请求仍是完整输入，不是上一次失败的增量
    let replayed = model_body(&requests[3]);
    assert_eq!(replayed["input"], quota_input());
    // 全量重发不重复入库；响应锚保存这次请求的逻辑输入与产出
    assert_eq!(
        notes.history_item_ids(),
        vec![
            "cprs:thread-native-quota:0:0".to_owned(),
            "msg_answer".to_owned()
        ]
    );
    let anchor = notes
        .history_anchor("resp_native_1")
        .expect("response anchor");
    assert_eq!(anchor.window, history_window("thread-native-quota"));
    assert_eq!(
        anchor.input,
        vec!["cprs:thread-native-quota:0:0".to_owned()]
    );
    assert_eq!(anchor.output, vec!["msg_answer".to_owned()]);
}

/// WS 增量遇到真实额度拒绝：客户端收到 previous_response_not_found 后全量重发，
/// 网关不把 A 的增量交给 B，重发后才在新 owner 的域上完成笔记发布与模型请求
///
/// 失败增量不带输入 ID：它已随发送前快照落库，但没有任何 A 的响应锚；
/// 全量重发必须复用原尾部引用，不能按当前条目数重复归档一次
#[tokio::test]
async fn websocket_quota_rejection_requires_a_full_replay_before_the_new_owner_runs() {
    const WINDOW: &str = "thread-quota-switch:0";
    const DELTA_ITEM: &str =
        r#"{"type":"message","role":"user","content":[{"type":"input_text","text":"continue"}]}"#;
    // 失败增量的稳定 ID 由窗口位置生成，尾部引用在全量重发时必须保持一致
    const DELTA_ID: &str = "cprs:thread-quota-switch:0:2";
    const FULL_INPUT: [&str; 3] = [
        r#"{"type":"message","id":"msg_a","role":"user","content":[{"type":"input_text","text":"hello"}]}"#,
        r#"{"type":"message","id":"msg_b","role":"assistant","content":[{"type":"output_text","text":"first answer"}]}"#,
        DELTA_ITEM,
    ];
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        // 连接 1：A 收到增量 create；上游在交付前以官方额度错误拒绝
        let (socket, _) = listener.accept().await.unwrap();
        let mut websocket = accept_codex_test_websocket(socket).await;
        let delta: Value = serde_json::from_str(
            websocket
                .next()
                .await
                .expect("delta create frame")
                .expect("delta frame")
                .to_text()
                .expect("text frame"),
        )
        .expect("delta json");
        assert_eq!(delta["previous_response_id"], json!("resp_a"));
        assert_eq!(
            delta["input"],
            json!([json_from(DELTA_ITEM)]),
            "增量请求只带本轮新增条目"
        );
        websocket
            .send(Message::Text(
                json!({
                    "type": "error",
                    "status": 429,
                    "error": {
                        "type": "usage_limit_reached",
                        "code": "usage_limit_reached",
                        "message": "You have reached your usage limit."
                    }
                })
                .to_string()
                .into(),
            ))
            .await
            .expect("quota error frame");

        // 连接 2：客户端全量重发；必须没有父响应且带完整上下文
        let (socket, _) = listener.accept().await.unwrap();
        let mut websocket = accept_codex_test_websocket(socket).await;
        let replay: Value = serde_json::from_str(
            websocket
                .next()
                .await
                .expect("replay create frame")
                .expect("replay frame")
                .to_text()
                .expect("text frame"),
        )
        .expect("replay json");
        assert!(
            replay.get("previous_response_id").is_none(),
            "全量重发不能续接 A 的响应"
        );
        assert_eq!(
            replay["input"],
            json!(
                FULL_INPUT
                    .iter()
                    .map(|item| json_from(item))
                    .collect::<Vec<_>>()
            )
        );
        for event in [
            json!({"type": "response.created", "response": {"id": "resp_replay", "model": "gpt-5.4", "status": "in_progress", "output": []}}),
            json!({"type": "response.output_item.done", "output_index": 0, "item": {"type": "message", "id": "msg_answer", "role": "assistant", "content": [{"type": "output_text", "text": "answer"}]}}),
            json!({"type": "response.completed", "response": {"id": "resp_replay", "model": "gpt-5.4", "status": "completed", "output": []}}),
        ] {
            websocket
                .send(Message::Text(event.to_string().into()))
                .await
                .expect("replay event frame");
        }
    });

    let store = Arc::new(MemoryAccountStore::default());
    seed_workspace(&store, OWNER_A, true, false, &format!("chatgpt-{OWNER_A}")).await;
    seed_workspace(&store, OWNER_B, true, false, &format!("chatgpt-{OWNER_B}")).await;
    let sessions = Arc::new(MemoryCodexSessions::default());
    // 切换前的上下文与 notes 域都在 A；这一轮只验证传输与 history 顺序，不重复 notes 重放
    let notes = MemoryNotesStore::with_state(migration_state(CodexSessionOrigin::Managed, OWNER_A));
    let window = history_window("thread-quota-switch");
    notes.seed_history_item(&window, history_item("msg_a", "user", "hello"));
    notes.seed_history_item(&window, history_item("msg_b", "assistant", "first answer"));
    notes.seed_history_response(window.clone(), "resp_a", &["msg_a"], &["msg_b"]);
    let provider = provider_with_codex_sessions_native_history_and_notes(
        &store,
        sessions.clone(),
        format!("http://{addr}"),
        history_service(&notes),
        notes_service(&notes),
    );
    let owner_a = ProviderAccountId::new(OWNER_A).expect("owner");
    let key_id = ClientApiKeyId::new("key_openai_contract").expect("client key");
    sessions.seed_owner(
        &key_id,
        &pool_session_key(&provider, MIGRATE_SESSION),
        owner_a.clone(),
    );

    let mut stream = provider
        .clone()
        .execute(
            planned_request(
                "openai",
                turn_operation(
                    MIGRATE_SESSION,
                    WINDOW,
                    Some("resp_a"),
                    Some(OWNER_A),
                    &[DELTA_ITEM],
                    true,
                ),
            ),
            pool_context("req_ws_quota_delta", CancellationToken::new())
                .with_continuation_attempt(ContinuationAttempt::Native),
        )
        .await
        .expect("delta provider stream");
    let mut error = loop {
        match stream.next().await {
            Some(Ok(event)) => assert!(!event.has_client_event(), "额度拒绝前不能交付输出"),
            Some(Err(error)) => break error,
            None => panic!("websocket quota rejection must surface a typed failure"),
        }
    };
    drop(stream);
    // 客户端得到的是既有额度反馈：原响应不可续接，必须自己重建完整输入
    assert_eq!(
        error
            .client_visible_upstream_error()
            .expect("client visible quota feedback")
            .code(),
        Some("previous_response_not_found")
    );
    assert!(error.take_atomic_client_events().is_empty());
    assert_eq!(
        store.account(OWNER_A).expect("account a").quota().access(),
        QuotaAccessState::Exhausted
    );
    // 失败 attempt 没有发布，也没有把增量交给 B；A 的域保持原样
    assert!(notes.publishes().is_empty());
    assert_eq!(notes.owner(), Some(owner_a.clone()));
    // 增量输入在发送前已按同一会话保存（无 ID，按位置生成稳定 ID），但没有任何 A 的响应锚
    assert_eq!(
        notes.history_item_ids(),
        vec!["msg_a".to_owned(), "msg_b".to_owned(), DELTA_ID.to_owned()]
    );
    assert!(notes.history_anchor("resp_replay").is_none());
    assert_eq!(
        notes
            .history_pending(WINDOW)
            .expect("sending-side input snapshot")
            .input,
        vec!["msg_a".to_owned(), "msg_b".to_owned(), DELTA_ID.to_owned()]
    );

    let mut stream = provider
        .clone()
        .execute(
            planned_request(
                "openai",
                turn_operation(MIGRATE_SESSION, WINDOW, None, None, &FULL_INPUT, true),
            ),
            pool_context("req_ws_quota_replay", CancellationToken::new()),
        )
        .await
        .expect("replayed provider stream");
    assert_eq!(
        stream.metadata().provider_account_id().as_str(),
        OWNER_B,
        "full replay must escape the quota exhausted owner"
    );
    while let Some(event) = stream.next().await {
        event.expect("upstream stream");
    }
    timeout_server(server).await;

    // 全量重发按引用对齐已保存前缀：增量条目不重复，问题不丢，只追加本次产出
    assert_eq!(
        notes.history_item_ids(),
        vec![
            "msg_a".to_owned(),
            "msg_b".to_owned(),
            DELTA_ID.to_owned(),
            "msg_answer".to_owned()
        ]
    );
    let anchor = notes
        .history_anchor("resp_replay")
        .expect("replayed response anchor");
    assert_eq!(anchor.window, window);
    assert_eq!(anchor.previous_response_id, None);
    assert_eq!(
        anchor.input,
        vec!["msg_a".to_owned(), "msg_b".to_owned(), DELTA_ID.to_owned()]
    );
    assert_eq!(anchor.output, vec!["msg_answer".to_owned()]);
    // 空日志的迁移仍然把 owner 推进到 B：增量绝不会发给 B
    assert_eq!(
        notes.owner(),
        Some(ProviderAccountId::new(OWNER_B).expect("owner b"))
    );
}

/// 一次失败响应之后，下一次 attempt 使用自己的采集句柄
///
/// 失败 attempt 已在上游建立 response 锚（response.created 之后才失败）；
/// 之后的重试是新 attempt，产出必须挂在新 response id 上，
/// 不能沿用旧响应的 id 或旧批次，也不能让同一份输入再入库一次
#[tokio::test]
async fn failed_response_does_not_reuse_the_capture_handle_of_the_next_attempt() {
    const FAILED_SSE: &str = concat!(
        "event: response.created\n",
        "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_failed\",\"model\":\"gpt-5.4\",\"status\":\"in_progress\",\"output\":[]}}\n\n",
        "event: response.failed\n",
        "data: {\"type\":\"response.failed\",\"status_code\":400,\"response\":{\"id\":\"resp_failed\",\"status\":\"failed\",\"error\":{\"code\":\"invalid_request_error\",\"message\":\"failed before output\"}}}\n\n"
    );
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/backend-api/codex/responses"))
        .respond_with(ResponseTemplate::new(200).set_body_string(FAILED_SSE))
        .up_to_n_times(1)
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/backend-api/codex/responses"))
        .respond_with(ResponseTemplate::new(200).set_body_string(CAPTURED_SSE))
        .expect(1)
        .mount(&server)
        .await;
    let store = Arc::new(MemoryAccountStore::default());
    seed_workspace(&store, OWNER_A, true, false, &format!("chatgpt-{OWNER_A}")).await;
    let sessions = Arc::new(MemoryCodexSessions::default());
    let notes = MemoryNotesStore::with_state(migration_state(CodexSessionOrigin::Managed, OWNER_A));
    let provider = provider_with_codex_sessions_native_history_and_notes(
        &store,
        sessions.clone(),
        format!("{}/backend-api", server.uri()),
        history_service(&notes),
        notes_service(&notes),
    );
    sessions.seed_owner(
        &ClientApiKeyId::new("key_openai_contract").expect("client key"),
        &pool_session_key(&provider, MIGRATE_SESSION),
        ProviderAccountId::new(OWNER_A).expect("owner"),
    );
    let request = || {
        planned_request(
            "openai",
            pool_turn_request(MIGRATE_SESSION, "thread-handle:0", "new"),
        )
    };
    let mut stream = provider
        .clone()
        .execute(
            request(),
            pool_context("req_handle_failed", CancellationToken::new()),
        )
        .await
        .expect("first attempt stream");
    let error = loop {
        match stream.next().await {
            Some(Ok(event)) => assert!(!event.has_client_event()),
            Some(Err(error)) => break error,
            None => panic!("failed response must surface a typed failure"),
        }
    };
    drop(stream);
    assert_eq!(error.kind(), ProviderErrorKind::InvalidRequest);
    // 失败 attempt 的锚只记录它自己的 response id 与当时的输入，没有产出
    let failed = notes.history_anchor("resp_failed").expect("failed anchor");
    assert_eq!(failed.input, vec!["msg_user".to_owned()]);
    assert!(failed.output.is_empty());

    let mut stream = provider
        .clone()
        .execute(
            request(),
            pool_context("req_handle_retry", CancellationToken::new()),
        )
        .await
        .expect("second attempt stream");
    while let Some(event) = stream.next().await {
        event.expect("upstream stream");
    }
    // 第二次 attempt 的句柄没有沿用旧 response id
    let anchor = notes
        .history_anchor("resp_native_1")
        .expect("second attempt anchor");
    assert_eq!(anchor.input, vec!["msg_user".to_owned()]);
    assert_eq!(anchor.output, vec!["msg_answer".to_owned()]);
    assert_eq!(
        notes.history_anchor("resp_failed").expect("failed anchor"),
        failed,
        "失败响应的事实不随后续 attempt 改写"
    );
    // 重试没有把同一输入再入库一次
    assert_eq!(
        notes.history_item_ids(),
        vec!["msg_user".to_owned(), "msg_answer".to_owned()]
    );
}

/// 官方 notes 写正文但会话 ID 可变：交付容量与互斥用例需要多个独立会话
fn write_body_for_session(session: &str, text: &str) -> String {
    json!({
        "context": {"session_id": session, "current_agent_name": "/root"},
        "path": "/root/notes.md",
        "text": text,
    })
    .to_string()
}

/// 已交付写响应；用例用它断言原始正文被完整保存
fn responded_operation(notes: &MemoryNotesStore) -> CodexNotesOperation {
    let operations = notes.operations();
    assert_eq!(operations.len(), 1, "exactly one recorded operation");
    operations[0].clone()
}

#[tokio::test]
async fn dropped_caller_future_keeps_the_real_delivery_observation() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/backend-api/codex/alpha/notes/v2/write_file"))
        .respond_with(ResponseTemplate::new(200).set_body_string(REPLAYED))
        .mount(&server)
        .await;
    let store = Arc::new(MemoryAccountStore::default());
    seed(&store, ACCOUNT, true, false).await;
    let sessions = Arc::new(MemoryCodexSessions::default());
    let notes = MemoryNotesStore::with_state(state(
        CodexSessionOrigin::Managed,
        CodexNotesReplica::native(account()),
    ));
    let append_gate = Gate::new();
    notes.attach_gate(NotesStage::Append, &append_gate);
    let provider = provider_with_codex_sessions_and_native_notes(
        &store,
        sessions,
        format!("{}/backend-api", server.uri()),
        notes_service(&notes),
    );
    let (service, _) = native_service_with_mode(provider, CodexAccessMode::SessionPool);
    let mut call = Box::pin(service.codex_auxiliary(
        prepare(&service).await,
        notes_request(
            CodexAuxiliaryOperation::NotesWriteFile,
            &write_body("cipher"),
        ),
    ));
    // 交付已经交接：调用方仍在等待结果，真实发送还没有开始
    assert!(futures::poll!(&mut call).is_pending());
    append_gate.wait(1).await;
    // 直接丢弃调用方 future；工作不能被连带丢弃
    drop(call);
    append_gate.release(1);
    notes.completions().wait(1).await;
    let operation = responded_operation(&notes);
    assert_eq!(operation.status, CodexNotesOperationStatus::Responded);
    assert_eq!(operation.response.as_deref(), Some(REPLAYED));
    assert_eq!(
        server
            .received_requests()
            .await
            .expect("received requests")
            .len(),
        1,
        "the dropped caller must not duplicate the upstream write"
    );
}

#[tokio::test]
async fn caller_cancellation_after_handoff_still_persists_the_observation() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/backend-api/codex/alpha/notes/v2/append_to_file"))
        .respond_with(ResponseTemplate::new(200).set_body_string(REPLAYED))
        .mount(&server)
        .await;
    let store = Arc::new(MemoryAccountStore::default());
    seed(&store, ACCOUNT, true, false).await;
    let notes = MemoryNotesStore::with_state(state(
        CodexSessionOrigin::Managed,
        CodexNotesReplica::native(account()),
    ));
    let append_gate = Gate::new();
    notes.attach_gate(NotesStage::Append, &append_gate);
    let provider = provider_with_codex_sessions_and_native_notes(
        &store,
        Arc::new(MemoryCodexSessions::default()),
        format!("{}/backend-api", server.uri()),
        notes_service(&notes),
    );
    let (service, _) = native_service_with_mode(provider, CodexAccessMode::SessionPool);
    let prepared = prepare(&service).await;
    let caller_cancellation = prepared.cancellation();
    let mut call = Box::pin(service.codex_auxiliary(
        prepared,
        notes_request(
            CodexAuxiliaryOperation::NotesAppendToFile,
            &write_body("cipher"),
        ),
    ));
    assert!(futures::poll!(&mut call).is_pending());
    append_gate.wait(1).await;
    caller_cancellation.cancel();
    // 调用方立即结束，但交接后的发送与落库仍要完成
    call.await.expect_err("cancelled caller");
    append_gate.release(1);
    notes.completions().wait(1).await;
    let operation = responded_operation(&notes);
    assert_eq!(operation.status, CodexNotesOperationStatus::Responded);
    assert_eq!(
        server
            .received_requests()
            .await
            .expect("received requests")
            .len(),
        1
    );
}

#[tokio::test]
async fn assembled_notes_ports_register_a_real_delivery_daemon() {
    let server = MockServer::start().await;
    let store = Arc::new(MemoryAccountStore::default());
    seed(&store, ACCOUNT, true, false).await;
    let notes = MemoryNotesStore::with_state(state(
        CodexSessionOrigin::Managed,
        CodexNotesReplica::native(account()),
    ));
    let (_, task) = provider_and_notes_delivery_task(
        &store,
        Arc::new(MemoryCodexSessions::default()),
        format!("{}/backend-api", server.uri()),
        Arc::new(TestLeaseCoordinator::default()),
        notes_service(&notes),
    );
    // 装配了 notes 持久端口时必须贡献真实 daemon，而不是禁用项
    let registration = provider_openai::notes_delivery_registration(task.expect("delivery task"))
        .expect("delivery registration");
    assert_eq!(registration.id.kind(), WorkerKind::CodexNotesDelivery);
    assert!(
        matches!(registration.runnable, WorkerRunnable::Daemon { .. }),
        "the notes delivery owner must run as a supervised daemon"
    );
}

#[tokio::test]
async fn legacy_resolution_after_the_delivery_lease_refuses_before_any_intent() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/backend-api/codex/alpha/notes/v2/write_file"))
        .respond_with(ResponseTemplate::new(200).set_body_string(REPLAYED))
        .mount(&server)
        .await;
    let store = Arc::new(MemoryAccountStore::default());
    seed(&store, ACCOUNT, true, false).await;
    let sessions = Arc::new(MemoryCodexSessions::default());
    let claim_gate = Gate::new();
    sessions.gate_claims(&claim_gate);
    // 未认领会话：先按准入规则准备成交付接管
    let notes = MemoryNotesStore::claiming(Arc::clone(&sessions), CodexSessionOrigin::Managed);
    let leases = Arc::new(TestLeaseCoordinator::default());
    let (provider, task) = provider_and_notes_delivery_task(
        &store,
        Arc::clone(&sessions),
        format!("{}/backend-api", server.uri()),
        Arc::clone(&leases),
        notes_service(&notes),
    );
    spawn_notes_delivery(task.expect("delivery task"), CancellationToken::new());
    let (service, _) = native_service_with_mode(provider, CodexAccessMode::SessionPool);
    let mut call = Box::pin(service.codex_auxiliary(
        prepare(&service).await,
        notes_request(
            CodexAuxiliaryOperation::NotesWriteFile,
            &write_body("cipher"),
        ),
    ));
    assert!(futures::poll!(&mut call).is_pending());
    // 停在认领之后：交付租约已取得并持有，最终落点尚未复读
    tokio::select! {
        () = claim_gate.wait(1) => {}
        _ = &mut call => panic!("the write completed before the claim gate"),
    }
    let held = leases.latest_held().expect("delivery lease slot");
    assert!(
        !held.cancellation.is_cancelled(),
        "the first bind must hold the delivery lease here"
    );
    // 另一请求按既有来源判定把同一会话固定为 legacy
    notes.resolve_unclaimed_as(CodexSessionOrigin::Legacy);
    claim_gate.release(1);
    // 交付接管不成立，但租约已按交付生命周期取得：在任何意图与发送之前拒绝
    call.await
        .expect_err("a session that became legacy keeps no delivery lease");
    assert!(
        notes.operations().is_empty(),
        "no notes intent may be recorded for the refused attempt"
    );
    assert!(
        server
            .received_requests()
            .await
            .expect("received requests")
            .is_empty(),
        "no notes bytes may leave the gateway for the refused attempt"
    );
    assert!(
        leases.held().is_empty(),
        "the refused attempt releases its slot"
    );
    // 后续请求走正常已知 legacy 路径：不再申请交付租约，也不记录日志
    let lease_requests = leases.requests.lock().expect("requests").len();
    let response = service
        .codex_auxiliary(
            prepare(&service).await,
            notes_request(
                CodexAuxiliaryOperation::NotesWriteFile,
                &write_body("cipher"),
            ),
        )
        .await
        .expect("known legacy passthrough");
    assert_eq!(
        leases.requests.lock().expect("requests").len(),
        lease_requests,
        "a known legacy session must not take a delivery lease"
    );
    assert!(notes.operations().is_empty());
    let (status, _) = read_body(response).await;
    assert_eq!(status, 200);
    assert_eq!(
        server
            .received_requests()
            .await
            .expect("received requests")
            .len(),
        1
    );
}

#[tokio::test]
async fn known_legacy_in_the_session_lock_takes_no_delivery_lease() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/backend-api/codex/alpha/notes/v2/write_file"))
        .respond_with(ResponseTemplate::new(200).set_body_string(REPLAYED))
        .mount(&server)
        .await;
    let store = Arc::new(MemoryAccountStore::default());
    seed(&store, ACCOUNT, true, false).await;
    let sessions = Arc::new(MemoryCodexSessions::default());
    // 探针仍看到托管会话，锁内复读被另一请求固定成 legacy
    let notes = MemoryNotesStore::with_state(state(
        CodexSessionOrigin::Managed,
        CodexNotesReplica::native(account()),
    ));
    let service_handle = notes_service(&notes);
    let leases = Arc::new(TestLeaseCoordinator::default());
    let (provider, task) = provider_and_notes_delivery_task(
        &store,
        Arc::clone(&sessions),
        format!("{}/backend-api", server.uri()),
        Arc::clone(&leases),
        Arc::clone(&service_handle),
    );
    let key_id = ClientApiKeyId::new("key_native_test").expect("client key");
    let session_key = auxiliary_session_key(&provider, SESSION);
    sessions.seed_owner(&key_id, &session_key, account());
    spawn_notes_delivery(task.expect("delivery task"), CancellationToken::new());
    let (service, _) = native_service_with_mode(provider, CodexAccessMode::SessionPool);
    // 测试先持有多会话互斥：请求在锁外探测之后、锁内复读之前停住
    let guard = service_handle.lock_session(&key_id, &session_key).await;
    let mut call = Box::pin(service.codex_auxiliary(
        prepare(&service).await,
        notes_request(
            CodexAuxiliaryOperation::NotesWriteFile,
            &write_body("cipher"),
        ),
    ));
    assert!(futures::poll!(&mut call).is_pending());
    notes.resolve_session_as(CodexSessionOrigin::Legacy);
    drop(guard);
    let response = call.await.expect("known legacy passthrough");
    let (status, _) = read_body(response).await;
    assert_eq!(status, 200);
    // 锁内已是 legacy：不申请交付租约、不占容量、不记录日志
    assert!(
        leases.requests.lock().expect("requests").is_empty(),
        "a session already known to be legacy must not request a delivery lease"
    );
    assert!(notes.operations().is_empty());
}

#[tokio::test]
async fn unmanaged_preparation_keeps_the_caller_budget_beyond_the_delivery_window() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/backend-api/codex/alpha/notes/v2/write_file"))
        .respond_with(ResponseTemplate::new(200).set_body_string(REPLAYED))
        .mount(&server)
        .await;
    let store = Arc::new(MemoryAccountStore::default());
    seed(&store, ACCOUNT, true, false).await;
    let sessions = Arc::new(MemoryCodexSessions::default());
    // 受控准备耗时超过托管交付的 60 秒准备窗口，但仍在调用方/选号预算内
    sessions.delay_claims(std::time::Duration::from_secs(70));
    let notes = MemoryNotesStore::claiming(Arc::clone(&sessions), CodexSessionOrigin::Legacy);
    let leases = Arc::new(TestLeaseCoordinator::default());
    let (provider, task) = provider_and_notes_delivery_task(
        &store,
        Arc::clone(&sessions),
        format!("{}/backend-api", server.uri()),
        Arc::clone(&leases),
        notes_service_with_admission(&notes, false),
    );
    spawn_notes_delivery(task.expect("delivery task"), CancellationToken::new());
    let (service, _) = native_service_with_mode(provider, CodexAccessMode::SessionPool);
    // 暂停时钟：受控准备按虚拟时间推进，结束后端口内部恢复真实时钟再做 loopback 发送
    tokio::time::pause();
    let response = service
        .codex_auxiliary(
            prepare(&service).await,
            notes_request(
                CodexAuxiliaryOperation::NotesWriteFile,
                &write_body("cipher"),
            ),
        )
        .await
        .expect("an unmanaged path keeps the caller budget");
    let (status, _) = read_body(response).await;
    assert_eq!(status, 200);
    // 未接管：不记录日志，也不申请交付租约
    assert!(notes.operations().is_empty());
    assert!(leases.held().is_empty());
}

#[tokio::test]
async fn managed_preparation_window_refuses_before_any_intent_or_send() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/backend-api/codex/alpha/notes/v2/write_file"))
        .respond_with(ResponseTemplate::new(200).set_body_string(REPLAYED))
        .mount(&server)
        .await;
    let store = Arc::new(MemoryAccountStore::default());
    seed(&store, ACCOUNT, true, false).await;
    let sessions = Arc::new(MemoryCodexSessions::default());
    // 同一受控准备耗时：接管路径必须被 60 秒准备窗口截断
    sessions.delay_claims(std::time::Duration::from_secs(70));
    let notes = MemoryNotesStore::claiming(Arc::clone(&sessions), CodexSessionOrigin::Managed);
    let leases = Arc::new(TestLeaseCoordinator::default());
    let (provider, task) = provider_and_notes_delivery_task(
        &store,
        Arc::clone(&sessions),
        format!("{}/backend-api", server.uri()),
        Arc::clone(&leases),
        notes_service(&notes),
    );
    spawn_notes_delivery(task.expect("delivery task"), CancellationToken::new());
    let (service, _) = native_service_with_mode(provider, CodexAccessMode::SessionPool);
    tokio::time::pause();
    service
        .codex_auxiliary(
            prepare(&service).await,
            notes_request(
                CodexAuxiliaryOperation::NotesWriteFile,
                &write_body("cipher"),
            ),
        )
        .await
        .expect_err("the managed preparation window is enforced");
    assert!(
        notes.operations().is_empty(),
        "a preparation timeout must not record a write intent"
    );
    assert!(
        server
            .received_requests()
            .await
            .expect("received requests")
            .is_empty(),
        "a preparation timeout must not reach the upstream"
    );
    assert!(leases.held().is_empty());
}

#[tokio::test]
async fn notes_write_that_arrives_before_the_model_request_binds_the_session() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/backend-api/codex/alpha/notes/v2/write_file"))
        .respond_with(ResponseTemplate::new(200).set_body_string(REPLAYED))
        .mount(&server)
        .await;
    let store = Arc::new(MemoryAccountStore::default());
    seed(&store, ACCOUNT, true, false).await;
    let sessions = Arc::new(MemoryCodexSessions::default());
    // 会话还没有状态行：本次 notes 写是第一次触达，必须先完成首绑再读落点
    let notes = MemoryNotesStore::claiming(Arc::clone(&sessions), CodexSessionOrigin::Managed);
    let provider = provider_with_codex_sessions_and_native_notes(
        &store,
        Arc::clone(&sessions),
        format!("{}/backend-api", server.uri()),
        notes_service(&notes),
    );
    let (service, _) = native_service_with_mode(provider, CodexAccessMode::SessionPool);
    service
        .codex_auxiliary(
            prepare(&service).await,
            notes_request(
                CodexAuxiliaryOperation::NotesWriteFile,
                &write_body("cipher"),
            ),
        )
        .await
        .expect("a notes write may arrive before the first model request");
    let operations = notes.operations();
    assert_eq!(operations.len(), 1);
    assert_eq!(operations[0].status, CodexNotesOperationStatus::Responded);
    // 首绑只发生一次，且落点与日志都属于认领到的账号
    assert_eq!(sessions.claims().len(), 1);
    assert_eq!(notes.owner(), Some(account()));
}

#[tokio::test]
async fn cancel_before_handoff_leaves_no_intent_and_no_upstream_write() {
    let server = MockServer::start().await;
    let store = Arc::new(MemoryAccountStore::default());
    seed(&store, ACCOUNT, true, false).await;
    let notes = MemoryNotesStore::with_state(state(
        CodexSessionOrigin::Managed,
        CodexNotesReplica::native(account()),
    ));
    let service_handle = notes_service(&notes);
    let provider = provider_with_codex_sessions_and_native_notes(
        &store,
        Arc::new(MemoryCodexSessions::default()),
        format!("{}/backend-api", server.uri()),
        Arc::clone(&service_handle),
    );
    let key_id = ClientApiKeyId::new("key_native_test").expect("client key");
    let session_key = auxiliary_session_key(&provider, SESSION);
    let (service, _) = native_service_with_mode(provider, CodexAccessMode::SessionPool);
    // 另一个写方持有多会话互斥：准备阶段停在取得互斥之前
    let guard = service_handle.lock_session(&key_id, &session_key).await;
    let prepared = prepare(&service).await;
    let caller_cancellation = prepared.cancellation();
    let mut call = Box::pin(service.codex_auxiliary(
        prepared,
        notes_request(
            CodexAuxiliaryOperation::NotesWriteFile,
            &write_body("cipher"),
        ),
    ));
    assert!(futures::poll!(&mut call).is_pending());
    caller_cancellation.cancel();
    call.await.expect_err("cancelled before handoff");
    drop(guard);
    tokio::task::yield_now().await;
    assert!(
        notes.operations().is_empty(),
        "a cancelled preparation must not record a write intent"
    );
    assert!(
        server
            .received_requests()
            .await
            .expect("received requests")
            .is_empty(),
        "a cancelled preparation must not reach the upstream"
    );
}

#[tokio::test]
async fn unbuildable_request_fails_before_any_intent_or_upstream_write() {
    let server = MockServer::start().await;
    let store = Arc::new(MemoryAccountStore::default());
    seed(&store, ACCOUNT, true, false).await;
    let notes = MemoryNotesStore::with_state(state(
        CodexSessionOrigin::Managed,
        CodexNotesReplica::native(account()),
    ));
    let provider = provider_with_codex_sessions_and_native_notes(
        &store,
        Arc::new(MemoryCodexSessions::default()),
        format!("{}/backend-api", server.uri()),
        notes_service(&notes),
    );
    let (service, _) = native_service_with_mode(provider, CodexAccessMode::SessionPool);
    // 可确定的请求头构造错误必须在记录写意图之前返回
    let mut request = notes_request(
        CodexAuxiliaryOperation::NotesWriteFile,
        &write_body("cipher"),
    );
    request.headers.push(MiddlewareHeader::new(
        "x-test",
        Bytes::from_static(b"bad\x7fvalue"),
    ));
    service
        .codex_auxiliary(prepare(&service).await, request)
        .await
        .expect_err("unbuildable request is rejected");
    assert!(
        notes.operations().is_empty(),
        "preparation failure must not record a write intent"
    );
    assert!(
        server
            .received_requests()
            .await
            .expect("received requests")
            .is_empty()
    );
}

#[tokio::test]
async fn committed_start_without_a_response_is_never_overwritten() {
    let server = MockServer::start().await;
    let store = Arc::new(MemoryAccountStore::default());
    seed(&store, ACCOUNT, true, false).await;
    let notes = MemoryNotesStore::with_state(state(
        CodexSessionOrigin::Managed,
        CodexNotesReplica::native(account()),
    ));
    notes.fail_start(StartFailure::Committed);
    let provider = provider_with_codex_sessions_and_native_notes(
        &store,
        Arc::new(MemoryCodexSessions::default()),
        format!("{}/backend-api", server.uri()),
        notes_service(&notes),
    );
    let (service, _) = native_service_with_mode(provider, CodexAccessMode::SessionPool);
    service
        .codex_auxiliary(
            prepare(&service).await,
            notes_request(
                CodexAuxiliaryOperation::NotesWriteFile,
                &write_body("cipher"),
            ),
        )
        .await
        .expect_err("ambiguous start failure is returned");
    // 读回是 InFlight：开始事实可能已提交，不能按“本调用没发送”改判
    assert_eq!(
        notes.status(1),
        Some(CodexNotesOperationStatus::InFlight),
        "an ambiguous start must keep the observed state"
    );
    assert!(
        server
            .received_requests()
            .await
            .expect("received requests")
            .is_empty(),
        "an unconfirmed start must not send bytes"
    );
}

#[tokio::test]
async fn unconfirmed_start_settles_only_the_proven_unsent_operation() {
    let server = MockServer::start().await;
    let store = Arc::new(MemoryAccountStore::default());
    seed(&store, ACCOUNT, true, false).await;
    let notes = MemoryNotesStore::with_state(state(
        CodexSessionOrigin::Managed,
        CodexNotesReplica::native(account()),
    ));
    notes.fail_start(StartFailure::NotCommitted);
    let provider = provider_with_codex_sessions_and_native_notes(
        &store,
        Arc::new(MemoryCodexSessions::default()),
        format!("{}/backend-api", server.uri()),
        notes_service(&notes),
    );
    let (service, _) = native_service_with_mode(provider, CodexAccessMode::SessionPool);
    service
        .codex_auxiliary(
            prepare(&service).await,
            notes_request(
                CodexAuxiliaryOperation::NotesWriteFile,
                &write_body("cipher"),
            ),
        )
        .await
        .expect_err("start failure is returned");
    // 读回仍是 Unsent：字节从未离开网关，按合法 Unsent → Failed 收尾
    assert_eq!(notes.status(1), Some(CodexNotesOperationStatus::Failed));
    assert!(
        server
            .received_requests()
            .await
            .expect("received requests")
            .is_empty()
    );
    // 终态已经终结；未终结日志不会再阻止发布
    migrations_are_not_blocked(&notes);
}

/// 收尾后的日志没有未终结操作
fn migrations_are_not_blocked(notes: &MemoryNotesStore) {
    assert!(
        notes
            .operations()
            .iter()
            .all(|operation| operation.status.is_terminal()),
        "a settled operation must not block migration"
    );
}

#[tokio::test]
async fn completion_commit_without_a_response_keeps_the_recorded_outcome() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/backend-api/codex/alpha/notes/v2/write_file"))
        .respond_with(ResponseTemplate::new(200).set_body_string(REPLAYED))
        .mount(&server)
        .await;
    let store = Arc::new(MemoryAccountStore::default());
    seed(&store, ACCOUNT, true, false).await;
    let notes = MemoryNotesStore::with_state(state(
        CodexSessionOrigin::Managed,
        CodexNotesReplica::native(account()),
    ));
    notes.complete_fails_after_commit();
    let provider = provider_with_codex_sessions_and_native_notes(
        &store,
        Arc::new(MemoryCodexSessions::default()),
        format!("{}/backend-api", server.uri()),
        notes_service(&notes),
    );
    let (service, _) = native_service_with_mode(provider, CodexAccessMode::SessionPool);
    service
        .codex_auxiliary(
            prepare(&service).await,
            notes_request(
                CodexAuxiliaryOperation::NotesWriteFile,
                &write_body("cipher"),
            ),
        )
        .await
        .expect_err("unknown persistence result is reported");
    let operation = responded_operation(&notes);
    // 提交结果可能已经落库：不得补写 Failed/Unknown 覆盖 Responded
    assert_eq!(operation.status, CodexNotesOperationStatus::Responded);
    assert_eq!(operation.response.as_deref(), Some(REPLAYED));
    assert_eq!(
        server
            .received_requests()
            .await
            .expect("received requests")
            .len(),
        1,
        "a persistence failure must not resend the write"
    );
}

#[tokio::test]
async fn session_mutex_outlives_the_dropped_caller_until_completion() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/backend-api/codex/alpha/notes/v2/write_file"))
        .respond_with(ResponseTemplate::new(200).set_body_string(REPLAYED))
        .mount(&server)
        .await;
    let store = Arc::new(MemoryAccountStore::default());
    seed(&store, ACCOUNT, true, false).await;
    let notes = MemoryNotesStore::with_state(state(
        CodexSessionOrigin::Managed,
        CodexNotesReplica::native(account()),
    ));
    let append_gate = Gate::new();
    notes.attach_gate(NotesStage::Append, &append_gate);
    let provider = provider_with_codex_sessions_and_native_notes(
        &store,
        Arc::new(MemoryCodexSessions::default()),
        format!("{}/backend-api", server.uri()),
        notes_service(&notes),
    );
    let (service, _) = native_service_with_mode(provider, CodexAccessMode::SessionPool);
    let mut first = Box::pin(service.codex_auxiliary(
        prepare(&service).await,
        notes_request(
            CodexAuxiliaryOperation::NotesWriteFile,
            &write_body("first"),
        ),
    ));
    assert!(futures::poll!(&mut first).is_pending());
    append_gate.wait(1).await;
    drop(first);
    // 第二个写方必须等同会话落库结束；交接后的互斥没有随调用方消失
    let mut second = Box::pin(service.codex_auxiliary(
        prepare(&service).await,
        notes_request(
            CodexAuxiliaryOperation::NotesWriteFile,
            &write_body("second"),
        ),
    ));
    for _ in 0..8 {
        assert!(
            futures::poll!(&mut second).is_pending(),
            "the second write must wait for the first delivery"
        );
    }
    assert_eq!(
        append_gate.entered(),
        1,
        "the second write must not reach the append stage"
    );
    append_gate.release(2);
    second.await.expect("second write");
    notes.completions().wait(2).await;
    let operations = notes.operations();
    assert_eq!(operations.len(), 2);
    assert!(operations.iter().all(|operation| {
        operation.status == CodexNotesOperationStatus::Responded
            && operation.response.as_deref() == Some(REPLAYED)
    }));
    assert_eq!(operations[0].seq, 1);
    assert_eq!(operations[1].seq, 2);
}

#[tokio::test]
async fn first_bind_lease_covers_the_delivery_after_the_post_acquisition_preparation() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/backend-api/codex/alpha/notes/v2/write_file"))
        .respond_with(ResponseTemplate::new(200).set_body_string(REPLAYED))
        .mount(&server)
        .await;
    let store = Arc::new(MemoryAccountStore::default());
    seed(&store, ACCOUNT, true, false).await;
    let notes = MemoryNotesStore::with_state(state(
        CodexSessionOrigin::Managed,
        CodexNotesReplica::native(account()),
    ));
    // 取得租约之后才进入的异步准备：认领停在这里，证明预算覆盖准备而不只是锁等待
    let claim_gate = Gate::new();
    let sessions = Arc::new(MemoryCodexSessions::default());
    sessions.gate_claims(&claim_gate);
    let append_gate = Gate::new();
    notes.attach_gate(NotesStage::Append, &append_gate);
    let complete_gate = Gate::new();
    notes.attach_gate(NotesStage::Complete, &complete_gate);
    let leases = Arc::new(TestLeaseCoordinator::default());
    let (provider, task) = provider_and_notes_delivery_task(
        &store,
        Arc::clone(&sessions),
        format!("{}/backend-api", server.uri()),
        Arc::clone(&leases),
        notes_service(&notes),
    );
    spawn_notes_delivery(task.expect("delivery task"), CancellationToken::new());
    let (service, _) = native_service_with_mode(provider, CodexAccessMode::SessionPool);
    let prepared = prepare(&service).await;
    let caller_cancellation = prepared.cancellation();
    let mut call = Box::pin(service.codex_auxiliary(
        prepared,
        notes_request(
            CodexAuxiliaryOperation::NotesWriteFile,
            &write_body("cipher"),
        ),
    ));
    assert!(futures::poll!(&mut call).is_pending());
    // 租约已取得、准备停在认领之后；必须继续驱动调用方才能推进交接
    tokio::select! {
        () = claim_gate.wait(1) => {}
        _ = &mut call => panic!("the write completed before the claim gate"),
    }
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let lease = leases.latest_held().expect("first-bind lease slot");
    // 准备消耗两秒后，槽位剩余仍须覆盖完整交付预算（旧实现只剩 155s-2s，必然失败）
    assert!(
        lease.remaining_at(std::time::SystemTime::now())
            >= provider_openai::NOTES_DELIVERY_LEASE_BUDGET,
        "post-acquisition preparation must not consume the delivery budget: {:?}",
        lease.remaining_at(std::time::SystemTime::now())
    );
    claim_gate.release(1);
    // 交接完成：工作已由交付 owner 持有
    tokio::select! {
        () = append_gate.wait(1) => {}
        _ = &mut call => panic!("the write completed before it was handed off"),
    }
    caller_cancellation.cancel();
    call.await.expect_err("cancelled caller");
    // 调用方取消不停止续期：槽位仍覆盖发送与落库
    assert!(
        !lease.cancellation.is_cancelled(),
        "the delivery lease must not follow the caller cancellation"
    );
    append_gate.release(1);
    // 交付已发送并读完正文，停在落库之前；drain 期间槽位必须仍有效
    complete_gate.wait(1).await;
    let observing_at = std::time::SystemTime::now();
    let lease = leases
        .latest_held()
        .expect("slot still held while observing");
    assert!(
        lease.covers(observing_at),
        "the account slot must stay valid until the observation ends"
    );
    complete_gate.release(1);
    notes.completions().wait(1).await;
    let completed_at = std::time::SystemTime::now();
    assert_eq!(
        notes.status(1),
        Some(CodexNotesOperationStatus::Responded),
        "the delivery must finish after the caller left"
    );
    assert!(
        lease.covers(completed_at),
        "the slot must not lapse before the delivery completes"
    );
    assert_eq!(leases.live_leases(), 0);
    assert!(leases.held().is_empty());
}

#[tokio::test]
async fn legacy_first_bind_keeps_the_caller_lease_lifecycle() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/backend-api/codex/alpha/notes/v2/write_file"))
        .respond_with(ResponseTemplate::new(200).set_body_string(REPLAYED))
        .mount(&server)
        .await;
    let store = Arc::new(MemoryAccountStore::default());
    seed(&store, ACCOUNT, true, false).await;
    let sessions = Arc::new(MemoryCodexSessions::default());
    // 关闭托管准入：无状态会话在认领后按既有准入规则固定为 legacy
    let notes = MemoryNotesStore::claiming(Arc::clone(&sessions), CodexSessionOrigin::Legacy);
    let claim_gate = Gate::new();
    sessions.gate_claims(&claim_gate);
    let leases = Arc::new(TestLeaseCoordinator::default());
    let (provider, task) = provider_and_notes_delivery_task(
        &store,
        Arc::clone(&sessions),
        format!("{}/backend-api", server.uri()),
        Arc::clone(&leases),
        notes_service_with_admission(&notes, false),
    );
    spawn_notes_delivery(task.expect("delivery task"), CancellationToken::new());
    let (service, _) = native_service_with_mode(provider, CodexAccessMode::SessionPool);
    let prepared = prepare(&service).await;
    let caller_cancellation = prepared.cancellation();
    let mut call = Box::pin(service.codex_auxiliary(
        prepared,
        notes_request(
            CodexAuxiliaryOperation::NotesWriteFile,
            &write_body("cipher"),
        ),
    ));
    assert!(futures::poll!(&mut call).is_pending());
    tokio::select! {
        () = claim_gate.wait(1) => {}
        _ = &mut call => panic!("the write must reach the claim before it can complete"),
    }
    let lease = leases.latest_held().expect("first-bind lease slot");
    // 未被交付接管的 legacy 写沿用调用方期限：这里调用方没有显式期限，
    // 槽位因此按 REQUEST_LEASE_TTL 兜底，而不是交付预留的期限
    let remaining = lease.remaining_at(std::time::SystemTime::now());
    assert!(
        remaining > provider_openai::NOTES_DELIVERY_LEASE_BUDGET,
        "legacy first bind must keep the caller deadline instead of the delivery reservation: {remaining:?}"
    );
    caller_cancellation.cancel();
    // 续期取消来源仍是调用方：取消后租约随之失效
    assert!(
        lease.cancellation.is_cancelled(),
        "legacy first bind must keep the caller cancellation"
    );
    claim_gate.release(1);
    let _ = call.await;
    // 未记录日志，沿既有转发语义
    assert!(notes.operations().is_empty());
}

#[tokio::test]
async fn delivery_capacity_refuses_the_next_write_before_any_intent() {
    /// 与交付 owner 的本地上限一致：占满后第 N+1 个写必须被拒绝
    const IN_FLIGHT: usize = 32;
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/backend-api/codex/alpha/notes/v2/write_file"))
        .respond_with(ResponseTemplate::new(200).set_body_string(REPLAYED))
        .mount(&server)
        .await;
    let store = Arc::new(MemoryAccountStore::default());
    seed(&store, ACCOUNT, true, false).await;
    let notes = MemoryNotesStore::with_state(state(
        CodexSessionOrigin::Managed,
        CodexNotesReplica::native(account()),
    ));
    let append_gate = Gate::new();
    notes.attach_gate(NotesStage::Append, &append_gate);
    let provider = provider_with_codex_sessions_and_native_notes(
        &store,
        Arc::new(MemoryCodexSessions::default()),
        format!("{}/backend-api", server.uri()),
        notes_service(&notes),
    );
    let (service, _) = native_service_with_mode(provider, CodexAccessMode::SessionPool);
    // 每个写交接后调用方立即离开：容量由交付 owner 持有，直到工作真正结束
    for index in 0..IN_FLIGHT {
        let body = write_body_for_session(&format!("session-capacity-{index}"), "cipher");
        let mut call = Box::pin(service.codex_auxiliary(
            prepare(&service).await,
            notes_request(CodexAuxiliaryOperation::NotesWriteFile, &body),
        ));
        assert!(futures::poll!(&mut call).is_pending());
        append_gate.wait(index + 1).await;
        drop(call);
    }
    assert_eq!(append_gate.entered(), IN_FLIGHT);
    let refused = service
        .codex_auxiliary(
            prepare(&service).await,
            notes_request(
                CodexAuxiliaryOperation::NotesWriteFile,
                &write_body_for_session("session-capacity-refused", "cipher"),
            ),
        )
        .await
        .expect_err("capacity is exhausted");
    assert!(
        notes.operations().is_empty(),
        "a refused write must not record an intent: {refused:?}"
    );
    assert_eq!(
        append_gate.entered(),
        IN_FLIGHT,
        "a refused write must not reach the append stage"
    );
    // 多放行一个许可留给容量恢复后的写
    append_gate.release(IN_FLIGHT + 1);
    notes.completions().wait(IN_FLIGHT).await;
    assert_eq!(
        notes.operations().len(),
        IN_FLIGHT,
        "only the admitted writes may be recorded"
    );
    assert!(
        notes
            .operations()
            .iter()
            .all(|operation| operation.status == CodexNotesOperationStatus::Responded),
        "every admitted write must still land its real observation"
    );
    // 工作结束后容量恢复
    service
        .codex_auxiliary(
            prepare(&service).await,
            notes_request(
                CodexAuxiliaryOperation::NotesWriteFile,
                &write_body_for_session("session-capacity-recovered", "cipher"),
            ),
        )
        .await
        .expect("capacity recovered");
    assert_eq!(notes.operations().len(), IN_FLIGHT + 1);
}

#[tokio::test]
async fn capacity_counts_same_session_waiters_before_the_session_mutex() {
    const IN_FLIGHT: usize = 32;
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/backend-api/codex/alpha/notes/v2/write_file"))
        .respond_with(ResponseTemplate::new(200).set_body_string(REPLAYED))
        .mount(&server)
        .await;
    let store = Arc::new(MemoryAccountStore::default());
    seed(&store, ACCOUNT, true, false).await;
    let notes = MemoryNotesStore::with_state(state(
        CodexSessionOrigin::Managed,
        CodexNotesReplica::native(account()),
    ));
    let append_gate = Gate::new();
    notes.attach_gate(NotesStage::Append, &append_gate);
    let leases = Arc::new(TestLeaseCoordinator::default());
    let (provider, task) = provider_and_notes_delivery_task(
        &store,
        Arc::new(MemoryCodexSessions::default()),
        format!("{}/backend-api", server.uri()),
        leases,
        notes_service(&notes),
    );
    // 保留任务句柄以便直接观察容量准入，同时按 Host 方式运行同一 daemon
    let task = Arc::new(task.expect("delivery task"));
    let capacity = Arc::clone(&task);
    tokio::spawn(async move {
        let context = gateway_core::task::WorkerDaemonContext::new(CancellationToken::new());
        let _ = gateway_core::task::DaemonTask::run(&*task, context).await;
    });
    let service = Arc::new(native_service_with_parallel_admissions(
        provider,
        CodexAccessMode::SessionPool,
    ));
    // 第一个写：交付持有会话互斥并停在 append（同一会话键，全部请求共用）
    let first = {
        let service = Arc::clone(&service);
        let body = write_body("first");
        tokio::spawn(async move {
            service
                .codex_auxiliary(
                    prepare(&service).await,
                    notes_request(CodexAuxiliaryOperation::NotesWriteFile, &body),
                )
                .await
        })
    };
    append_gate.wait(1).await;
    // 同一会话再提交其余在途写：它们在会话互斥之前就占用容量
    let mut waiters = Vec::new();
    for index in 1..IN_FLIGHT {
        let service = Arc::clone(&service);
        let body = write_body(&format!("waiter-{index}"));
        waiters.push(tokio::spawn(async move {
            service
                .codex_auxiliary(
                    prepare(&service).await,
                    notes_request(CodexAuxiliaryOperation::NotesWriteFile, &body),
                )
                .await
        }));
    }
    // 等容量真正被占满：同会话等待者也计入在途上限
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while capacity.available_capacity() > 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("same-session waiters must occupy the delivery capacity");
    // 第 33 个同会话写在取得会话互斥之前就被拒绝，且零意图、零发送
    let refused = service
        .codex_auxiliary(
            prepare(&service).await,
            notes_request(
                CodexAuxiliaryOperation::NotesWriteFile,
                &write_body("refused"),
            ),
        )
        .await
        .expect_err("capacity is exhausted for the same session");
    assert!(notes.operations().is_empty(), "{refused:?}");
    assert_eq!(append_gate.entered(), 1);
    append_gate.release(IN_FLIGHT);
    notes.completions().wait(IN_FLIGHT).await;
    first.await.expect("first write task").expect("first write");
    for waiter in waiters {
        waiter.await.expect("waiter task").expect("waiter write");
    }
    assert_eq!(notes.operations().len(), IN_FLIGHT);
}

#[tokio::test]
async fn graceful_close_keeps_the_delivery_lease_until_the_observation_ends() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/backend-api/codex/alpha/notes/v2/write_file"))
        .respond_with(ResponseTemplate::new(200).set_body_string(REPLAYED))
        .mount(&server)
        .await;
    let store = Arc::new(MemoryAccountStore::default());
    seed(&store, ACCOUNT, true, false).await;
    let notes = MemoryNotesStore::with_state(state(
        CodexSessionOrigin::Managed,
        CodexNotesReplica::native(account()),
    ));
    let complete_gate = Gate::new();
    notes.attach_gate(NotesStage::Complete, &complete_gate);
    let leases = Arc::new(TestLeaseCoordinator::default());
    let (provider, task) = provider_and_notes_delivery_task(
        &store,
        Arc::new(MemoryCodexSessions::default()),
        format!("{}/backend-api", server.uri()),
        Arc::clone(&leases),
        notes_service(&notes),
    );
    let shutdown = CancellationToken::new();
    let daemon = spawn_notes_delivery(task.expect("delivery task"), shutdown.clone());
    let (service, _) = native_service_with_mode(provider, CodexAccessMode::SessionPool);
    let prepared = prepare(&service).await;
    let caller_cancellation = prepared.cancellation();
    let mut call = Box::pin(service.codex_auxiliary(
        prepared,
        notes_request(
            CodexAuxiliaryOperation::NotesWriteFile,
            &write_body("cipher"),
        ),
    ));
    assert!(futures::poll!(&mut call).is_pending());
    // 交付已经发送并读完正文，停在落库之前；此间调用方离开
    complete_gate.wait(1).await;
    let lease = leases.latest_held().expect("first-bind lease slot");
    assert_eq!(lease.account_id, account());
    caller_cancellation.cancel();
    call.await.expect_err("cancelled caller");
    // Host 优雅关闭：先停止接收，再让在途工作完成观察
    shutdown.cancel();
    // 关闭不再接收新工作，但在途工作继续完成观察；落库许可必须早于 30s 持久上限
    complete_gate.release(1);
    daemon.await.expect("delivery daemon exits");
    let completed_at = std::time::SystemTime::now();
    assert!(
        notes
            .operations()
            .iter()
            .all(|operation| operation.status.is_terminal()),
        "the drain must finish the in-flight observation before the daemon exits"
    );
    assert_eq!(
        notes.status(1),
        Some(CodexNotesOperationStatus::Responded),
        "a delivered write must still record its real observation during the drain"
    );
    assert!(
        lease.covers(completed_at),
        "the account slot must cover the drain of an in-flight delivery"
    );
    assert!(leases.held().is_empty());
    // 关闭后不再接收新工作：新写在准备阶段被拒绝，不写意图
    service
        .codex_auxiliary(
            prepare(&service).await,
            notes_request(
                CodexAuxiliaryOperation::NotesWriteFile,
                &write_body("after-close"),
            ),
        )
        .await
        .expect_err("closed delivery refuses the write");
    assert_eq!(notes.operations().len(), 1);
}

#[tokio::test]
async fn hard_kill_drops_the_lease_without_rewriting_the_operation() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/backend-api/codex/alpha/notes/v2/write_file"))
        .respond_with(ResponseTemplate::new(200).set_body_string(REPLAYED))
        .mount(&server)
        .await;
    let store = Arc::new(MemoryAccountStore::default());
    seed(&store, ACCOUNT, true, false).await;
    let notes = MemoryNotesStore::with_state(state(
        CodexSessionOrigin::Managed,
        CodexNotesReplica::native(account()),
    ));
    let complete_gate = Gate::new();
    notes.attach_gate(NotesStage::Complete, &complete_gate);
    let leases = Arc::new(TestLeaseCoordinator::default());
    let (provider, task) = provider_and_notes_delivery_task(
        &store,
        Arc::new(MemoryCodexSessions::default()),
        format!("{}/backend-api", server.uri()),
        Arc::clone(&leases),
        notes_service(&notes),
    );
    let daemon = spawn_notes_delivery(task.expect("delivery task"), CancellationToken::new());
    let (service, _) = native_service_with_mode(provider, CodexAccessMode::SessionPool);
    let mut call = Box::pin(service.codex_auxiliary(
        prepare(&service).await,
        notes_request(
            CodexAuxiliaryOperation::NotesWriteFile,
            &write_body("cipher"),
        ),
    ));
    assert!(futures::poll!(&mut call).is_pending());
    complete_gate.wait(1).await;
    assert_eq!(leases.live_leases(), 1);
    // Host 硬终止：任务被 abort，交付 future 连同 guard、互斥与租约一起释放
    daemon.abort();
    let _ = daemon.await;
    drop(call);
    tokio::time::timeout(GATE_WAIT_TIMEOUT, async {
        while leases.live_leases() > 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("aborted delivery must release the account slot");
    assert!(leases.held().is_empty());
    // 允许留下未终结操作：不补写终态、不重放
    assert_eq!(
        notes.status(1),
        Some(CodexNotesOperationStatus::InFlight),
        "a hard kill leaves the operation unsettled instead of rewriting it"
    );
}

#[tokio::test]
async fn closing_the_delivery_gate_refuses_new_writes_without_intent() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/backend-api/codex/alpha/notes/v2/write_file"))
        .respond_with(ResponseTemplate::new(200).set_body_string(REPLAYED))
        .mount(&server)
        .await;
    let store = Arc::new(MemoryAccountStore::default());
    seed(&store, ACCOUNT, true, false).await;
    let notes = MemoryNotesStore::with_state(state(
        CodexSessionOrigin::Managed,
        CodexNotesReplica::native(account()),
    ));
    let leases = Arc::new(TestLeaseCoordinator::default());
    let (provider, task) = provider_and_notes_delivery_task(
        &store,
        Arc::new(MemoryCodexSessions::default()),
        format!("{}/backend-api", server.uri()),
        leases,
        notes_service(&notes),
    );
    let cancellation = CancellationToken::new();
    let daemon = spawn_notes_delivery(task.expect("delivery task"), cancellation.clone());
    let (service, _) = native_service_with_mode(provider, CodexAccessMode::SessionPool);
    service
        .codex_auxiliary(
            prepare(&service).await,
            notes_request(
                CodexAuxiliaryOperation::NotesWriteFile,
                &write_body("cipher"),
            ),
        )
        .await
        .expect("write before close");
    cancellation.cancel();
    daemon.await.expect("delivery daemon exits");
    service
        .codex_auxiliary(
            prepare(&service).await,
            notes_request(
                CodexAuxiliaryOperation::NotesWriteFile,
                &write_body("cipher"),
            ),
        )
        .await
        .expect_err("closed delivery refuses the write");
    assert_eq!(
        notes.operations().len(),
        1,
        "a closed delivery must not record a new intent"
    );
}

#[tokio::test]
async fn stale_migration_intent_replays_nothing_after_a_concurrent_publish() {
    let server = MockServer::start().await;
    for route in ["write_file", "append_to_file"] {
        Mock::given(method("POST"))
            .and(path(format!("/backend-api/codex/alpha/notes/v2/{route}")))
            .respond_with(ResponseTemplate::new(200).set_body_string(REPLAYED))
            .mount(&server)
            .await;
    }
    let store = Arc::new(MemoryAccountStore::default());
    seed_workspace(&store, OWNER_A, true, false, &format!("chatgpt-{OWNER_A}")).await;
    seed_workspace(&store, OWNER_B, true, false, &format!("chatgpt-{OWNER_B}")).await;
    let notes = MemoryNotesStore::with_state(migration_state(CodexSessionOrigin::Managed, OWNER_A));
    migration_log(&notes);
    let provider = provider_with_codex_sessions_and_native_notes(
        &store,
        Arc::new(MemoryCodexSessions::default()),
        format!("{}/backend-api", server.uri()),
        notes_service(&notes),
    );
    let owner_a = ProviderAccountId::new(OWNER_A).expect("owner a");
    let owner_b = ProviderAccountId::new(OWNER_B).expect("owner b");
    let session_key = pool_session_key(&provider, MIGRATE_SESSION);
    let authorization = pool_context("req_stale_migration", CancellationToken::new())
        .session_authorization()
        .expect("session authorization");
    let revision_b = store
        .account(OWNER_B)
        .expect("seeded account")
        .revision()
        .get();
    // 选号时观测到的迁移意图：A 与代次 3
    let intent = CodexNotesSessionExpectation {
        owner: owner_a.clone(),
        generation: 3,
    };
    // 第一个请求按该意图重放冻结日志并发布 B
    let published = provider
        .rebuild_notes(&authorization, &session_key, &owner_b, revision_b, &intent)
        .await
        .expect("first migration publishes");
    assert!(matches!(published, CodexNotesPublishOutcome::Published(_)));
    let replayed = server
        .received_requests()
        .await
        .expect("received requests")
        .len();
    assert_eq!(replayed, 2, "the accepted intent replays the frozen log");
    // 并发请求已发布 B；同一旧意图随后取得会话锁时必须零重放、零发布
    let stale = provider
        .rebuild_notes(&authorization, &session_key, &owner_b, revision_b, &intent)
        .await
        .expect_err("a stale intent is rejected");
    assert!(stale.retry_is_prohibited());
    assert_eq!(
        server
            .received_requests()
            .await
            .expect("received requests")
            .len(),
        replayed,
        "a stale intent must not replay any operation"
    );
    assert_eq!(
        notes.publishes().len(),
        1,
        "a stale intent must not publish"
    );
    assert_eq!(notes.owner(), Some(owner_b.clone()));
    // 目标恰好等于当前 owner 也不构成成功捷径：代次已经前进
    let same_target = CodexNotesSessionExpectation {
        owner: owner_b.clone(),
        generation: 3,
    };
    let error = provider
        .rebuild_notes(
            &authorization,
            &session_key,
            &owner_b,
            revision_b,
            &same_target,
        )
        .await
        .expect_err("matching the owner alone is not enough");
    assert!(error.retry_is_prohibited());
    assert_eq!(notes.publishes().len(), 1);
    assert_eq!(
        server
            .received_requests()
            .await
            .expect("received requests")
            .len(),
        replayed
    );
    assert_eq!(notes.owner(), Some(owner_b));
}

#[tokio::test]
async fn delivery_lease_budget_is_anchored_at_the_lease_not_the_preparation() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/backend-api/codex/alpha/notes/v2/write_file"))
        .respond_with(ResponseTemplate::new(200).set_body_string(REPLAYED))
        .mount(&server)
        .await;
    let store = Arc::new(MemoryAccountStore::default());
    seed(&store, ACCOUNT, true, false).await;
    let notes = MemoryNotesStore::with_state(state(
        CodexSessionOrigin::Managed,
        CodexNotesReplica::native(account()),
    ));
    let append_gate = Gate::new();
    notes.attach_gate(NotesStage::Append, &append_gate);
    let service_handle = notes_service(&notes);
    let leases = Arc::new(TestLeaseCoordinator::default());
    let (provider, task) = provider_and_notes_delivery_task(
        &store,
        Arc::new(MemoryCodexSessions::default()),
        format!("{}/backend-api", server.uri()),
        Arc::clone(&leases),
        Arc::clone(&service_handle),
    );
    spawn_notes_delivery(task.expect("delivery task"), CancellationToken::new());
    let key_id = ClientApiKeyId::new("key_native_test").expect("client key");
    let session_key = auxiliary_session_key(&provider, SESSION);
    let (service, _) = native_service_with_mode(provider, CodexAccessMode::SessionPool);
    // 准备阶段先在会话互斥上等待：这段等待不能提前消耗交付租约有效期
    let guard = service_handle.lock_session(&key_id, &session_key).await;
    let mut call = Box::pin(service.codex_auxiliary(
        prepare(&service).await,
        notes_request(
            CodexAuxiliaryOperation::NotesWriteFile,
            &write_body("cipher"),
        ),
    ));
    assert!(futures::poll!(&mut call).is_pending());
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    drop(guard);
    // 调用方必须继续被驱动到交接，闸门才能被工作进入
    tokio::select! {
        () = append_gate.wait(1) => {}
        _ = &mut call => panic!("the write must be handed off before it can complete"),
    }
    let request = leases
        .requests
        .lock()
        .expect("lease requests lock")
        .last()
        .cloned()
        .expect("first-bind lease request");
    let remaining = request
        .deadline()
        .remaining()
        .expect("delivery lease keeps a bounded deadline");
    assert!(
        remaining
            > provider_openai::NOTES_DELIVERY_LEASE_BUDGET - std::time::Duration::from_secs(1),
        "the lease budget must start at the lease, not at preparation: {remaining:?}"
    );
    append_gate.release(1);
    call.await.expect("write after the preparation wait");
    notes.completions().wait(1).await;
    assert_eq!(notes.status(1), Some(CodexNotesOperationStatus::Responded));
}

#[tokio::test]
async fn upstream_total_budget_covers_the_full_response_body() {
    // 上游只回响应头、正文永不完整：35 秒整体预算必须覆盖发送到完整响应
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("listener");
    let address = listener.local_addr().expect("listener address");
    let upstream = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept");
        let mut request = [0_u8; 4096];
        let _ = socket.read(&mut request).await;
        socket
            .write_all(
                b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 64\r\n\r\npartial",
            )
            .await
            .expect("response headers");
        std::future::pending::<()>().await;
    });
    let store = Arc::new(MemoryAccountStore::default());
    seed(&store, ACCOUNT, true, false).await;
    let notes = MemoryNotesStore::with_state(state(
        CodexSessionOrigin::Managed,
        CodexNotesReplica::native(account()),
    ));
    let provider = provider_with_codex_sessions_and_native_notes(
        &store,
        Arc::new(MemoryCodexSessions::default()),
        format!("http://{address}/backend-api"),
        notes_service(&notes),
    );
    let (service, _) = native_service_with_mode(provider, CodexAccessMode::SessionPool);
    let started = std::time::Instant::now();
    service
        .codex_auxiliary(
            prepare(&service).await,
            notes_request(
                CodexAuxiliaryOperation::NotesWriteFile,
                &write_body("cipher"),
            ),
        )
        .await
        .expect_err("a stalled body is bounded by the total budget");
    let elapsed = started.elapsed();
    assert!(
        elapsed >= std::time::Duration::from_secs(30),
        "the stalled body must be cut by the overall budget: {elapsed:?}"
    );
    assert!(
        elapsed < std::time::Duration::from_secs(60),
        "the overall budget must not extend into a caller wait: {elapsed:?}"
    );
    notes.completions().wait(1).await;
    // 已收到响应头但正文不完整：结果不明，仍阻止自动迁移
    let operation = responded_operation(&notes);
    assert_eq!(operation.status, CodexNotesOperationStatus::Unknown);
    assert!(operation.response.is_none());
    assert_eq!(operation.response_status, Some(200));
    upstream.abort();
}
