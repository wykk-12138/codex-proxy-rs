//! 验证原生 history 采集的来源判定、位置对齐、父响应精确前缀与窗口切换

use std::sync::{
    Mutex,
    atomic::{AtomicBool, Ordering},
};

use futures::future::BoxFuture;
use gateway_core::account::ProviderAccountId;
use gateway_core::engine::codex_history::CodexHistoryService;
use gateway_core::engine::codex_history_capture::{
    CodexHistoryCaptureError, CodexHistoryCaptureItem, CodexHistoryCapturedRequest,
    CodexHistoryInitialization, CodexHistoryRequestFacts,
};
use gateway_core::policy::ClientApiKeyId;
use gateway_core::provider_ports::{
    CodexHistoryItem, CodexHistoryPendingInput, CodexHistoryRequest, CodexHistoryResponse,
    CodexHistoryResponseAnchor, CodexHistoryRole, CodexHistoryStoredItem, CodexHistoryStoredWindow,
    CodexHistoryWindow, CodexHistoryWindowMetadata, CodexNativeWindowId, CodexNotesCompletion,
    CodexNotesOperation, CodexNotesPublish, CodexNotesPublishOutcome, CodexNotesReplay,
    CodexNotesReplica, CodexSessionContentPort, CodexSessionOrigin, CodexSessionState,
    CodexSessionStatePort, NewCodexNotesOperation, ProviderSessionAffinityKey, ProviderStoreError,
    ProviderStoreErrorKind,
};

fn unavailable(operation: &'static str) -> ProviderStoreError {
    ProviderStoreError::new(ProviderStoreErrorKind::Unavailable, operation)
}

fn key() -> ClientApiKeyId {
    ClientApiKeyId::new("key_native_history").expect("client key id")
}

fn session() -> ProviderSessionAffinityKey {
    ProviderSessionAffinityKey::try_new("session_native_history").expect("session key")
}

fn owner() -> ProviderAccountId {
    ProviderAccountId::new("acct_native_history").expect("account id")
}

fn state(origin: CodexSessionOrigin) -> CodexSessionState {
    CodexSessionState {
        origin,
        owner: owner(),
        generation: 0,
        notes: CodexNotesReplica::native(owner()),
        notes_replay: None,
    }
}

fn window_id(value: &str) -> CodexNativeWindowId {
    CodexNativeWindowId::try_new(value).expect("window id")
}

fn window(value: &str) -> CodexHistoryWindow {
    CodexHistoryWindow {
        window_id: window_id(value),
        agent_name: Some("/root".to_owned()),
        context_window_id: None,
    }
}

/// 一次模型请求的原生状态事实
fn facts(
    window_value: &str,
    previous_response_id: Option<&str>,
    request_kind: Option<&str>,
    initialization: CodexHistoryInitialization,
) -> CodexHistoryRequestFacts {
    CodexHistoryRequestFacts {
        key_id: key(),
        session_key: session(),
        window: window(window_value),
        previous_response_id: previous_response_id.map(ToOwned::to_owned),
        request_kind: request_kind.map(ToOwned::to_owned),
        history_initialization: initialization,
    }
}

/// 消息类条目的测试语义身份；真实身份由协议完整投影计算
fn message_identity(role: CodexHistoryRole, text: &str) -> String {
    format!("{}:{text}", role.as_str())
}

fn item(declared: Option<&str>, role: CodexHistoryRole, text: &str) -> CodexHistoryCaptureItem {
    CodexHistoryCaptureItem {
        declared_item_id: declared.map(ToOwned::to_owned),
        role: Some(role),
        tool_namespace: None,
        tool_name: None,
        content_type: "message".to_owned(),
        identity: message_identity(role, text),
        raw: format!("{{\"type\":\"message\",\"role\":\"user\",\"content\":\"{text}\"}}"),
        text: Some(text.to_owned()),
    }
}

/// 同正文但附件不同的消息；语义身份必须包含图片本身
fn image_item(image: &str) -> CodexHistoryCaptureItem {
    CodexHistoryCaptureItem {
        declared_item_id: None,
        role: Some(CodexHistoryRole::User),
        tool_namespace: None,
        tool_name: None,
        content_type: "message".to_owned(),
        identity: format!("user:see the image:{image}"),
        raw: format!(
            "{{\"type\":\"message\",\"role\":\"user\",\"content\":[{{\"type\":\"input_text\",\"text\":\"see the image\"}},{{\"type\":\"input_image\",\"image_url\":\"data:image/png;base64,{image}\"}}]}}"
        ),
        text: Some("see the image".to_owned()),
    }
}

/// 不可读对象；语义身份必须包含原始密文
fn opaque_item(ciphertext: &str) -> CodexHistoryCaptureItem {
    CodexHistoryCaptureItem {
        declared_item_id: None,
        role: None,
        tool_namespace: None,
        tool_name: None,
        content_type: "reasoning".to_owned(),
        identity: format!("reasoning:{ciphertext}"),
        raw: format!("{{\"type\":\"reasoning\",\"encrypted_content\":\"{ciphertext}\"}}"),
        text: None,
    }
}

fn stored_item(id: &str, role: CodexHistoryRole, text: &str) -> CodexHistoryItem {
    CodexHistoryItem {
        item_id: id.to_owned(),
        role: Some(role),
        tool_namespace: None,
        tool_name: None,
        content_type: "message".to_owned(),
        identity: message_identity(role, text),
        raw: format!("{{\"type\":\"message\",\"id\":\"{id}\"}}"),
        text: Some(text.to_owned()),
    }
}

fn stored_opaque_item(id: &str, ciphertext: &str) -> CodexHistoryItem {
    CodexHistoryItem {
        item_id: id.to_owned(),
        role: None,
        tool_namespace: None,
        tool_name: None,
        content_type: "reasoning".to_owned(),
        identity: format!("reasoning:{ciphertext}"),
        raw: format!(
            "{{\"type\":\"reasoning\",\"id\":\"{id}\",\"encrypted_content\":\"{ciphertext}\"}}"
        ),
        text: None,
    }
}

#[derive(Default)]
struct MemoryStore {
    state: Mutex<Option<CodexSessionState>>,
    requests: Mutex<Vec<CodexHistoryRequest>>,
    responses: Mutex<Vec<CodexHistoryResponse>>,
    items: Mutex<Vec<CodexHistoryStoredItem>>,
    anchors: Mutex<Vec<CodexHistoryResponseAnchor>>,
    pending: Mutex<Vec<CodexHistoryPendingInput>>,
    fail_commits: AtomicBool,
}

impl MemoryStore {
    fn with_origin(origin: CodexSessionOrigin) -> Self {
        Self {
            state: Mutex::new(Some(state(origin))),
            ..Self::default()
        }
    }

    fn origin(&self) -> Option<CodexSessionOrigin> {
        self.state
            .lock()
            .expect("state lock")
            .as_ref()
            .map(|state| state.origin)
    }

    fn committed_responses(&self) -> Vec<CodexHistoryResponse> {
        self.responses.lock().expect("responses lock").clone()
    }

    fn committed_items(&self) -> Vec<CodexHistoryStoredItem> {
        self.items.lock().expect("items lock").clone()
    }

    /// 当前窗口的发送前逻辑输入快照
    fn pending_input(&self, window_value: &str) -> Option<CodexHistoryPendingInput> {
        self.pending
            .lock()
            .expect("pending lock")
            .iter()
            .find(|pending| pending.window.window_id == window_id(window_value))
            .cloned()
    }

    fn item_ids(&self) -> Vec<String> {
        self.committed_items()
            .into_iter()
            .map(|item| item.item.item_id)
            .collect()
    }

    fn fail_commits(&self) {
        self.fail_commits.store(true, Ordering::SeqCst);
    }

    /// 预置已持久条目，模拟同一会话此前的轮次
    fn seed_item(&self, window_value: &str, id: &str, role: CodexHistoryRole, text: &str) {
        let mut items = self.items.lock().expect("items lock");
        let ordinal = items.len() as u64;
        items.push(CodexHistoryStoredItem {
            window_id: window_id(window_value),
            ordinal,
            item: stored_item(id, role, text),
        });
    }

    fn push_items(&self, window: &CodexHistoryWindow, items: &[CodexHistoryItem]) {
        let mut stored = self.items.lock().expect("items lock");
        for item in items {
            let ordinal = stored.len() as u64;
            stored.push(CodexHistoryStoredItem {
                window_id: window.window_id.clone(),
                ordinal,
                item: item.clone(),
            });
        }
    }
}

impl CodexSessionContentPort for MemoryStore {
    fn commit_history_request<'a>(
        &'a self,
        record: &'a CodexHistoryRequest,
    ) -> BoxFuture<'a, Result<(), ProviderStoreError>> {
        Box::pin(async move {
            if self.fail_commits.load(Ordering::SeqCst) {
                return Err(unavailable("commit Codex history request"));
            }
            self.push_items(&record.window, &record.items);
            self.requests
                .lock()
                .expect("requests lock")
                .push(record.clone());
            Ok(())
        })
    }

    fn commit_history_response<'a>(
        &'a self,
        record: &'a CodexHistoryResponse,
    ) -> BoxFuture<'a, Result<(), ProviderStoreError>> {
        Box::pin(async move {
            self.push_items(&record.window, &record.items);
            let mut anchors = self.anchors.lock().expect("anchors lock");
            match anchors
                .iter_mut()
                .find(|anchor| anchor.response_id == record.response_id)
            {
                // 同一响应的后续批次只追加输出引用，不改写首次输入
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
            drop(anchors);
            self.responses
                .lock()
                .expect("responses lock")
                .push(record.clone());
            Ok(())
        })
    }

    fn load_history_response<'a>(
        &'a self,
        _key_id: &'a ClientApiKeyId,
        _session_key: &'a ProviderSessionAffinityKey,
        response_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<CodexHistoryResponseAnchor>, ProviderStoreError>> {
        let anchor = self
            .anchors
            .lock()
            .expect("anchors lock")
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
        // 锚定顺序即响应创建顺序；同窗口最后一支就是当前逻辑上下文
        let anchor = self
            .anchors
            .lock()
            .expect("anchors lock")
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
        let items = self.committed_items();
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
        let items = self.committed_items();
        Box::pin(async move { Ok(items) })
    }

    fn load_history_window_items<'a>(
        &'a self,
        _key_id: &'a ClientApiKeyId,
        _session_key: &'a ProviderSessionAffinityKey,
        window_id: &'a CodexNativeWindowId,
    ) -> BoxFuture<'a, Result<Vec<CodexHistoryStoredItem>, ProviderStoreError>> {
        let items = self
            .committed_items()
            .into_iter()
            .filter(|item| item.window_id == *window_id)
            .collect::<Vec<_>>();
        Box::pin(async move { Ok(items) })
    }

    fn save_history_pending_input<'a>(
        &'a self,
        _key_id: &'a ClientApiKeyId,
        _session_key: &'a ProviderSessionAffinityKey,
        pending: &'a CodexHistoryPendingInput,
    ) -> BoxFuture<'a, Result<(), ProviderStoreError>> {
        let mut slots = self.pending.lock().expect("pending lock");
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
            .pending
            .lock()
            .expect("pending lock")
            .iter()
            .find(|pending| pending.window.window_id == *window_id)
            .cloned();
        Box::pin(async move { Ok(pending) })
    }

    fn append_notes_operation<'a>(
        &'a self,
        _operation: &'a NewCodexNotesOperation,
    ) -> BoxFuture<'a, Result<CodexNotesOperation, ProviderStoreError>> {
        Box::pin(async { Err(unavailable("append Codex notes operation")) })
    }

    fn start_notes_operation<'a>(
        &'a self,
        _key_id: &'a ClientApiKeyId,
        _session_key: &'a ProviderSessionAffinityKey,
        _seq: u64,
    ) -> BoxFuture<'a, Result<CodexNotesOperation, ProviderStoreError>> {
        Box::pin(async { Err(unavailable("start Codex notes operation")) })
    }

    fn complete_notes_operation<'a>(
        &'a self,
        _completion: &'a CodexNotesCompletion,
    ) -> BoxFuture<'a, Result<CodexNotesOperation, ProviderStoreError>> {
        Box::pin(async { Err(unavailable("complete Codex notes operation")) })
    }

    fn load_notes_operations<'a>(
        &'a self,
        _key_id: &'a ClientApiKeyId,
        _session_key: &'a ProviderSessionAffinityKey,
    ) -> BoxFuture<'a, Result<Vec<CodexNotesOperation>, ProviderStoreError>> {
        Box::pin(async { Err(unavailable("load Codex notes operations")) })
    }
}

impl CodexSessionStatePort for MemoryStore {
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
        _replay: &'a CodexNotesReplay,
    ) -> BoxFuture<'a, Result<(), ProviderStoreError>> {
        Box::pin(async { Err(unavailable("save Codex notes replay")) })
    }

    fn publish_notes_replay<'a>(
        &'a self,
        _publish: &'a CodexNotesPublish,
    ) -> BoxFuture<'a, Result<CodexNotesPublishOutcome, ProviderStoreError>> {
        Box::pin(async { Err(unavailable("publish Codex notes replay")) })
    }
}

fn service(store: &std::sync::Arc<MemoryStore>) -> CodexHistoryService {
    CodexHistoryService::new(store.clone(), store.clone(), true)
}

fn store() -> std::sync::Arc<MemoryStore> {
    std::sync::Arc::new(MemoryStore::default())
}

fn captured(input: &[&str], appended: usize) -> CodexHistoryCapturedRequest {
    CodexHistoryCapturedRequest {
        input: input.iter().map(|value| (*value).to_owned()).collect(),
        appended,
    }
}

#[test]
fn fresh_new_session_input_is_captured_and_declared_managed() {
    let store = std::sync::Arc::new(MemoryStore::with_origin(CodexSessionOrigin::Unclassified));
    let service = service(&store);
    let facts = facts(
        "thread_new:0",
        None,
        Some("turn"),
        CodexHistoryInitialization::New,
    );
    let input = vec![
        item(Some("msg_1"), CodexHistoryRole::User, "hello"),
        item(Some("msg_2"), CodexHistoryRole::User, "again"),
    ];
    let outcome = futures::executor::block_on(service.capture_request(&facts, &input)).unwrap();
    assert_eq!(outcome, captured(&["msg_1", "msg_2"], 2));
    assert_eq!(store.origin(), Some(CodexSessionOrigin::Managed));
    assert_eq!(store.item_ids(), vec!["msg_1", "msg_2"]);
    let request = store.requests.lock().unwrap().first().cloned().unwrap();
    assert_eq!(request.previous_response_id, None);
    assert_eq!(request.window, window("thread_new:0"));
}

#[test]
fn begin_request_resolves_the_first_unclassified_session() {
    let store = std::sync::Arc::new(MemoryStore::with_origin(CodexSessionOrigin::Unclassified));
    let service = service(&store);
    let prewarm = facts(
        "thread_begin:0",
        None,
        Some("prewarm"),
        CodexHistoryInitialization::New,
    );
    // 来源判定在发送前的统一入口完成，首次请求即可成为 managed
    assert!(futures::executor::block_on(service.begin_request(&prewarm)).unwrap());
    assert_eq!(store.origin(), Some(CodexSessionOrigin::Managed));
    // 空输入预热不写条目，但仍走同一入口并建立零输出响应锚
    assert_eq!(
        futures::executor::block_on(service.capture_request(&prewarm, &[])).unwrap(),
        captured(&[], 0)
    );
    futures::executor::block_on(service.capture_response(&prewarm, "resp_warmup", &[], &[]))
        .unwrap();
    let anchor =
        futures::executor::block_on(store.load_history_response(&key(), &session(), "resp_warmup"))
            .unwrap()
            .expect("warmup anchor");
    assert_eq!(anchor.window, window("thread_begin:0"));
    assert!(anchor.input.is_empty());
    assert!(anchor.output.is_empty());
    assert!(store.committed_items().is_empty());
}

#[test]
fn request_store_failure_is_returned_before_anything_is_sent() {
    let store = std::sync::Arc::new(MemoryStore::with_origin(CodexSessionOrigin::Managed));
    store.fail_commits();
    let service = service(&store);
    let facts = facts(
        "thread_failure:0",
        None,
        Some("turn"),
        CodexHistoryInitialization::New,
    );
    assert!(matches!(
        futures::executor::block_on(
            service.capture_request(&facts, &[item(Some("msg_f"), CodexHistoryRole::User, "x")])
        ),
        Err(CodexHistoryCaptureError::Store(_))
    ));
    assert!(store.item_ids().is_empty());
}

#[test]
fn full_replay_of_a_stored_prefix_is_not_counted_twice() {
    let store = std::sync::Arc::new(MemoryStore::with_origin(CodexSessionOrigin::Managed));
    store.seed_item("thread_replay:0", "msg_a", CodexHistoryRole::User, "first");
    store.seed_item("thread_replay:0", "msg_b", CodexHistoryRole::User, "second");
    // 上一轮响应的逻辑输入就是当前上下文前缀；窗口旧档案不自动进入前缀
    futures::executor::block_on(store.commit_history_response(&CodexHistoryResponse {
        key_id: key(),
        session_key: session(),
        response_id: "resp_replay".to_owned(),
        window: window("thread_replay:0"),
        previous_response_id: None,
        input: vec!["msg_a".to_owned(), "msg_b".to_owned()],
        items: Vec::new(),
    }))
    .unwrap();
    let service = service(&store);
    let facts = facts(
        "thread_replay:0",
        None,
        Some("turn"),
        CodexHistoryInitialization::New,
    );
    let input = vec![
        item(Some("msg_a"), CodexHistoryRole::User, "first"),
        item(Some("msg_b"), CodexHistoryRole::User, "second"),
        item(Some("msg_c"), CodexHistoryRole::User, "third"),
    ];
    let outcome = futures::executor::block_on(service.capture_request(&facts, &input)).unwrap();
    assert_eq!(outcome, captured(&["msg_a", "msg_b", "msg_c"], 1));
    assert_eq!(store.item_ids(), vec!["msg_a", "msg_b", "msg_c"]);
}

/// 上游在建立响应锚之前失败：客户端全量重发必须复用发送前已保存的原条目
///
/// 额度拒绝等失败没有新响应，失败 attempt 的输入尚未进入任何响应锚；
/// 第二次无父响应请求只能靠发送前逻辑输入快照对齐，不能再按当前条目数归档一次
#[test]
fn failed_attempt_before_any_anchor_does_not_archive_the_input_twice() {
    let store = std::sync::Arc::new(MemoryStore::with_origin(CodexSessionOrigin::Managed));
    let service = service(&store);
    let facts = facts(
        "thread_retry:0",
        None,
        Some("turn"),
        CodexHistoryInitialization::New,
    );
    // 无 ID 输入无法按声明 ID 对齐，只能靠位置与语义身份
    let input = vec![CodexHistoryCaptureItem {
        declared_item_id: None,
        ..item(None, CodexHistoryRole::User, "hello")
    }];
    let first = futures::executor::block_on(service.capture_request(&facts, &input)).unwrap();
    assert_eq!(first, captured(&["cprs:thread_retry:0:0"], 1));
    let pending = store
        .pending_input("thread_retry:0")
        .expect("sending-side input snapshot");
    assert_eq!(pending.input, vec!["cprs:thread_retry:0:0".to_owned()]);
    assert_eq!(pending.previous_response_id, None);
    // 上游在 response.created 之前失败，没有新响应锚
    assert!(store.committed_responses().is_empty());

    let replay = futures::executor::block_on(service.capture_request(&facts, &input)).unwrap();
    assert_eq!(replay, captured(&["cprs:thread_retry:0:0"], 0));
    assert_eq!(store.item_ids(), vec!["cprs:thread_retry:0:0"]);
}

/// 父响应之后的增量在额度拒绝中丢失锚：全量重发时尾部沿用原引用
#[test]
fn failed_delta_after_a_parent_is_reused_by_the_full_replay() {
    let store = std::sync::Arc::new(MemoryStore::with_origin(CodexSessionOrigin::Managed));
    let service = service(&store);
    let root = facts(
        "thread_delta:0",
        None,
        Some("turn"),
        CodexHistoryInitialization::New,
    );
    let first = futures::executor::block_on(service.capture_request(
        &root,
        &[CodexHistoryCaptureItem {
            declared_item_id: None,
            ..item(None, CodexHistoryRole::User, "hello")
        }],
    ))
    .unwrap();
    futures::executor::block_on(service.capture_response(
        &root,
        "resp_a",
        &first.input,
        &[item(Some("msg_a"), CodexHistoryRole::Assistant, "answer")],
    ))
    .unwrap();

    // WS 增量只带本轮新输入；上游拒绝时没有 resp_b，尾部只在发送前快照里
    let delta = facts(
        "thread_delta:0",
        Some("resp_a"),
        Some("turn"),
        CodexHistoryInitialization::New,
    );
    let tail = futures::executor::block_on(service.capture_request(
        &delta,
        &[CodexHistoryCaptureItem {
            declared_item_id: None,
            ..item(None, CodexHistoryRole::User, "continue")
        }],
    ))
    .unwrap();
    assert_eq!(
        tail,
        captured(
            &["cprs:thread_delta:0:0", "msg_a", "cprs:thread_delta:0:2"],
            1
        )
    );
    assert_eq!(store.committed_responses().len(), 1, "no new anchor");

    // 客户端全量重发：整段上下文按位置对齐，尾部复用原引用且不新增条目
    let full = facts(
        "thread_delta:0",
        None,
        Some("turn"),
        CodexHistoryInitialization::New,
    );
    let replay = futures::executor::block_on(service.capture_request(
        &full,
        &[
            CodexHistoryCaptureItem {
                declared_item_id: None,
                ..item(None, CodexHistoryRole::User, "hello")
            },
            item(Some("msg_a"), CodexHistoryRole::Assistant, "answer"),
            CodexHistoryCaptureItem {
                declared_item_id: None,
                ..item(None, CodexHistoryRole::User, "continue")
            },
        ],
    ))
    .unwrap();
    assert_eq!(
        replay,
        captured(
            &["cprs:thread_delta:0:0", "msg_a", "cprs:thread_delta:0:2"],
            0
        )
    );
    assert_eq!(
        store.item_ids(),
        vec!["cprs:thread_delta:0:0", "msg_a", "cprs:thread_delta:0:2"]
    );
}

#[test]
fn input_without_ids_aligns_with_stored_output_ids() {
    let store = std::sync::Arc::new(MemoryStore::with_origin(CodexSessionOrigin::Managed));
    store.seed_item(
        "thread_omit:0",
        "msg_question",
        CodexHistoryRole::User,
        "question",
    );
    store.seed_item(
        "thread_omit:0",
        "msg_answer",
        CodexHistoryRole::Assistant,
        "answer",
    );
    futures::executor::block_on(store.commit_history_response(&CodexHistoryResponse {
        key_id: key(),
        session_key: session(),
        response_id: "resp_omit".to_owned(),
        window: window("thread_omit:0"),
        previous_response_id: None,
        input: vec!["msg_question".to_owned(), "msg_answer".to_owned()],
        items: Vec::new(),
    }))
    .unwrap();
    let service = service(&store);
    let facts = facts(
        "thread_omit:0",
        None,
        Some("turn"),
        CodexHistoryInitialization::New,
    );
    // 客户端回填历史时省略上游声明的输出 ID，仍按同位置语义对象对齐
    let input = vec![
        CodexHistoryCaptureItem {
            declared_item_id: None,
            ..item(None, CodexHistoryRole::User, "question")
        },
        CodexHistoryCaptureItem {
            declared_item_id: None,
            ..item(None, CodexHistoryRole::Assistant, "answer")
        },
        CodexHistoryCaptureItem {
            declared_item_id: None,
            ..item(None, CodexHistoryRole::User, "next")
        },
    ];
    let outcome = futures::executor::block_on(service.capture_request(&facts, &input)).unwrap();
    assert_eq!(
        outcome,
        captured(&["msg_question", "msg_answer", "cprs:thread_omit:0:2"], 1)
    );
    assert_eq!(
        store.item_ids(),
        vec!["msg_question", "msg_answer", "cprs:thread_omit:0:2"]
    );
}

#[test]
fn changed_content_at_the_same_position_is_appended_not_swallowed() {
    let store = std::sync::Arc::new(MemoryStore::with_origin(CodexSessionOrigin::Managed));
    store.seed_item(
        "thread_change:0",
        "cprs:thread_change:0:0",
        CodexHistoryRole::User,
        "old",
    );
    let service = service(&store);
    let facts = facts(
        "thread_change:0",
        None,
        Some("turn"),
        CodexHistoryInitialization::New,
    );
    let input = vec![CodexHistoryCaptureItem {
        declared_item_id: None,
        ..item(None, CodexHistoryRole::User, "new")
    }];
    let outcome = futures::executor::block_on(service.capture_request(&facts, &input)).unwrap();
    assert_eq!(outcome, captured(&["cprs:thread_change:0:1"], 1));
    let items = store.committed_items();
    assert_eq!(items.len(), 2);
    // 原条目保留，同位置变化作为新条目追加
    assert_eq!(items[0].item.text.as_deref(), Some("old"));
    assert_eq!(items[1].item.text.as_deref(), Some("new"));
}

#[test]
fn identical_text_at_different_positions_stays_two_items() {
    let managed = std::sync::Arc::new(MemoryStore::with_origin(CodexSessionOrigin::Managed));
    let service = service(&managed);
    let facts = facts(
        "thread_same_text:0",
        None,
        Some("turn"),
        CodexHistoryInitialization::New,
    );
    let input = vec![
        CodexHistoryCaptureItem {
            declared_item_id: None,
            ..item(None, CodexHistoryRole::User, "same text")
        },
        CodexHistoryCaptureItem {
            declared_item_id: None,
            ..item(None, CodexHistoryRole::User, "same text")
        },
    ];
    let outcome = futures::executor::block_on(service.capture_request(&facts, &input)).unwrap();
    assert_eq!(
        outcome,
        captured(
            &["cprs:thread_same_text:0:0", "cprs:thread_same_text:0:1"],
            2
        )
    );
    assert_eq!(
        managed.item_ids(),
        vec![
            "cprs:thread_same_text:0:0".to_owned(),
            "cprs:thread_same_text:0:1".to_owned()
        ]
    );
    // 上一轮响应锚定后，客户端整段重放同文输入按语义身份对齐，不会新增条目
    futures::executor::block_on(service.capture_response(
        &facts,
        "resp_same_text",
        &outcome.input,
        &[],
    ))
    .unwrap();
    let replay = futures::executor::block_on(service.capture_request(&facts, &input)).unwrap();
    assert_eq!(
        replay,
        captured(
            &["cprs:thread_same_text:0:0", "cprs:thread_same_text:0:1"],
            0
        )
    );
    assert_eq!(managed.item_ids().len(), 2);
}

#[test]
fn unknown_parent_response_requires_client_replay() {
    let store = std::sync::Arc::new(MemoryStore::with_origin(CodexSessionOrigin::Managed));
    let service = service(&store);
    let facts = facts(
        "thread_parent:0",
        Some("resp_missing"),
        Some("turn"),
        CodexHistoryInitialization::New,
    );
    let input = vec![item(Some("msg_delta"), CodexHistoryRole::User, "delta")];
    assert!(matches!(
        futures::executor::block_on(service.validate_parent(&facts)),
        Err(CodexHistoryCaptureError::ParentResponseUnknown)
    ));
    assert!(matches!(
        futures::executor::block_on(service.capture_request(&facts, &input)),
        Err(CodexHistoryCaptureError::ParentResponseUnknown)
    ));
    assert!(store.item_ids().is_empty());
}

#[test]
fn parent_response_from_another_window_is_rejected() {
    let store = std::sync::Arc::new(MemoryStore::with_origin(CodexSessionOrigin::Managed));
    futures::executor::block_on(store.commit_history_response(&CodexHistoryResponse {
        key_id: key(),
        session_key: session(),
        response_id: "resp_window".to_owned(),
        window: window("thread_switch:0"),
        previous_response_id: None,
        input: Vec::new(),
        items: Vec::new(),
    }))
    .unwrap();
    let service = service(&store);
    let facts = facts(
        "thread_switch:1",
        Some("resp_window"),
        Some("turn"),
        CodexHistoryInitialization::New,
    );
    assert!(matches!(
        futures::executor::block_on(service.validate_parent(&facts)),
        Err(CodexHistoryCaptureError::ParentWindowChanged)
    ));
}

#[test]
fn parent_response_restores_and_reloads_the_exact_logical_prefix() {
    let store = std::sync::Arc::new(MemoryStore::with_origin(CodexSessionOrigin::Managed));
    let service = service(&store);
    let root = facts(
        "thread_chain:0",
        None,
        Some("turn"),
        CodexHistoryInitialization::New,
    );
    let first = futures::executor::block_on(service.capture_request(
        &root,
        &[item(Some("msg_u"), CodexHistoryRole::User, "question")],
    ))
    .unwrap();
    assert_eq!(first, captured(&["msg_u"], 1));
    futures::executor::block_on(service.capture_response(
        &root,
        "resp_1",
        &first.input,
        &[item(Some("msg_a"), CodexHistoryRole::Assistant, "answer")],
    ))
    .unwrap();

    // WS 续接只发增量；输入引用由父响应的逻辑输入与产出精确衔接
    let second = facts(
        "thread_chain:0",
        Some("resp_1"),
        Some("turn"),
        CodexHistoryInitialization::New,
    );
    let captured_second = futures::executor::block_on(
        service.capture_request(&second, &[item(None, CodexHistoryRole::User, "follow up")]),
    )
    .unwrap();
    assert_eq!(
        captured_second,
        captured(&["msg_u", "msg_a", "cprs:thread_chain:0:2"], 1)
    );
    futures::executor::block_on(service.capture_response(
        &second,
        "resp_2",
        &captured_second.input,
        &[item(
            Some("msg_b"),
            CodexHistoryRole::Assistant,
            "second answer",
        )],
    ))
    .unwrap();

    // 普通重启后仅凭持久事实读回该响应的输入与输出引用
    let restarted = CodexHistoryService::new(store.clone(), store.clone(), true);
    let anchor =
        futures::executor::block_on(store.load_history_response(&key(), &session(), "resp_2"))
            .unwrap()
            .expect("child anchor");
    assert_eq!(anchor.previous_response_id.as_deref(), Some("resp_1"));
    assert_eq!(
        anchor.input,
        vec!["msg_u", "msg_a", "cprs:thread_chain:0:2"]
    );
    assert_eq!(anchor.output, vec!["msg_b"]);

    // 完整重发父链前缀时按引用对齐，不重复入库既有输出
    let third = facts(
        "thread_chain:0",
        Some("resp_2"),
        Some("turn"),
        CodexHistoryInitialization::New,
    );
    let replay = vec![
        item(Some("msg_u"), CodexHistoryRole::User, "question"),
        item(Some("msg_a"), CodexHistoryRole::Assistant, "answer"),
        CodexHistoryCaptureItem {
            declared_item_id: None,
            ..item(None, CodexHistoryRole::User, "follow up")
        },
        item(Some("msg_b"), CodexHistoryRole::Assistant, "second answer"),
        CodexHistoryCaptureItem {
            declared_item_id: None,
            ..item(None, CodexHistoryRole::User, "third")
        },
    ];
    let captured_third =
        futures::executor::block_on(restarted.capture_request(&third, &replay)).unwrap();
    assert_eq!(
        captured_third,
        captured(
            &[
                "msg_u",
                "msg_a",
                "cprs:thread_chain:0:2",
                "msg_b",
                "cprs:thread_chain:0:4"
            ],
            1
        )
    );
    assert_eq!(
        store.item_ids(),
        vec![
            "msg_u",
            "msg_a",
            "cprs:thread_chain:0:2",
            "msg_b",
            "cprs:thread_chain:0:4"
        ]
    );
}

#[test]
fn prewarm_of_a_fresh_session_captures_the_shared_input_once() {
    let store = std::sync::Arc::new(MemoryStore::with_origin(CodexSessionOrigin::Unclassified));
    let service = service(&store);
    let prewarm = facts(
        "thread_prewarm:0",
        None,
        Some("prewarm"),
        CodexHistoryInitialization::New,
    );
    let input = vec![item(Some("msg_p"), CodexHistoryRole::User, "warm")];
    let warm = futures::executor::block_on(service.capture_request(&prewarm, &input)).unwrap();
    assert_eq!(warm, captured(&["msg_p"], 1));
    assert_eq!(store.origin(), Some(CodexSessionOrigin::Managed));
    // 预热响应建立续链锚，正式生成只追加增量输入
    futures::executor::block_on(service.capture_response(
        &prewarm,
        "resp_warmup",
        &warm.input,
        &[],
    ))
    .unwrap();
    let turn = facts(
        "thread_prewarm:0",
        Some("resp_warmup"),
        Some("turn"),
        CodexHistoryInitialization::New,
    );
    assert_eq!(
        futures::executor::block_on(service.capture_request(
            &turn,
            &[item(Some("msg_t"), CodexHistoryRole::User, "next")]
        ))
        .unwrap(),
        captured(&["msg_p", "msg_t"], 1)
    );
    assert_eq!(store.item_ids(), vec!["msg_p", "msg_t"]);
}

#[test]
fn undeclared_initialization_stays_unclassified() {
    let store = std::sync::Arc::new(MemoryStore::with_origin(CodexSessionOrigin::Unclassified));
    let service = service(&store);
    for request_kind in ["turn", "prewarm"] {
        let undeclared = facts(
            "thread_undeclared:0",
            None,
            Some(request_kind),
            CodexHistoryInitialization::Undeclared,
        );
        assert_eq!(
            futures::executor::block_on(service.capture_request(
                &undeclared,
                &[item(Some("msg_u"), CodexHistoryRole::User, "turn")]
            ))
            .unwrap(),
            captured(&[], 0)
        );
        assert_eq!(store.origin(), Some(CodexSessionOrigin::Unclassified));
    }
    assert!(store.item_ids().is_empty());
}

#[test]
fn supplied_history_and_existing_windows_are_legacy() {
    for facts in [
        facts(
            "thread_legacy:0",
            None,
            Some("turn"),
            CodexHistoryInitialization::Supplied,
        ),
        facts(
            "thread_legacy:2",
            None,
            Some("turn"),
            CodexHistoryInitialization::Undeclared,
        ),
        facts(
            "thread_legacy:0",
            Some("resp_earlier"),
            Some("turn"),
            CodexHistoryInitialization::Undeclared,
        ),
    ] {
        let store = std::sync::Arc::new(MemoryStore::with_origin(CodexSessionOrigin::Unclassified));
        let service = service(&store);
        assert_eq!(
            futures::executor::block_on(service.capture_request(
                &facts,
                &[item(Some("msg_l"), CodexHistoryRole::User, "old")]
            ))
            .unwrap(),
            captured(&[], 0)
        );
        assert_eq!(store.origin(), Some(CodexSessionOrigin::Legacy));
        assert!(store.item_ids().is_empty());
    }
}

#[test]
fn legacy_session_is_never_written() {
    let store = std::sync::Arc::new(MemoryStore::with_origin(CodexSessionOrigin::Legacy));
    let service = service(&store);
    let facts = facts(
        "thread_old:0",
        None,
        Some("turn"),
        CodexHistoryInitialization::New,
    );
    assert!(!futures::executor::block_on(service.uses_local_history(&key(), &session())).unwrap());
    assert_eq!(
        futures::executor::block_on(service.capture_request(
            &facts,
            &[item(Some("msg_x"), CodexHistoryRole::User, "ignored")]
        ))
        .unwrap(),
        captured(&[], 0)
    );
    assert!(store.item_ids().is_empty());
}

#[test]
fn unclaimed_session_is_not_local_history() {
    let store = store();
    let service = service(&store);
    assert!(!futures::executor::block_on(service.uses_local_history(&key(), &session())).unwrap());
    let facts = facts(
        "thread_unclaimed:0",
        None,
        Some("turn"),
        CodexHistoryInitialization::New,
    );
    assert_eq!(
        futures::executor::block_on(service.capture_request(
            &facts,
            &[item(Some("msg_n"), CodexHistoryRole::User, "none")]
        ))
        .unwrap(),
        captured(&[], 0)
    );
    assert!(store.item_ids().is_empty());
}

#[test]
fn new_window_capture_keeps_the_previous_window_archive() {
    let store = std::sync::Arc::new(MemoryStore::with_origin(CodexSessionOrigin::Managed));
    store.seed_item(
        "thread_window:0",
        "msg_w0_a",
        CodexHistoryRole::User,
        "before compact",
    );
    store.seed_item(
        "thread_window:0",
        "msg_w0_b",
        CodexHistoryRole::Assistant,
        "old answer",
    );
    let service = service(&store);
    let facts = facts(
        "thread_window:1",
        None,
        Some("compaction"),
        CodexHistoryInitialization::New,
    );
    let input = vec![item(
        Some("msg_w1_a"),
        CodexHistoryRole::User,
        "after compact",
    )];
    let outcome = futures::executor::block_on(service.capture_request(&facts, &input)).unwrap();
    assert_eq!(outcome, captured(&["msg_w1_a"], 1));
    let items = store.committed_items();
    assert_eq!(
        items
            .iter()
            .filter(|item| item.window_id == window_id("thread_window:0"))
            .count(),
        2
    );
    let new_window = items
        .iter()
        .find(|item| item.window_id == window_id("thread_window:1"))
        .expect("new window item");
    assert_eq!(new_window.item.item_id, "msg_w1_a");
    assert_eq!(new_window.ordinal, 2, "窗口切换不重排既有条目");
}

#[test]
fn response_batches_commit_items_with_the_parent_chain() {
    let store = std::sync::Arc::new(MemoryStore::with_origin(CodexSessionOrigin::Managed));
    let service = service(&store);
    let facts = facts(
        "thread_response:0",
        Some("resp_parent"),
        Some("turn"),
        CodexHistoryInitialization::New,
    );
    futures::executor::block_on(async {
        // 先写入父锚，模拟上一轮已完整采集
        store
            .commit_history_response(&CodexHistoryResponse {
                key_id: key(),
                session_key: session(),
                response_id: "resp_parent".to_owned(),
                window: window("thread_response:0"),
                previous_response_id: None,
                input: Vec::new(),
                items: Vec::new(),
            })
            .await
            .unwrap();
        service
            .capture_response(
                &facts,
                "resp_child",
                &["msg_in".to_owned()],
                &[item(Some("msg_out"), CodexHistoryRole::Assistant, "answer")],
            )
            .await
            .unwrap();
    });
    let responses = store.committed_responses();
    let child = responses
        .iter()
        .find(|response| response.response_id == "resp_child")
        .expect("child response");
    assert_eq!(child.previous_response_id.as_deref(), Some("resp_parent"));
    assert_eq!(child.input, vec!["msg_in"]);
    assert_eq!(child.items.len(), 1);
    assert_eq!(child.items[0].item_id, "msg_out");
    let anchor =
        futures::executor::block_on(store.load_history_response(&key(), &session(), "resp_child"))
            .unwrap()
            .expect("anchor");
    assert_eq!(anchor.previous_response_id.as_deref(), Some("resp_parent"));
    assert_eq!(anchor.input, vec!["msg_in"]);
    assert_eq!(anchor.output, vec!["msg_out"]);
}

#[test]
fn later_batches_append_outputs_without_rewriting_the_anchor_input() {
    let store = std::sync::Arc::new(MemoryStore::with_origin(CodexSessionOrigin::Managed));
    let service = service(&store);
    let facts = facts(
        "thread_batch:0",
        None,
        Some("turn"),
        CodexHistoryInitialization::New,
    );
    futures::executor::block_on(async {
        service
            .capture_response(
                &facts,
                "resp_batch",
                &["msg_in".to_owned()],
                &[item(Some("msg_first"), CodexHistoryRole::Assistant, "one")],
            )
            .await
            .unwrap();
        // 后续批次仍沿同一响应锚定，只追加输出引用
        service
            .capture_response(
                &facts,
                "resp_batch",
                &["msg_ignored".to_owned()],
                &[item(Some("msg_second"), CodexHistoryRole::Assistant, "two")],
            )
            .await
            .unwrap();
    });
    let anchor =
        futures::executor::block_on(store.load_history_response(&key(), &session(), "resp_batch"))
            .unwrap()
            .expect("anchor");
    assert_eq!(anchor.input, vec!["msg_in"]);
    assert_eq!(anchor.output, vec!["msg_first", "msg_second"]);
}

#[test]
fn empty_delta_after_a_parent_keeps_the_parent_input_and_output() {
    let store = std::sync::Arc::new(MemoryStore::with_origin(CodexSessionOrigin::Managed));
    let service = service(&store);
    let root = facts(
        "thread_empty_delta:0",
        None,
        Some("turn"),
        CodexHistoryInitialization::New,
    );
    let first = futures::executor::block_on(service.capture_request(
        &root,
        &[item(Some("msg_u"), CodexHistoryRole::User, "question")],
    ))
    .unwrap();
    futures::executor::block_on(service.capture_response(
        &root,
        "resp_1",
        &first.input,
        &[item(Some("msg_a"), CodexHistoryRole::Assistant, "answer")],
    ))
    .unwrap();

    // 空 delta 续接没有新条目，但本次请求的上下文仍是父响应的输入与产出；
    // 空引用会让下一轮的逻辑前缀从零开始
    let empty = facts(
        "thread_empty_delta:0",
        Some("resp_1"),
        Some("turn"),
        CodexHistoryInitialization::New,
    );
    let kept = futures::executor::block_on(service.capture_request(&empty, &[])).unwrap();
    assert_eq!(kept, captured(&["msg_u", "msg_a"], 0));
    futures::executor::block_on(service.capture_response(&empty, "resp_2", &kept.input, &[]))
        .unwrap();

    // 该空响应的锚同样保存父前缀，下一轮增量的逻辑前缀不丢父级输入与产出
    let anchor =
        futures::executor::block_on(store.load_history_response(&key(), &session(), "resp_2"))
            .unwrap()
            .expect("empty delta anchor");
    assert_eq!(anchor.previous_response_id.as_deref(), Some("resp_1"));
    assert_eq!(anchor.input, vec!["msg_u", "msg_a"]);
    assert!(anchor.output.is_empty());
    let next = facts(
        "thread_empty_delta:0",
        Some("resp_2"),
        Some("turn"),
        CodexHistoryInitialization::New,
    );
    assert_eq!(
        futures::executor::block_on(service.capture_request(
            &next,
            &[item(None, CodexHistoryRole::User, "after empty answer")]
        ))
        .unwrap(),
        captured(&["msg_u", "msg_a", "cprs:thread_empty_delta:0:2"], 1)
    );
}

#[test]
fn empty_response_batch_records_an_anchor_for_zero_output_responses() {
    let store = std::sync::Arc::new(MemoryStore::with_origin(CodexSessionOrigin::Managed));
    let service = service(&store);
    let facts = facts(
        "thread_warmup:0",
        None,
        Some("prewarm"),
        CodexHistoryInitialization::New,
    );
    futures::executor::block_on(service.capture_response(&facts, "resp_warmup", &[], &[])).unwrap();
    let anchor =
        futures::executor::block_on(store.load_history_response(&key(), &session(), "resp_warmup"))
            .unwrap()
            .expect("warmup anchor");
    assert_eq!(anchor.window, window("thread_warmup:0"));
    assert!(store.committed_items().is_empty());
}

#[test]
fn opaque_items_with_different_ciphertext_are_not_the_same_object() {
    let store = std::sync::Arc::new(MemoryStore::with_origin(CodexSessionOrigin::Managed));
    store.push_items(
        &window("thread_opaque:0"),
        &[stored_opaque_item("cprs:thread_opaque:0:0", "cipher-old")],
    );
    futures::executor::block_on(store.commit_history_response(&CodexHistoryResponse {
        key_id: key(),
        session_key: session(),
        response_id: "resp_opaque".to_owned(),
        window: window("thread_opaque:0"),
        previous_response_id: None,
        input: vec!["cprs:thread_opaque:0:0".to_owned()],
        items: Vec::new(),
    }))
    .unwrap();
    let service = service(&store);
    let facts = facts(
        "thread_opaque:0",
        None,
        Some("turn"),
        CodexHistoryInitialization::New,
    );
    // 两份不可读对象的明文投影都是 None；不同密文不能被当成同一条
    let input = vec![opaque_item("cipher-new")];
    let outcome = futures::executor::block_on(service.capture_request(&facts, &input)).unwrap();
    assert_eq!(outcome, captured(&["cprs:thread_opaque:0:1"], 1));
    assert_eq!(
        store.item_ids(),
        vec!["cprs:thread_opaque:0:0", "cprs:thread_opaque:0:1"]
    );
    assert!(store.committed_items()[0].item.text.is_none());
}

#[test]
fn same_plaintext_with_a_different_image_is_not_the_same_item() {
    let store = std::sync::Arc::new(MemoryStore::with_origin(CodexSessionOrigin::Managed));
    store.push_items(
        &window("thread_image:0"),
        &[CodexHistoryItem {
            item_id: "cprs:thread_image:0:0".to_owned(),
            role: Some(CodexHistoryRole::User),
            tool_namespace: None,
            tool_name: None,
            content_type: "message".to_owned(),
            identity: "user:see the image:first-image".to_owned(),
            raw: r#"{"type":"message","role":"user","content":[{"type":"input_text","text":"see the image"},{"type":"input_image","image_url":"data:image/png;base64,first-image"}]}"#.to_owned(),
            text: Some("see the image".to_owned()),
        }],
    );
    futures::executor::block_on(store.commit_history_response(&CodexHistoryResponse {
        key_id: key(),
        session_key: session(),
        response_id: "resp_image".to_owned(),
        window: window("thread_image:0"),
        previous_response_id: None,
        input: vec!["cprs:thread_image:0:0".to_owned()],
        items: Vec::new(),
    }))
    .unwrap();
    let service = service(&store);
    let facts = facts(
        "thread_image:0",
        None,
        Some("turn"),
        CodexHistoryInitialization::New,
    );
    // 正文相同但附件不同：保留原图片对象，并把新图片作为另一条追加
    let outcome =
        futures::executor::block_on(service.capture_request(&facts, &[image_item("second-image")]))
            .unwrap();
    assert_eq!(outcome, captured(&["cprs:thread_image:0:1"], 1));
    let items = store.committed_items();
    assert_eq!(items.len(), 2);
    assert!(items[0].item.raw.contains("first-image"));
    assert!(items[1].item.raw.contains("second-image"));
}

#[test]
fn full_replay_uses_the_latest_response_snapshot_not_every_window_item() {
    let store = std::sync::Arc::new(MemoryStore::with_origin(CodexSessionOrigin::Managed));
    let service = service(&store);
    let window_facts = facts(
        "thread_latest:0",
        None,
        Some("turn"),
        CodexHistoryInitialization::New,
    );
    let first = futures::executor::block_on(service.capture_request(
        &window_facts,
        &[item(Some("msg_a"), CodexHistoryRole::User, "a")],
    ))
    .unwrap();
    futures::executor::block_on(service.capture_response(
        &window_facts,
        "resp_a",
        &first.input,
        &[item(Some("msg_b"), CodexHistoryRole::Assistant, "b")],
    ))
    .unwrap();

    // 同一窗口内换成另一段上下文；窗口旧档案不属于当前逻辑上下文
    let branch = futures::executor::block_on(service.capture_request(
        &window_facts,
        &[item(Some("msg_x"), CodexHistoryRole::User, "x")],
    ))
    .unwrap();
    // 换了一段上下文：本次输入就是它自己，旧窗口档案不冒充当前上下文
    assert_eq!(branch, captured(&["msg_x"], 1));
    futures::executor::block_on(service.capture_response(
        &window_facts,
        "resp_x",
        &branch.input,
        &[item(Some("msg_y"), CodexHistoryRole::Assistant, "y")],
    ))
    .unwrap();
    assert_eq!(store.item_ids(), vec!["msg_a", "msg_b", "msg_x", "msg_y"]);

    // 最新响应是 [x, y]；下一轮完整回填只与它对齐，不把旧档案再归档一次
    let outcome = futures::executor::block_on(service.capture_request(
        &window_facts,
        &[
            item(Some("msg_x"), CodexHistoryRole::User, "x"),
            item(Some("msg_y"), CodexHistoryRole::Assistant, "y"),
            item(None, CodexHistoryRole::User, "z"),
        ],
    ))
    .unwrap();
    assert_eq!(
        outcome,
        captured(&["msg_x", "msg_y", "cprs:thread_latest:0:4"], 1)
    );
    assert_eq!(
        store.item_ids(),
        vec!["msg_a", "msg_b", "msg_x", "msg_y", "cprs:thread_latest:0:4"]
    );
}

#[test]
fn changed_input_after_the_latest_response_is_appended_once() {
    let store = std::sync::Arc::new(MemoryStore::with_origin(CodexSessionOrigin::Managed));
    store.push_items(
        &window("thread_rebase:0"),
        &[
            stored_item("msg_a", CodexHistoryRole::User, "a"),
            stored_item("msg_b", CodexHistoryRole::Assistant, "b"),
        ],
    );
    futures::executor::block_on(store.commit_history_response(&CodexHistoryResponse {
        key_id: key(),
        session_key: session(),
        response_id: "resp_rebase".to_owned(),
        window: window("thread_rebase:0"),
        previous_response_id: None,
        input: vec!["msg_a".to_owned(), "msg_b".to_owned()],
        items: Vec::new(),
    }))
    .unwrap();
    let service = service(&store);
    let facts = facts(
        "thread_rebase:0",
        None,
        Some("turn"),
        CodexHistoryInitialization::New,
    );
    // 前两条对齐已保存前缀；改动后的第三条作为新条目追加，不改写原对象
    let outcome = futures::executor::block_on(service.capture_request(
        &facts,
        &[
            item(Some("msg_a"), CodexHistoryRole::User, "a"),
            item(Some("msg_b"), CodexHistoryRole::Assistant, "b"),
            item(None, CodexHistoryRole::User, "b-edited"),
        ],
    ))
    .unwrap();
    assert_eq!(
        outcome,
        captured(&["msg_a", "msg_b", "cprs:thread_rebase:0:2"], 1)
    );
    // 同一份改动再次回填时按最新语义身份对齐，不重复归档
    futures::executor::block_on(service.capture_response(
        &facts,
        "resp_rebase_2",
        &outcome.input,
        &[],
    ))
    .unwrap();
    let again = futures::executor::block_on(service.capture_request(
        &facts,
        &[
            item(Some("msg_a"), CodexHistoryRole::User, "a"),
            item(Some("msg_b"), CodexHistoryRole::Assistant, "b"),
            item(None, CodexHistoryRole::User, "b-edited"),
        ],
    ))
    .unwrap();
    assert_eq!(
        again,
        captured(&["msg_a", "msg_b", "cprs:thread_rebase:0:2"], 0)
    );
    assert_eq!(store.item_ids().len(), 3);
}

#[test]
fn disabled_admission_keeps_managed_sessions_and_fixes_new_ones_as_legacy() {
    let managed = std::sync::Arc::new(MemoryStore::with_origin(CodexSessionOrigin::Managed));
    let service = CodexHistoryService::new(managed.clone(), managed.clone(), false);
    let managed_facts = facts(
        "thread_disabled_managed:0",
        None,
        Some("turn"),
        CodexHistoryInitialization::New,
    );
    // 已有 managed 会话在关闭开关后继续本地采集
    let outcome = futures::executor::block_on(service.capture_request(
        &managed_facts,
        &[item(Some("msg_kept"), CodexHistoryRole::User, "kept")],
    ))
    .unwrap();
    assert_eq!(outcome, captured(&["msg_kept"], 1));
    assert_eq!(managed.origin(), Some(CodexSessionOrigin::Managed));

    // 新会话按 legacy 固定：不采集、不写条目，以后开启开关也不会冒充完整新建
    let fresh = std::sync::Arc::new(MemoryStore::with_origin(CodexSessionOrigin::Unclassified));
    let service = CodexHistoryService::new(fresh.clone(), fresh.clone(), false);
    let fresh_facts = facts(
        "thread_disabled_new:0",
        None,
        Some("turn"),
        CodexHistoryInitialization::New,
    );
    assert!(!futures::executor::block_on(service.begin_request(&fresh_facts)).unwrap());
    let outcome = futures::executor::block_on(service.capture_request(
        &fresh_facts,
        &[item(None, CodexHistoryRole::User, "ignored")],
    ))
    .unwrap();
    assert_eq!(outcome, captured(&[], 0));
    futures::executor::block_on(service.capture_response(&fresh_facts, "resp_ignored", &[], &[]))
        .unwrap();
    assert!(fresh.committed_items().is_empty());
    assert_eq!(fresh.origin(), Some(CodexSessionOrigin::Legacy));
}
