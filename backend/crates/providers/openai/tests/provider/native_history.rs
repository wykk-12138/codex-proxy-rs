//! 验证原生 history 的请求/响应采集、本地四工具回答与检索声明适配

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use bytes::Bytes;
use futures::{StreamExt as _, future::BoxFuture};
use gateway_core::account::ProviderAccountId;
use gateway_core::engine::codex::CodexAuxiliaryOperation;
use gateway_core::engine::codex_history::CodexHistoryService;
use gateway_core::engine::execution::ExecutionService as _;
use gateway_core::engine::provider::Provider as _;
use gateway_core::error::GatewayErrorKind;
use gateway_core::lifecycle::CancellationToken;
use gateway_core::operation::{GenerateRequest, Operation, ProtocolPayload};
use gateway_core::policy::{ClientApiKeyId, CodexAccessMode};
use gateway_core::provider_ports::{
    CodexHistoryItem, CodexHistoryPendingInput, CodexHistoryRequest, CodexHistoryResponse,
    CodexHistoryResponseAnchor, CodexHistoryRole, CodexHistoryStoredItem, CodexHistoryStoredWindow,
    CodexHistoryWindowMetadata, CodexNativeWindowId, CodexNotesCompletion, CodexNotesOperation,
    CodexNotesPublish, CodexNotesPublishOutcome, CodexNotesReplay, CodexNotesReplica,
    CodexSessionContentPort, CodexSessionOrigin, CodexSessionState, CodexSessionStatePort,
    NewCodexNotesOperation, ProviderSessionAffinityKey, ProviderStoreError, ProviderStoreErrorKind,
};
use gateway_core::upstream::UpstreamSendState;
use serde_json::{Map, Value, json};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

use super::contract::{
    create_account, planned_request, pool_context, provider_with_codex_sessions_and_native_history,
};
use super::native::seed;
use crate::support::{MemoryAccountStore, MemoryCodexSessions};

pub(super) const CAPTURED_SSE: &str = concat!(
    "event: response.created\n",
    "data: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_native_1\",\"model\":\"gpt-5.4\",\"status\":\"in_progress\",\"output\":[]}}\n\n",
    "event: response.output_item.done\n",
    "data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"type\":\"message\",\"id\":\"msg_answer\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"answer\"}]}}\n\n",
    "event: response.completed\n",
    "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_native_1\",\"model\":\"gpt-5.4\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"total_tokens\":2}}}\n\n"
);

fn unavailable(operation: &'static str) -> ProviderStoreError {
    ProviderStoreError::new(ProviderStoreErrorKind::Unavailable, operation)
}

fn account() -> ProviderAccountId {
    ProviderAccountId::new("acct_affinity_switch_a").expect("account id")
}

fn state(origin: CodexSessionOrigin) -> CodexSessionState {
    CodexSessionState {
        origin,
        owner: account(),
        generation: 0,
        notes: CodexNotesReplica::native(account()),
        notes_replay: None,
    }
}

/// 只回答来源与 history 读写的内存存储；Key 由同一个测试会话固定
#[derive(Default)]
struct MemoryNativeStore {
    state: Mutex<Option<CodexSessionState>>,
    items: Mutex<Vec<CodexHistoryStoredItem>>,
    pending: Mutex<Vec<CodexHistoryPendingInput>>,
    /// 内部窗口已持久保存的 context window UUID 别名
    window_aliases: Mutex<Vec<(String, String)>>,
    fail_commits: AtomicBool,
}

impl MemoryNativeStore {
    fn with_origin(origin: CodexSessionOrigin) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(Some(state(origin))),
            items: Mutex::new(Vec::new()),
            pending: Mutex::new(Vec::new()),
            window_aliases: Mutex::new(Vec::new()),
            fail_commits: AtomicBool::new(false),
        })
    }

    /// 模拟发送前落库失败；上游不得收到请求
    fn fail_commits(&self) {
        self.fail_commits.store(true, Ordering::SeqCst);
    }

    fn seed_item(&self, window_id: &str, item_id: &str, text: &str) {
        let mut items = self.items.lock().expect("items lock");
        let ordinal = items.len() as u64;
        items.push(CodexHistoryStoredItem {
            window_id: CodexNativeWindowId::try_new(window_id).expect("window id"),
            ordinal,
            item: CodexHistoryItem {
                item_id: item_id.to_owned(),
                role: Some(CodexHistoryRole::User),
                tool_namespace: None,
                tool_name: None,
                content_type: "message".to_owned(),
                raw: format!("{{\"type\":\"message\",\"id\":\"{item_id}\"}}"),
                text: Some(text.to_owned()),
                identity: item_id.to_owned(),
            },
        });
    }

    /// 登记某内部窗口已持久保存的 context window UUID
    ///
    /// 官方客户端用 UUID 传窗口，查询必须解析到同一份档案
    fn seed_window_alias(&self, window_id: &str, context_window_id: &str) {
        self.window_aliases
            .lock()
            .expect("window aliases lock")
            .push((window_id.to_owned(), context_window_id.to_owned()));
    }

    fn item_ids(&self) -> Vec<String> {
        self.items
            .lock()
            .expect("items lock")
            .iter()
            .map(|item| item.item.item_id.clone())
            .collect()
    }

    fn origin(&self) -> Option<CodexSessionOrigin> {
        self.state
            .lock()
            .expect("state lock")
            .as_ref()
            .map(|state| state.origin)
    }
}

impl CodexSessionStatePort for MemoryNativeStore {
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

impl CodexSessionContentPort for MemoryNativeStore {
    fn commit_history_request<'a>(
        &'a self,
        record: &'a CodexHistoryRequest,
    ) -> BoxFuture<'a, Result<(), ProviderStoreError>> {
        Box::pin(async move {
            if self.fail_commits.load(Ordering::SeqCst) {
                return Err(unavailable("commit Codex history request"));
            }
            let mut items = self.items.lock().expect("items lock");
            for item in &record.items {
                let ordinal = items.len() as u64;
                items.push(CodexHistoryStoredItem {
                    window_id: record.window.window_id.clone(),
                    ordinal,
                    item: item.clone(),
                });
            }
            Ok(())
        })
    }

    fn commit_history_response<'a>(
        &'a self,
        record: &'a CodexHistoryResponse,
    ) -> BoxFuture<'a, Result<(), ProviderStoreError>> {
        Box::pin(async move {
            let mut items = self.items.lock().expect("items lock");
            for item in &record.items {
                let ordinal = items.len() as u64;
                items.push(CodexHistoryStoredItem {
                    window_id: record.window.window_id.clone(),
                    ordinal,
                    item: item.clone(),
                });
            }
            Ok(())
        })
    }

    fn load_history_response<'a>(
        &'a self,
        _key_id: &'a ClientApiKeyId,
        _session_key: &'a ProviderSessionAffinityKey,
        _response_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<CodexHistoryResponseAnchor>, ProviderStoreError>> {
        Box::pin(async { Ok(None) })
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
        let items = self.items.lock().expect("items lock").clone();
        let aliases = self
            .window_aliases
            .lock()
            .expect("window aliases lock")
            .clone();
        Box::pin(async move {
            let mut windows: Vec<CodexHistoryStoredWindow> = Vec::new();
            for item in items {
                match windows
                    .iter_mut()
                    .find(|window| window.window_id == item.window_id)
                {
                    Some(window) => window.item_count += 1,
                    None => {
                        let mut metadata = vec![CodexHistoryWindowMetadata {
                            agent_name: Some("/root".to_owned()),
                            context_window_id: None,
                        }];
                        metadata.extend(
                            aliases
                                .iter()
                                .filter(|(window_id, _)| {
                                    *window_id == item.window_id.expose_to_store()
                                })
                                .map(|(_, context_window_id)| CodexHistoryWindowMetadata {
                                    agent_name: Some("/root".to_owned()),
                                    context_window_id: Some(context_window_id.clone()),
                                }),
                        );
                        windows.push(CodexHistoryStoredWindow {
                            window_id: item.window_id.clone(),
                            item_count: 1,
                            metadata,
                        });
                    }
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
        let items = self.items.lock().expect("items lock").clone();
        Box::pin(async move { Ok(items) })
    }

    fn load_history_window_items<'a>(
        &'a self,
        _key_id: &'a ClientApiKeyId,
        _session_key: &'a ProviderSessionAffinityKey,
        window_id: &'a CodexNativeWindowId,
    ) -> BoxFuture<'a, Result<Vec<CodexHistoryStoredItem>, ProviderStoreError>> {
        let items = self
            .items
            .lock()
            .expect("items lock")
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
        self.pending
            .lock()
            .expect("pending lock")
            .push(pending.clone());
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
            .rev()
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

fn service(store: &Arc<MemoryNativeStore>) -> CodexHistoryService {
    CodexHistoryService::new(store.clone(), store.clone(), true)
}

/// 池模式模型请求：窗口与初始化来源来自官方 turn metadata
pub(super) fn pool_turn_request(
    session: &str,
    window_id: &str,
    history_initialization: &str,
) -> Operation {
    pool_turn_request_with_input(
        session,
        window_id,
        history_initialization,
        json!([{
            "type": "message",
            "id": "msg_user",
            "role": "user",
            "content": [{"type": "input_text", "text": "hello"}]
        }]),
    )
}

/// 同上的模型请求，但输入条目由用例给出
///
/// 用于验证无 ID 输入、或客户端自己重建的完整上下文
pub(super) fn pool_turn_request_with_input(
    session: &str,
    window_id: &str,
    history_initialization: &str,
    input: Value,
) -> Operation {
    Operation::Generate(GenerateRequest::from_protocol_payload(
        ProtocolPayload::json_object(
            "openai",
            json!({
                "model": "gpt-5.4",
                "input": input,
                "client_metadata": {
                    "session_id": session,
                    "x-codex-turn-metadata": json!({
                        "window_id": window_id,
                        "history_initialization": history_initialization,
                        "request_kind": "turn",
                        "agent_name": "/root",
                    })
                    .to_string(),
                },
                "tools": [
                    {
                        "type": "namespace",
                        "name": "history",
                        "tools": [{
                            "type": "function",
                            "name": "search_contents",
                            "parameters": {
                                "type": "object",
                                "properties": {"query": {"type": "string", "encrypted": true}}
                            },
                            "required": ["query"]
                        }]
                    },
                    {
                        "type": "namespace",
                        "name": "notes",
                        "tools": [{
                            "type": "function",
                            "name": "search_contents",
                            "parameters": {
                                "type": "object",
                                "properties": {"query": {"type": "string", "encrypted": true}}
                            },
                            "required": ["query"]
                        }]
                    }
                ]
            })
            .as_object()
            .expect("request body")
            .clone(),
        )
        .expect("protocol payload")
        .with_context(Map::from_iter([("use_websocket".to_owned(), json!(false))])),
    ))
}

fn sent_body(request: &wiremock::Request) -> Value {
    let decoded = zstd::stream::decode_all(request.body.as_slice()).expect("zstd request body");
    serde_json::from_slice(&decoded).expect("request body json")
}

async fn read_body(response: gateway_core::engine::middleware::MiddlewareResponse) -> Value {
    let (_, status, headers, mut body, _) = response.into_parts();
    assert_eq!(status, 200);
    assert!(
        headers
            .iter()
            .any(|header| header.name().eq_ignore_ascii_case("content-type")
                && header.value().as_ref() == b"application/json")
    );
    let mut bytes = Vec::new();
    loop {
        let Some(frame) = body.next_frame().await.expect("history body frame") else {
            break;
        };
        bytes.extend_from_slice(frame.bytes());
    }
    body.close().await;
    serde_json::from_slice(&bytes).expect("history result json")
}

#[tokio::test]
async fn managed_session_captures_items_and_plaintext_history_search() {
    let store = Arc::new(MemoryAccountStore::default());
    create_account(&store, "acct_affinity_switch_a").await;
    let sessions = Arc::new(MemoryCodexSessions::default());
    let native = MemoryNativeStore::with_origin(CodexSessionOrigin::Managed);
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/codex/responses"))
        .respond_with(ResponseTemplate::new(200).set_body_string(CAPTURED_SSE))
        .mount(&server)
        .await;
    let provider = provider_with_codex_sessions_and_native_history(
        &store,
        sessions,
        server.uri(),
        Arc::new(service(&native)),
    );
    let mut stream = provider
        .clone()
        .execute(
            planned_request(
                "openai",
                pool_turn_request("session-native-capture", "thread-native:0", "new"),
            ),
            pool_context("req_native_capture", CancellationToken::new()),
        )
        .await
        .expect("managed capture request");
    while let Some(event) = stream.next().await {
        event.expect("upstream stream");
    }
    // 请求输入与响应产出都按官方条目保存
    let ids = native.item_ids();
    assert_eq!(ids, vec!["msg_user".to_owned(), "msg_answer".to_owned()]);
    assert_eq!(native.origin(), Some(CodexSessionOrigin::Managed));

    // 只有 history 检索查询变成明文，notes 仍由上游加密
    let requests = server.received_requests().await.expect("received requests");
    assert_eq!(requests.len(), 1);
    let body = sent_body(&requests[0]);
    assert!(
        body["tools"][0]["tools"][0]["parameters"]["properties"]["query"]
            .get("encrypted")
            .is_none()
    );
    assert_eq!(
        body["tools"][1]["tools"][0]["parameters"]["properties"]["query"]["encrypted"],
        json!(true)
    );
}

#[tokio::test]
async fn local_history_answers_without_credentials_or_upstream() {
    let native = MemoryNativeStore::with_origin(CodexSessionOrigin::Managed);
    native.seed_item("thread-local:0", "msg_history", "recover me");
    let store = Arc::new(MemoryAccountStore::default());
    let sessions = Arc::new(MemoryCodexSessions::default());
    // 无可用账号与上游地址：本地回答不能依赖凭据、whoami 或网络
    let provider = provider_with_codex_sessions_and_native_history(
        &store,
        sessions,
        "http://127.0.0.1:9".to_owned(),
        Arc::new(service(&native)),
    );
    let provider: Arc<dyn gateway_core::engine::provider::Provider> = provider;
    let (service, _admissions) =
        super::native::native_service_with_mode(provider, CodexAccessMode::SessionPool);
    let prepared = super::native::prepare(&service).await;
    let response = service
        .codex_auxiliary(
            prepared,
            gateway_core::engine::codex::CodexAuxiliaryRequest {
                operation: CodexAuxiliaryOperation::HistoryListWindows,
                query: None,
                headers: Vec::new(),
                body: Bytes::from_static(
                    br#"{"client_metadata":{"session_id":"session-native-local"},"context":{"current_agent_name":"/root"}}"#,
                ),
            },
        )
        .await
        .expect("local history answer");
    let body = read_body(response).await;
    assert_eq!(body["agent_name"], json!("/root"));
    assert_eq!(body["window_count"], json!(1));
    assert_eq!(body["windows"][0]["window_id"], json!("thread-local:0"));
    assert_eq!(body["windows"][0]["item_count"], json!(1));
}

/// 官方客户端用 context window UUID 传窗口；本地 history 必须解析到同一份档案
#[tokio::test]
async fn local_history_resolves_persisted_context_window_uuids() {
    const CONTEXT_WINDOW_ID: &str = "0193f0b7-1d2a-7c31-9b0e-2a3f5c6d7e80";
    const UNKNOWN_WINDOW_ID: &str = "0193f0b7-1d2a-7c31-9b0e-2a3f5c6d7eff";
    let native = MemoryNativeStore::with_origin(CodexSessionOrigin::Managed);
    native.seed_item("thread-alias:0", "msg_history", "recover me");
    native.seed_window_alias("thread-alias:0", CONTEXT_WINDOW_ID);
    let store = Arc::new(MemoryAccountStore::default());
    let sessions = Arc::new(MemoryCodexSessions::default());
    let provider = provider_with_codex_sessions_and_native_history(
        &store,
        sessions,
        "http://127.0.0.1:9".to_owned(),
        Arc::new(service(&native)),
    );
    let provider: Arc<dyn gateway_core::engine::provider::Provider> = provider;
    let (service, _admissions) =
        super::native::native_service_with_mode(provider, CodexAccessMode::SessionPool);

    let call = |operation, body: String| {
        let service = &service;
        async move {
            service
                .codex_auxiliary(
                    super::native::prepare(service).await,
                    gateway_core::engine::codex::CodexAuxiliaryRequest {
                        operation,
                        query: None,
                        headers: Vec::new(),
                        body: Bytes::from(body),
                    },
                )
                .await
        }
    };

    // UUID 与内部 ID 得到同一份条目列表；不再被当成非法参数
    let listed = call(
        CodexAuxiliaryOperation::HistoryListItems,
        format!(
            r#"{{"client_metadata":{{"session_id":"session-native-alias"}},"context":{{"current_agent_name":"/root"}},"window_id":"{CONTEXT_WINDOW_ID}"}}"#
        ),
    )
    .await
    .expect("uuid window list");
    let body = read_body(listed).await;
    assert_eq!(body["window_id"], json!("thread-alias:0"));
    assert_eq!(body["item_count"], json!(1));
    assert_eq!(body["items"][0]["item_id"], json!("msg_history"));

    // read_item 同样接受 UUID，并回显规范的内部窗口身份
    let read = call(
        CodexAuxiliaryOperation::HistoryReadItem,
        format!(
            r#"{{"client_metadata":{{"session_id":"session-native-alias"}},"context":{{"current_agent_name":"/root"}},"window_id":"{CONTEXT_WINDOW_ID}","item_id":"msg_history"}}"#
        ),
    )
    .await
    .expect("uuid window read");
    let body = read_body(read).await;
    assert_eq!(body["window_id"], json!("thread-alias:0"));
    assert_eq!(body["content"], json!("recover me"));

    // 窗口命中但条目不存在时保持官方 item_not_found 结果
    let missing = call(
        CodexAuxiliaryOperation::HistoryReadItem,
        format!(
            r#"{{"client_metadata":{{"session_id":"session-native-alias"}},"context":{{"current_agent_name":"/root"}},"window_id":"{CONTEXT_WINDOW_ID}","item_id":"msg_absent"}}"#
        ),
    )
    .await
    .expect("uuid window read miss");
    let body = read_body(missing).await;
    assert_eq!(body["error"], json!("item_not_found"));
    assert_eq!(body["window_id"], json!(CONTEXT_WINDOW_ID));
    assert_eq!(body["window_found"], json!(true));
    assert_eq!(body["item_found"], json!(false));

    // 未知 UUID 与其它不透明形状都只是无匹配，不扩大到全部窗口，也不是参数错误
    for unknown in [UNKNOWN_WINDOW_ID, "thread-alias", "thread-alias:9"] {
        let response = call(
            CodexAuxiliaryOperation::HistoryListItems,
            format!(
                r#"{{"client_metadata":{{"session_id":"session-native-alias"}},"context":{{"current_agent_name":"/root"}},"window_id":"{unknown}"}}"#
            ),
        )
        .await
        .expect("unknown window is not an argument error");
        let body = read_body(response).await;
        assert_eq!(body["item_count"], json!(0), "unknown window `{unknown}`");
        assert_eq!(body["window_id"], json!(unknown));
    }

    // 真实参数错误仍然报错
    let error = call(
        CodexAuxiliaryOperation::HistoryListItems,
        format!(
            r#"{{"client_metadata":{{"session_id":"session-native-alias"}},"context":{{"current_agent_name":"/root"}},"window_id":"{CONTEXT_WINDOW_ID}","limit":0}}"#
        ),
    )
    .await
    .expect_err("zero limit is still invalid");
    assert_eq!(error.kind(), GatewayErrorKind::InvalidRequest);
}

#[tokio::test]
async fn legacy_session_keeps_upstream_history() {
    let native = MemoryNativeStore::with_origin(CodexSessionOrigin::Legacy);
    native.seed_item("thread-legacy:0", "msg_legacy", "old");
    let store = Arc::new(MemoryAccountStore::default());
    // 本地辅助请求固定使用入口账号；legacy 会话仍走该账号的真实上游
    seed(&store, "acct_native_bound", true, false).await;
    let sessions = Arc::new(MemoryCodexSessions::default());
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/codex/alpha/history/v2/list_windows"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"windows": []})))
        .expect(1)
        .mount(&server)
        .await;
    let provider = provider_with_codex_sessions_and_native_history(
        &store,
        sessions,
        server.uri(),
        Arc::new(service(&native)),
    );
    let provider: Arc<dyn gateway_core::engine::provider::Provider> = provider;
    let (service, _admissions) =
        super::native::native_service_with_mode(provider, CodexAccessMode::SessionPool);
    let prepared = super::native::prepare(&service).await;
    let response = service
        .codex_auxiliary(
            prepared,
            gateway_core::engine::codex::CodexAuxiliaryRequest {
                operation: CodexAuxiliaryOperation::HistoryListWindows,
                query: None,
                headers: Vec::new(),
                body: Bytes::from_static(
                    br#"{"client_metadata":{"session_id":"session-native-legacy"},"context":{"current_agent_name":"/root"}}"#,
                ),
            },
        )
        .await
        .expect("legacy history stays upstream");
    let body = read_body(response).await;
    assert_eq!(body, json!({"windows": []}));
}

#[tokio::test]
async fn unclassified_session_becomes_managed_on_the_first_request() {
    let store = Arc::new(MemoryAccountStore::default());
    create_account(&store, "acct_affinity_switch_a").await;
    let sessions = Arc::new(MemoryCodexSessions::default());
    let native = MemoryNativeStore::with_origin(CodexSessionOrigin::Unclassified);
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/codex/responses"))
        .respond_with(ResponseTemplate::new(200).set_body_string(CAPTURED_SSE))
        .mount(&server)
        .await;
    let provider = provider_with_codex_sessions_and_native_history(
        &store,
        sessions,
        server.uri(),
        Arc::new(service(&native)),
    );
    let mut stream = provider
        .clone()
        .execute(
            planned_request(
                "openai",
                pool_turn_request("session-native-first", "thread-native-first:0", "new"),
            ),
            pool_context("req_native_first", CancellationToken::new()),
        )
        .await
        .expect("first unclassified request");
    while let Some(event) = stream.next().await {
        event.expect("upstream stream");
    }
    // 首次请求在发送前完成来源判定，输入与输出都按 managed 采集
    assert_eq!(native.origin(), Some(CodexSessionOrigin::Managed));
    assert_eq!(
        native.item_ids(),
        vec!["msg_user".to_owned(), "msg_answer".to_owned()]
    );

    // 同一首轮出站已按 managed 改写 history 检索声明；notes 仍由上游加密
    let requests = server.received_requests().await.expect("received requests");
    assert_eq!(requests.len(), 1);
    let body = sent_body(&requests[0]);
    assert!(
        body["tools"][0]["tools"][0]["parameters"]["properties"]["query"]
            .get("encrypted")
            .is_none()
    );
    assert_eq!(
        body["tools"][1]["tools"][0]["parameters"]["properties"]["query"]["encrypted"],
        json!(true)
    );
}

#[tokio::test]
async fn request_capture_failure_stops_before_the_upstream_send() {
    let store = Arc::new(MemoryAccountStore::default());
    create_account(&store, "acct_affinity_switch_a").await;
    let sessions = Arc::new(MemoryCodexSessions::default());
    let native = MemoryNativeStore::with_origin(CodexSessionOrigin::Managed);
    native.fail_commits();
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/codex/responses"))
        .respond_with(ResponseTemplate::new(200).set_body_string(CAPTURED_SSE))
        .mount(&server)
        .await;
    let provider = provider_with_codex_sessions_and_native_history(
        &store,
        sessions,
        server.uri(),
        Arc::new(service(&native)),
    );
    let mut stream = provider
        .clone()
        .execute(
            planned_request(
                "openai",
                pool_turn_request("session-native-failure", "thread-native-failure:0", "new"),
            ),
            pool_context("req_native_failure", CancellationToken::new()),
        )
        .await
        .expect("stream handle");
    let error = loop {
        match stream.next().await {
            Some(Err(error)) => break error,
            Some(Ok(_)) => continue,
            None => panic!("stream ended without a capture error"),
        }
    };
    assert_eq!(error.send_state(), UpstreamSendState::NotSent);
    assert!(native.item_ids().is_empty());
    assert!(
        server
            .received_requests()
            .await
            .expect("received requests")
            .is_empty(),
        "request must not reach upstream when input persistence fails"
    );
}
