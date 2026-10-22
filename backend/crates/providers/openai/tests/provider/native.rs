//! 原生辅助能力的真实授权、字节透传、当前凭据出口与流取消合同

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use futures::future::BoxFuture;
use gateway_core::{
    account::{
        CredentialState, OutboundProxy, ProviderAccountId, ProviderAccountStore as _,
        QuotaAccessChange, QuotaEvidence, QuotaState,
    },
    engine::{
        AttemptRecord, ExecutionStore, IntermediateFailure, ModelRequestFinalization,
        ModelRequestId, NewModelRequest, RecoveryReport,
        admission::{
            ClientAdmissionDecision, ClientAdmissionError, ClientAdmissionPort,
            ClientAdmissionRecovery, ClientAdmissionRequest, ClientAdmissionRestoreResult,
        },
        authentication::{ClientAuthenticationPurpose, ClientAuthenticationRequest},
        codex::{CodexAuxiliaryOperation, CodexAuxiliaryRequest},
        continuation::{
            NativeContinuationPin, NativeContinuationPort, NativeContinuationStoreError,
            PreviousResponseId,
        },
        execution::{
            ClientApiKeyUsageSink, DefaultExecutionService, ExecutionService, PreparedRootExecution,
        },
        middleware::{MiddlewareHeader, MiddlewareResponse},
        provider::{Provider, ProviderRegistry},
    },
    error::StoreError,
    policy::{
        ClientApiKeyId, ClientPolicy, CodexAccessBinding, CodexAccessMode, PlaintextClientApiKey,
        RateLimits,
    },
    routing::{
        ClientRoutingScope, ConfigRevision, FrozenAccountScope, ProviderKind, RuntimeAccount,
        RuntimeAccountDirectory, RuntimeSnapshot,
    },
    runtime::RuntimeSnapshotHandle,
    settings::SettingsValues,
    upstream::UpstreamSendState,
};
use provider_openai::credential::{ImportCodexOAuthCredential, ResponsesTransport};
use serde_json::json;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, SystemTime},
};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

use crate::support::{MemoryAccountStore, MemoryCodexSessions, profile, secret};

const ACCOUNT: &str = "acct_native_bound";

pub(crate) fn native_service(
    provider: Arc<dyn Provider>,
) -> (DefaultExecutionService, Arc<NativeAdmissions>) {
    native_service_with_mode(provider, CodexAccessMode::Fixed)
}

pub(crate) fn native_service_with_mode(
    provider: Arc<dyn Provider>,
    mode: CodexAccessMode,
) -> (DefaultExecutionService, Arc<NativeAdmissions>) {
    let provider_kind = ProviderKind::new("openai").unwrap();
    let account_id = ProviderAccountId::new(ACCOUNT).unwrap();
    let directory = Arc::new(RuntimeAccountDirectory::new(BTreeMap::from([
        (
            account_id.clone(),
            RuntimeAccount::new(provider_kind.clone(), BTreeSet::new()),
        ),
        (
            ProviderAccountId::new("acct_native_other").unwrap(),
            RuntimeAccount::new(provider_kind.clone(), BTreeSet::new()),
        ),
    ])));
    let policy = ClientPolicy::new(
        ClientApiKeyId::new("key_native_test").unwrap(),
        PlaintextClientApiKey::new("sk_native_test").unwrap(),
        Arc::new(FrozenAccountScope::new(
            directory.clone(),
            ClientRoutingScope::all_accounts(),
        )),
        true,
        RateLimits {
            max_concurrency: 1,
            requests_per_minute: 100,
        },
    )
    .with_codex_access(Some(CodexAccessBinding {
        enabled: true,
        mode,
        account_id,
    }));
    let snapshot = RuntimeSnapshot::new(
        ConfigRevision::new(1).unwrap(),
        SettingsValues::new(2, "smart", Default::default(), None, None)
            .with_codex_public_origin(Some("https://native.example.test".to_owned())),
        vec![provider_kind],
        vec![],
        vec![policy],
    )
    .unwrap()
    .with_account_directory(directory);
    let admissions = Arc::new(NativeAdmissions::default());
    (
        DefaultExecutionService::new(
            RuntimeSnapshotHandle::new(snapshot),
            Arc::new(NoModelExecution),
            ProviderRegistry::new([provider]).unwrap(),
            admissions.clone(),
            Arc::new(NoModelExecution),
            Arc::new(NoModelExecution),
            Arc::new(crate::RecordingDiagnostics::default()),
        ),
        admissions,
    )
}

pub(crate) async fn prepare(service: &DefaultExecutionService) -> PreparedRootExecution {
    let client = service
        .authenticate_request(
            ClientAuthenticationRequest::bearer("sk_native_test")
                .unwrap()
                .with_purpose(ClientAuthenticationPurpose::Codex),
        )
        .await
        .unwrap();
    service.prepare_execution(client).await.unwrap()
}

async fn seed(store: &Arc<MemoryAccountStore>, id: &str, enabled: bool, fedramp: bool) {
    seed_workspace(store, id, enabled, fedramp, "workspace_native").await;
}

async fn seed_workspace(
    store: &Arc<MemoryAccountStore>,
    id: &str,
    enabled: bool,
    fedramp: bool,
    workspace: &str,
) {
    let jwt = format!(
        "header.{}.signature",
        URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&json!({
                "https://api.openai.com/auth": {"chatgpt_account_is_fedramp": fedramp},
            }))
            .unwrap()
        )
    );
    let mut account = profile(workspace);
    account.chatgpt_user_id = "user_native".to_owned();
    account.plan_type = Some("plus".to_owned());
    store
        .seed_oauth_credential(ImportCodexOAuthCredential {
            account_id: id.to_owned(),
            name: id.to_owned(),
            secret: secret(&jwt),
            verified_account: account,
            next_refresh_at: None,
            enabled,
        })
        .await;
}

fn request(operation: CodexAuxiliaryOperation) -> CodexAuxiliaryRequest {
    CodexAuxiliaryRequest {
        operation, query: Some("cursor=a%2Fb&same=1&same=2&space=+".to_owned()),
        headers: vec![
            MiddlewareHeader::new("x-openai-encrypted-tool-arguments", Bytes::from_static(b"true")),
            MiddlewareHeader::new("x-openai-tool-output-truncation-policy", Bytes::from_static(br#"{"type":"bytes","limit":1000}"#)),
            MiddlewareHeader::new("thread-id", Bytes::from_static(b"thread-context")),
            MiddlewareHeader::new("x-extension", Bytes::from_static(b"first")),
            MiddlewareHeader::new("x-extension", Bytes::from_static(b"\x80second")),
            MiddlewareHeader::new("authorization", Bytes::from_static(b"Bearer downstream-secret")),
            MiddlewareHeader::new("cookie", Bytes::from_static(b"downstream=cookie")),
            MiddlewareHeader::new("chatgpt-account-id", Bytes::from_static(b"wrong-workspace")),
            MiddlewareHeader::new("x-openai-fedramp", Bytes::from_static(b"false")),
        ],
        body: Bytes::from_static(br#"{ "context":{"session_id":"session-native","current_agent_name":"child"},"opaque":"encrypted","duplicate":1,"duplicate":2 }"#),
    }
}

async fn read_response(response: MiddlewareResponse) -> (u16, Vec<MiddlewareHeader>, Vec<u8>) {
    let (_, status, headers, mut body, _) = response.into_parts();
    let mut bytes = Vec::new();
    while let Some(frame) = body.next_frame().await.unwrap() {
        bytes.extend_from_slice(frame.bytes());
    }
    (status, headers, bytes)
}

#[tokio::test]
async fn finite_operations_preserve_wire_and_errors_without_replay() {
    use CodexAuxiliaryOperation::*;
    let server = MockServer::start().await;
    let store = Arc::new(MemoryAccountStore::default());
    seed(&store, ACCOUNT, true, true).await;
    seed(&store, "acct_native_other", true, false).await;
    let provider =
        super::contract::provider_with_base_url(&store, format!("{}/backend-api", server.uri()));
    let (service, admissions) = native_service(provider);
    let operations = [
        (
            HistoryListWindows,
            "POST",
            "/codex/alpha/history/v2/list_windows",
        ),
        (
            HistoryListItems,
            "POST",
            "/codex/alpha/history/v2/list_items",
        ),
        (HistoryReadItem, "POST", "/codex/alpha/history/v2/read_item"),
        (
            HistorySearchContents,
            "POST",
            "/codex/alpha/history/v2/search_contents",
        ),
        (NotesThreadHint, "POST", "/codex/alpha/notes/v2/thread_hint"),
        (
            NotesListFilesByPrefix,
            "POST",
            "/codex/alpha/notes/v2/list_files_by_prefix",
        ),
        (NotesReadFile, "POST", "/codex/alpha/notes/v2/read_file"),
        (
            NotesSearchContents,
            "POST",
            "/codex/alpha/notes/v2/search_contents",
        ),
        (
            NotesAppendToFile,
            "POST",
            "/codex/alpha/notes/v2/append_to_file",
        ),
        (NotesWriteFile, "POST", "/codex/alpha/notes/v2/write_file"),
        (Usage, "GET", "/wham/usage"),
        (Profile, "GET", "/wham/profiles/me"),
        (UserSettings, "GET", "/wham/settings/user"),
        (WorkspaceMessages, "GET", "/wham/workspace-messages"),
        (ConfigBundle, "GET", "/wham/config/bundle"),
    ];
    for (index, (operation, verb, suffix)) in operations.into_iter().enumerate() {
        let status = if matches!(operation, NotesAppendToFile | NotesWriteFile) {
            503
        } else {
            [200, 400, 404, 503][index % 4]
        };
        let upstream_body = b"opaque\x00\xff\r\nerror-or-success".to_vec();
        Mock::given(method(verb))
            .and(path(format!("/backend-api{suffix}")))
            .respond_with(
                ResponseTemplate::new(status)
                    .set_body_bytes(upstream_body.clone())
                    .insert_header("content-type", "application/octet-stream")
                    .append_header("x-upstream", "first")
                    .append_header("x-upstream", "second")
                    .insert_header("set-cookie", "upstream=private")
                    .insert_header("chatgpt-account-id", "upstream-private"),
            )
            .expect(1)
            .mount(&server)
            .await;
        let response = service
            .codex_auxiliary(prepare(&service).await, request(operation))
            .await
            .unwrap();
        assert!(admissions.0.load(Ordering::SeqCst));
        let (actual_status, headers, body) = read_response(response).await;
        assert_eq!((actual_status, body), (status, upstream_body));
        assert_eq!(
            headers
                .iter()
                .filter(|h| h.name() == "x-upstream")
                .map(|h| h.value())
                .collect::<Vec<_>>(),
            [b"first".as_slice(), b"second".as_slice()]
        );
        assert!(
            !headers
                .iter()
                .any(|h| matches!(h.name(), "set-cookie" | "chatgpt-account-id"))
        );
        assert!(!admissions.0.load(Ordering::SeqCst));
        let calls = server.received_requests().await.unwrap();
        let call = calls.last().unwrap();
        assert_eq!(call.url.query(), request(operation).query.as_deref());
        assert_eq!(call.body.as_slice(), request(operation).body.as_ref());
        assert_eq!(call.headers["chatgpt-account-id"], "workspace_native");
        assert_eq!(call.headers["x-openai-fedramp"], "true");
        assert_eq!(call.headers["thread-id"], "thread-context");
        assert_eq!(call.headers["x-openai-encrypted-tool-arguments"], "true");
        assert_eq!(
            call.headers["x-openai-tool-output-truncation-policy"],
            br#"{"type":"bytes","limit":1000}"#.as_slice()
        );
        assert_eq!(
            call.headers
                .get_all("x-extension")
                .iter()
                .map(|h| h.as_bytes())
                .collect::<Vec<_>>(),
            [b"first".as_slice(), b"\x80second".as_slice()]
        );
        assert!(call.headers.get("cookie").is_none());
        assert_ne!(call.headers["authorization"], "Bearer downstream-secret");
    }
    assert_eq!(server.received_requests().await.unwrap().len(), 15);
}

#[tokio::test]
async fn identity_uses_real_plus_plan_and_fedramp_claim_or_official_default() {
    for fedramp in [false, true] {
        let store = Arc::new(MemoryAccountStore::default());
        seed(&store, ACCOUNT, true, fedramp).await;
        let provider =
            super::contract::provider_with_base_url(&store, "http://127.0.0.1:9".to_owned());
        let (service, _) = native_service(provider);
        let identity = service
            .codex_identity(prepare(&service).await)
            .await
            .unwrap();
        assert_eq!(identity.user_id, "user_native");
        assert_eq!(identity.account_id, "workspace_native");
        assert_eq!(identity.plan_type.as_deref(), Some("plus"));
        assert_eq!(identity.is_fedramp, fedramp);
        let account = store.account(ACCOUNT).unwrap();
        store
            .repository()
            .rotate_refreshed_oauth_secret(&account, secret("opaque-current-access"), None, None)
            .await
            .unwrap();
        let refreshed = service
            .codex_identity(prepare(&service).await)
            .await
            .unwrap();
        assert!(!refreshed.is_fedramp);
        assert_eq!(refreshed.user_id, identity.user_id);
        assert_eq!(refreshed.plan_type, identity.plan_type);
    }
}

#[tokio::test]
async fn native_access_rejects_disabled_expired_invalid_and_api_key_accounts_without_fallback() {
    let server = MockServer::start().await;
    for kind in ["disabled", "expired", "invalid", "api_key"] {
        let store = Arc::new(MemoryAccountStore::default());
        seed(&store, "acct_native_other", true, false).await;
        if kind == "api_key" {
            store
                .seed_api_key(ACCOUNT, server.uri(), ResponsesTransport::Http)
                .await;
        } else {
            seed(&store, ACCOUNT, kind != "disabled", false).await;
            let account = store.account(ACCOUNT).unwrap();
            if kind == "expired" {
                store
                    .repository()
                    .rotate_refreshed_oauth_secret(
                        &account,
                        secret("expired-access"),
                        Some(SystemTime::now() - Duration::from_secs(1)),
                        None,
                    )
                    .await
                    .unwrap();
            } else if kind == "invalid" {
                store
                    .repository()
                    .apply_state(&account, CredentialState::Invalid, SystemTime::now())
                    .await
                    .unwrap();
            }
        }
        let provider = super::contract::provider_with_base_url(&store, server.uri());
        let (service, _) = native_service(provider);
        assert!(
            service
                .codex_identity(prepare(&service).await)
                .await
                .is_err(),
            "{kind}"
        );
        assert!(
            service
                .codex_auxiliary(
                    prepare(&service).await,
                    request(CodexAuxiliaryOperation::Usage)
                )
                .await
                .is_err(),
            "{kind}"
        );
    }
    assert!(server.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn auxiliary_uses_current_refresh_token_result_and_current_proxy_without_model_quota_gate() {
    let server = MockServer::start().await;
    let proxy = MockServer::start().await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string("direct"))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .respond_with(ResponseTemplate::new(200).set_body_string("proxy"))
        .expect(1)
        .mount(&proxy)
        .await;
    let store = Arc::new(MemoryAccountStore::default());
    seed(&store, ACCOUNT, true, false).await;
    let provider = super::contract::provider_with_base_url(&store, server.uri());
    let (service, _) = native_service(provider);
    let account = store.account(ACCOUNT).unwrap();
    store
        .apply_quota_access(QuotaAccessChange {
            account_id: account.id().clone(),
            expected_revision: account.revision(),
            state: QuotaState::exhausted(QuotaEvidence::UsageLimitReached, SystemTime::now(), None),
        })
        .await
        .unwrap();
    let first = service
        .codex_auxiliary(
            prepare(&service).await,
            request(CodexAuxiliaryOperation::Usage),
        )
        .await
        .unwrap();
    assert_eq!(read_response(first).await.2, b"direct");
    let account = store.account(ACCOUNT).unwrap();
    store
        .repository()
        .rotate_refreshed_oauth_secret(&account, secret("rotated-native-access"), None, None)
        .await
        .unwrap();
    store.set_egress(
        ACCOUNT,
        Some(OutboundProxy::parse(&proxy.uri()).unwrap()),
        None,
    );
    let second = service
        .codex_auxiliary(
            prepare(&service).await,
            request(CodexAuxiliaryOperation::Usage),
        )
        .await
        .unwrap();
    assert_eq!(read_response(second).await.2, b"proxy");
    let calls = proxy.received_requests().await.unwrap();
    assert_eq!(
        calls[0].headers["authorization"],
        "Bearer rotated-native-access"
    );
    store.set_egress(
        ACCOUNT,
        Some(OutboundProxy::parse("http://127.0.0.1:1").unwrap()),
        None,
    );
    assert!(
        service
            .codex_auxiliary(
                prepare(&service).await,
                request(CodexAuxiliaryOperation::NotesWriteFile)
            )
            .await
            .is_err()
    );
    assert_eq!(server.received_requests().await.unwrap().len(), 1);
}

#[tokio::test]
async fn lazy_body_completion_releases_connection_and_admission() {
    use tokio::{
        io::{AsyncReadExt as _, AsyncWriteExt as _},
        net::TcpListener,
    };
    for finish in ["cancel", "timeout", "close", "drop"] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let upstream = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let _ = read_http_body(&mut socket).await;
            socket
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\nfirst")
                .await
                .unwrap();
            let mut byte = [0];
            assert_eq!(socket.read(&mut byte).await.unwrap(), 0);
        });
        let store = Arc::new(MemoryAccountStore::default());
        seed(&store, ACCOUNT, true, false).await;
        let provider = super::contract::provider_with_base_url(&store, format!("http://{address}"));
        let (service, admissions) = native_service(provider);
        let mut prepared = prepare(&service).await;
        if finish == "timeout" {
            let settings = prepared.request_settings();
            let mut values = settings.execution_values().unwrap();
            values.timeout_ms = Some(1000);
            prepared
                .apply_settings(&settings.replace_execution(&values, "native-test").unwrap())
                .unwrap();
        }
        let cancellation = prepared.cancellation();
        let response = tokio::time::timeout(
            Duration::from_secs(2),
            service.codex_auxiliary(prepared, request(CodexAuxiliaryOperation::HistoryReadItem)),
        )
        .await
        .unwrap()
        .unwrap();
        let (_, _, _, mut body, _) = response.into_parts();
        assert_eq!(
            body.next_frame().await.unwrap().unwrap().bytes().as_ref(),
            b"first"
        );
        match finish {
            "cancel" => {
                cancellation.cancel();
                assert!(body.next_frame().await.is_err());
                drop(body);
            }
            "timeout" => {
                assert!(
                    tokio::time::timeout(Duration::from_secs(2), body.next_frame())
                        .await
                        .unwrap()
                        .is_err()
                );
                drop(body);
            }
            "close" => body.close().await,
            "drop" => drop(body),
            _ => unreachable!(),
        }
        assert!(!admissions.0.load(Ordering::SeqCst));
        tokio::time::timeout(Duration::from_secs(2), upstream)
            .await
            .unwrap()
            .unwrap();
    }
}

async fn read_http_body(socket: &mut tokio::net::TcpStream) -> Vec<u8> {
    use tokio::io::AsyncReadExt as _;
    let mut headers = Vec::new();
    loop {
        let mut byte = [0];
        socket.read_exact(&mut byte).await.unwrap();
        headers.push(byte[0]);
        if headers.ends_with(b"\r\n\r\n") {
            break;
        }
    }
    let text = String::from_utf8_lossy(&headers);
    let length = text
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|value| value.trim().parse::<usize>().unwrap())
        })
        .unwrap_or(0);
    let mut body = vec![0; length];
    socket.read_exact(&mut body).await.unwrap();
    body
}

#[tokio::test]
async fn notes_with_unknown_write_result_are_never_replayed() {
    use std::sync::atomic::AtomicUsize;
    for operation in [
        CodexAuxiliaryOperation::NotesAppendToFile,
        CodexAuxiliaryOperation::NotesWriteFile,
    ] {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let (stop, mut stopped) = tokio::sync::oneshot::channel::<()>();
        let upstream = tokio::spawn(async move {
            loop {
                let (mut socket, _) = tokio::select! {
                    accepted = listener.accept() => accepted.unwrap(),
                    _ = &mut stopped => break,
                };
                assert_eq!(
                    read_http_body(&mut socket).await,
                    request(operation).body.as_ref()
                );
                observed.fetch_add(1, Ordering::SeqCst);
                // 已消费完整写入正文但没有响应，模拟结果不明的传输断开
                drop(socket);
            }
        });
        let store = Arc::new(MemoryAccountStore::default());
        seed(&store, ACCOUNT, true, false).await;
        let (service, admissions) = native_service(super::contract::provider_with_base_url(
            &store,
            format!("http://{address}"),
        ));
        let result = tokio::time::timeout(
            Duration::from_secs(2),
            service.codex_auxiliary(prepare(&service).await, request(operation)),
        )
        .await
        .unwrap();
        assert!(result.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(!admissions.0.load(Ordering::SeqCst));
        stop.send(()).unwrap();
        upstream.await.unwrap();
    }
}

#[derive(Default)]
pub(crate) struct NativeAdmissions(AtomicBool);
impl ClientAdmissionPort for NativeAdmissions {
    fn abandon(&self, _: &ClientApiKeyId, _: &ModelRequestId) {
        self.0.store(false, Ordering::SeqCst);
    }
    fn admit(
        &self,
        _: ClientAdmissionRequest,
    ) -> BoxFuture<'_, Result<ClientAdmissionDecision, ClientAdmissionError>> {
        Box::pin(async {
            assert!(!self.0.swap(true, Ordering::SeqCst));
            Ok(ClientAdmissionDecision::Granted)
        })
    }
    fn release<'a>(
        &'a self,
        _: &'a ClientApiKeyId,
        _: &'a ModelRequestId,
    ) -> BoxFuture<'a, Result<bool, ClientAdmissionError>> {
        Box::pin(async { Ok(self.0.swap(false, Ordering::SeqCst)) })
    }
    fn restore(
        &self,
        _: ClientAdmissionRecovery,
    ) -> BoxFuture<'_, Result<ClientAdmissionRestoreResult, ClientAdmissionError>> {
        Box::pin(async { Ok(ClientAdmissionRestoreResult::default()) })
    }
}

struct NoModelExecution;
impl ClientApiKeyUsageSink for NoModelExecution {
    fn record_used(&self, _: &ClientApiKeyId) {}
}
impl NativeContinuationPort for NoModelExecution {
    fn resolve<'a>(
        &'a self,
        _: &'a ClientApiKeyId,
        _: &'a PreviousResponseId,
    ) -> BoxFuture<'a, Result<Option<NativeContinuationPin>, NativeContinuationStoreError>> {
        panic!("native auxiliary must not resolve model continuation")
    }
    fn record<'a>(
        &'a self,
        _: NativeContinuationPin,
    ) -> BoxFuture<'a, Result<(), NativeContinuationStoreError>> {
        panic!("native auxiliary must not record model continuation")
    }
}
#[async_trait]
impl ExecutionStore for NoModelExecution {
    async fn create_model_request(&self, _: NewModelRequest) -> Result<(), StoreError> {
        panic!("auxiliary must not create model usage")
    }
    async fn record_attempt(&self, _: AttemptRecord) -> Result<(), StoreError> {
        panic!("auxiliary must not create model attempts")
    }
    async fn mark_send_state(
        &self,
        _: &ModelRequestId,
        _: UpstreamSendState,
    ) -> Result<(), StoreError> {
        unreachable!()
    }
    async fn mark_downstream_committed(
        &self,
        _: &ModelRequestId,
        _: SystemTime,
        _: Option<u16>,
    ) -> Result<(), StoreError> {
        unreachable!()
    }
    async fn record_client_status(&self, _: &ModelRequestId, _: u16) -> Result<(), StoreError> {
        unreachable!()
    }
    async fn record_intermediate_failure(&self, _: IntermediateFailure) -> Result<(), StoreError> {
        unreachable!()
    }
    async fn finalize_model_request(&self, _: ModelRequestFinalization) -> Result<(), StoreError> {
        unreachable!()
    }
    async fn recover_expired(&self, _: SystemTime) -> Result<RecoveryReport, StoreError> {
        unreachable!()
    }
}

#[tokio::test]
async fn cancelling_before_headers_closes_the_pending_auxiliary_request() {
    use tokio::io::AsyncReadExt as _;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (sent, received) = tokio::sync::oneshot::channel();
    let upstream = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let _ = read_http_body(&mut socket).await;
        sent.send(()).unwrap();
        let mut byte = [0];
        assert_eq!(socket.read(&mut byte).await.unwrap(), 0);
    });
    let store = Arc::new(MemoryAccountStore::default());
    seed(&store, ACCOUNT, true, false).await;
    let (service, admissions) = native_service(super::contract::provider_with_base_url(
        &store,
        format!("http://{address}"),
    ));
    let prepared = prepare(&service).await;
    let cancellation = prepared.cancellation();
    let call = tokio::spawn(async move {
        service
            .codex_auxiliary(
                prepared,
                request(CodexAuxiliaryOperation::NotesAppendToFile),
            )
            .await
    });
    tokio::time::timeout(Duration::from_secs(2), received)
        .await
        .unwrap()
        .unwrap();
    cancellation.cancel();
    assert!(
        tokio::time::timeout(Duration::from_secs(2), call)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    assert!(!admissions.0.load(Ordering::SeqCst));
    tokio::time::timeout(Duration::from_secs(2), upstream)
        .await
        .unwrap()
        .unwrap();
}

/// 辅助请求与模型请求共用同一派生：用观测哈希取得池会话键
fn auxiliary_session_key(
    provider: &provider_openai::CodexProvider,
    session: &str,
) -> gateway_core::provider_ports::ProviderSessionAffinityKey {
    use gateway_core::operation::{GenerateRequest, Operation, ProtocolPayload};
    let operation = Operation::Generate(GenerateRequest::from_protocol_payload(
        ProtocolPayload::json_object(
            "openai",
            json!({"model":"gpt-5.4","input":"hello","session_id":session})
                .as_object()
                .unwrap()
                .clone(),
        )
        .unwrap(),
    ));
    let observation =
        provider.request_observation(&operation, &ClientApiKeyId::new("key_native_test").unwrap());
    gateway_core::provider_ports::ProviderSessionAffinityKey::try_new(
        observation
            .continuation
            .affinity_hash
            .expect("session affinity hash"),
    )
    .unwrap()
}

#[tokio::test]
async fn pool_session_auxiliary_claims_then_reuses_owner() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/backend-api/codex/alpha/notes/v2/read_file"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"notes".to_vec()))
        .mount(&server)
        .await;
    let store = Arc::new(MemoryAccountStore::default());
    seed(&store, ACCOUNT, true, false).await;
    let sessions = Arc::new(MemoryCodexSessions::default());
    let provider = super::contract::provider_with_base_url_and_codex_sessions(
        &store,
        sessions.clone(),
        format!("{}/backend-api", server.uri()),
    );
    let (service, _) = native_service_with_mode(provider, CodexAccessMode::SessionPool);
    for _ in 0..2 {
        let response = service
            .codex_auxiliary(
                prepare(&service).await,
                request(CodexAuxiliaryOperation::NotesReadFile),
            )
            .await
            .unwrap();
        let (status, _, body) = read_response(response).await;
        assert_eq!((status, body.as_slice()), (200, b"notes".as_slice()));
    }
    let claims = sessions.claims();
    assert_eq!(claims.len(), 1, "同会话第二次辅助请求只复用归属");
    assert_eq!(claims[0].account_id.as_str(), ACCOUNT);
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 2);
    for request in requests {
        assert_eq!(request.headers["chatgpt-account-id"], "workspace_native");
    }
}

#[tokio::test]
async fn pool_account_level_auxiliary_keeps_entry_account_without_claim() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/backend-api/wham/config/bundle"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"bundle".to_vec()))
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/backend-api/codex/alpha/notes/v2/read_file"))
        .respond_with(ResponseTemplate::new(200).set_body_bytes(b"notes".to_vec()))
        .mount(&server)
        .await;
    let store = Arc::new(MemoryAccountStore::default());
    seed(&store, ACCOUNT, true, false).await;
    seed_workspace(
        &store,
        "acct_native_other",
        true,
        false,
        "workspace_native_other",
    )
    .await;
    let sessions = Arc::new(MemoryCodexSessions::default());
    let provider = super::contract::provider_with_base_url_and_codex_sessions(
        &store,
        sessions.clone(),
        format!("{}/backend-api", server.uri()),
    );
    // 该会话已归属另一个账号；账号级辅助仍描述入口账号，也不改写归属
    sessions.seed_owner(
        &ClientApiKeyId::new("key_native_test").unwrap(),
        &auxiliary_session_key(&provider, "session-native"),
        ProviderAccountId::new("acct_native_other").unwrap(),
    );
    let (service, _) = native_service_with_mode(provider, CodexAccessMode::SessionPool);
    let response = service
        .codex_auxiliary(
            prepare(&service).await,
            request(CodexAuxiliaryOperation::ConfigBundle),
        )
        .await
        .unwrap();
    let (status, _, body) = read_response(response).await;
    assert_eq!((status, body.as_slice()), (200, b"bundle".as_slice()));
    assert!(sessions.claims().is_empty());
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0].headers["chatgpt-account-id"],
        "workspace_native"
    );

    // 同一 Key、同一会话的 history/notes 走归属账号，与上面的账号级辅助形成对照
    let response = service
        .codex_auxiliary(
            prepare(&service).await,
            request(CodexAuxiliaryOperation::NotesReadFile),
        )
        .await
        .unwrap();
    let (status, _, body) = read_response(response).await;
    assert_eq!((status, body.as_slice()), (200, b"notes".as_slice()));
    assert!(
        sessions.claims().is_empty(),
        "既有归属不需要为辅助请求重新认领"
    );
    let requests = server.received_requests().await.unwrap();
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[1].headers["chatgpt-account-id"],
        "workspace_native_other"
    );
}

#[tokio::test]
async fn pool_auxiliary_without_session_identity_fails_before_send() {
    let server = MockServer::start().await;
    let store = Arc::new(MemoryAccountStore::default());
    seed(&store, ACCOUNT, true, false).await;
    let sessions = Arc::new(MemoryCodexSessions::default());
    let provider = super::contract::provider_with_base_url_and_codex_sessions(
        &store,
        sessions.clone(),
        server.uri(),
    );
    let (service, _) = native_service_with_mode(provider, CodexAccessMode::SessionPool);
    let mut request = request(CodexAuxiliaryOperation::NotesReadFile);
    request.body = Bytes::from_static(br#"{"opaque":"encrypted"}"#);
    assert!(
        service
            .codex_auxiliary(prepare(&service).await, request)
            .await
            .is_err()
    );
    assert!(server.received_requests().await.unwrap().is_empty());
    assert!(sessions.claims().is_empty());
}
