//! 原生路由的真实 Core 授权、身份投影、有限操作与透明交付测试

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};

use async_trait::async_trait;
use axum::{
    body::{Body, to_bytes},
    http::{Method, Request, StatusCode},
};
use bytes::Bytes;
use futures::future::BoxFuture;
use gateway_core::{
    account::ProviderAccountId,
    engine::{
        AttemptContext,
        admission::{
            ClientAdmissionDecision, ClientAdmissionError, ClientAdmissionPort,
            ClientAdmissionRecovery, ClientAdmissionRequest, ClientAdmissionRestoreResult,
        },
        codex::{
            CodexAuthorizedContext, CodexAuxiliaryOperation, CodexAuxiliaryRequest, CodexGateway,
            CodexIdentity,
        },
        execution::{DefaultExecutionService, ExecutionService},
        middleware::{
            MiddlewareBody, MiddlewareError, MiddlewareFrame, MiddlewareFraming, MiddlewareHeader,
            MiddlewareResponse,
        },
        provider::{Provider, ProviderRegistry, ProviderRequest, ProviderStream},
    },
    error::ProviderError,
    operation::OperationKind,
    policy::{
        ClientApiKeyId, ClientPolicy, CodexAccessBinding, CodexAccessMode, PlaintextClientApiKey,
        RateLimits,
    },
    routing::{
        ClientRoutingScope, ConfigRevision, FrozenAccountScope, ModelCapabilities,
        ProviderCatalogGeneration, ProviderKind, ProviderModel, ProviderModelCapabilities,
        ProviderModelContent, ProviderModelDescriptor, RuntimeAccount, RuntimeAccountDirectory,
        RuntimeSnapshot, UpstreamModelId,
    },
    runtime::RuntimeSnapshotHandle,
};
use serde_json::{Value, json};
use tower::ServiceExt;

const KEY: &str = "sk_native_test";

#[derive(Default)]
struct Admissions {
    active: Arc<AtomicUsize>,
}
impl ClientAdmissionPort for Admissions {
    fn admit(
        &self,
        _: ClientAdmissionRequest,
    ) -> BoxFuture<'_, Result<ClientAdmissionDecision, ClientAdmissionError>> {
        Box::pin(async {
            self.active.fetch_add(1, Ordering::SeqCst);
            Ok(ClientAdmissionDecision::Granted)
        })
    }
    fn release<'a>(
        &'a self,
        _: &'a ClientApiKeyId,
        _: &'a gateway_core::engine::ModelRequestId,
    ) -> BoxFuture<'a, Result<bool, ClientAdmissionError>> {
        Box::pin(async {
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(true)
        })
    }
    fn abandon(&self, _: &ClientApiKeyId, _: &gateway_core::engine::ModelRequestId) {
        self.active.fetch_sub(1, Ordering::SeqCst);
    }
    fn restore(
        &self,
        _: ClientAdmissionRecovery,
    ) -> BoxFuture<'_, Result<ClientAdmissionRestoreResult, ClientAdmissionError>> {
        Box::pin(async { Ok(ClientAdmissionRestoreResult::default()) })
    }
}

#[derive(Default)]
struct Gateway {
    requests: Mutex<Vec<CodexAuxiliaryRequest>>,
    identity_calls: AtomicUsize,
}
impl CodexGateway for Gateway {
    fn identity<'a>(
        &'a self,
        context: &'a CodexAuthorizedContext,
    ) -> BoxFuture<'a, Result<CodexIdentity, ProviderError>> {
        Box::pin(async move {
            assert_eq!(context.entry_account_id().as_str(), "acct_native");
            self.identity_calls.fetch_add(1, Ordering::SeqCst);
            Ok(CodexIdentity {
                user_id: "user-real".into(),
                account_id: "workspace-real".into(),
                email: Some("test@example.test".into()),
                plan_type: Some("plus".into()),
                is_fedramp: true,
            })
        })
    }
    fn auxiliary<'a>(
        &'a self,
        context: &'a CodexAuthorizedContext,
        request: CodexAuxiliaryRequest,
    ) -> BoxFuture<'a, Result<MiddlewareResponse, ProviderError>> {
        Box::pin(async move {
            assert_eq!(context.entry_account_id().as_str(), "acct_native");
            assert!(
                !context
                    .account_scope()
                    .allows(&ProviderAccountId::new("acct_other").unwrap())
            );
            let status = if request.operation == CodexAuxiliaryOperation::ConfigBundle {
                404
            } else {
                201
            };
            self.requests.lock().unwrap().push(request);
            Ok(MiddlewareResponse::new(
                "openai".into(),
                status,
                vec![
                    MiddlewareHeader::new(
                        "content-type",
                        Bytes::from_static(b"application/octet-stream"),
                    ),
                    MiddlewareHeader::new("x-business", Bytes::from_static(b"one")),
                    MiddlewareHeader::new("x-business", Bytes::from_static(b"\x80two")),
                    MiddlewareHeader::new("set-cookie", Bytes::from_static(b"private")),
                    MiddlewareHeader::new("chatgpt-account-id", Bytes::from_static(b"private")),
                ],
                Box::new(Chunks(VecDeque::from([
                    Bytes::from_static(b"first\x00"),
                    Bytes::from_static(b"\xfflast"),
                ]))),
            ))
        })
    }
}
struct Chunks(VecDeque<Bytes>);
impl MiddlewareBody for Chunks {
    fn next_frame(&mut self) -> BoxFuture<'_, Result<Option<MiddlewareFrame>, MiddlewareError>> {
        Box::pin(async move {
            Ok(self.0.pop_front().map(|bytes| {
                MiddlewareFrame::new(bytes, MiddlewareFraming::RawBytes, self.0.is_empty())
            }))
        })
    }
    fn close(self: Box<Self>) -> BoxFuture<'static, ()> {
        Box::pin(async move { drop(self) })
    }
}

struct NativeProvider {
    gateway: Arc<Gateway>,
    versions: Mutex<Vec<String>>,
    operations: Mutex<Vec<gateway_core::operation::Operation>>,
}
#[async_trait]
impl Provider for NativeProvider {
    fn name(&self) -> &'static str {
        "openai"
    }
    fn codex_gateway(&self) -> Option<Arc<dyn CodexGateway>> {
        Some(self.gateway.clone())
    }
    fn catalog_generation(&self) -> ProviderCatalogGeneration {
        ProviderCatalogGeneration::default()
    }
    async fn query_model_capabilities(
        &self,
    ) -> Result<Vec<ProviderModelCapabilities>, ProviderError> {
        Ok(Vec::new())
    }
    async fn query_client_model_catalog(
        &self,
        scope: &FrozenAccountScope,
        protocol: &str,
        version: &str,
    ) -> Result<
        Option<Vec<ProviderModelDescriptor>>,
        gateway_core::routing::ProviderCatalogUnavailable,
    > {
        assert!(scope.allows(&ProviderAccountId::new("acct_native").unwrap()));
        assert!(!scope.allows(&ProviderAccountId::new("acct_other").unwrap()));
        assert_eq!(protocol, "codex");
        self.versions.lock().unwrap().push(version.to_owned());
        Ok(Some(vec![ProviderModelDescriptor {
            model: UpstreamModelId::new("gpt-native").unwrap(),
            content: ProviderModelContent::Native(
                gateway_core::operation::RawJsonPayload::new(
                    "codex",
                    Bytes::from_static(br#"{"slug":"gpt-native","future":{"nested":null}}"#),
                )
                .unwrap(),
            ),
        }]))
    }
    async fn execute(
        self: Arc<Self>,
        request: ProviderRequest,
        context: AttemptContext,
    ) -> Result<ProviderStream, ProviderError> {
        use gateway_core::engine::provider::ProviderCallMetadata;
        use gateway_core::event::{GatewayEvent, ProtocolWireEvent, ProviderEvent, ResponseMeta};
        use gateway_core::upstream::UpstreamTransport;
        assert_eq!(
            context.required_account().map(ProviderAccountId::as_str),
            Some("acct_native")
        );
        assert!(!context.is_diagnostic_required_account());
        assert!(
            !context
                .account_scope()
                .unwrap()
                .allows(&ProviderAccountId::new("acct_other").unwrap())
        );
        self.operations
            .lock()
            .unwrap()
            .push(request.operation().clone());
        let provider = ProviderKind::new("openai").unwrap();
        let account = ProviderAccountId::new("acct_native").unwrap();
        let transport = UpstreamTransport::new("http").unwrap();
        let (metadata, events) = if request.operation().kind() == OperationKind::Search {
            (
                ProviderCallMetadata::for_provider_endpoint(provider, account, transport),
                vec![ProviderEvent::canonical_with_wire(
                    vec![
                        GatewayEvent::Started(ResponseMeta::for_provider_endpoint("search-native")),
                        GatewayEvent::Completed(ResponseMeta::for_provider_endpoint(
                            "search-native",
                        )),
                    ],
                    ProtocolWireEvent::raw_json(
                        "openai",
                        Bytes::from_static(br#"{ "output":"native search", "future":1 }"#),
                    )
                    .unwrap(),
                )],
            )
        } else {
            let response = ResponseMeta::new("resp_native", "gpt-native");
            let events = vec![
                ProviderEvent::canonical_with_wire(vec![GatewayEvent::Started(response.clone())], ProtocolWireEvent::json("openai", Some("response.created".into()), json!({"type":"response.created","response":{"id":"resp_native","model":"gpt-native","status":"in_progress"}})).unwrap()),
                ProviderEvent::canonical_with_wire(vec![GatewayEvent::Completed(response)], ProtocolWireEvent::json("openai", Some("response.completed".into()), json!({"type":"response.completed","response":{"id":"resp_native","model":"gpt-native","status":"completed","output":[],"future":{"opaque":true}}})).unwrap()),
            ];
            (
                ProviderCallMetadata::new(
                    provider,
                    UpstreamModelId::new("gpt-native").unwrap(),
                    account,
                    transport,
                ),
                events,
            )
        };
        Ok(ProviderStream::new(
            metadata,
            futures::stream::iter(events.into_iter().map(Ok)),
            (),
        ))
    }
}

struct Fixture {
    app: axum::Router,
    provider: Arc<NativeProvider>,
    admissions: Arc<Admissions>,
    snapshots: RuntimeSnapshotHandle,
}

fn snapshot(enabled: bool) -> RuntimeSnapshot {
    let provider = ProviderKind::new("openai").unwrap();
    let directory = Arc::new(RuntimeAccountDirectory::new(BTreeMap::from([
        (
            ProviderAccountId::new("acct_native").unwrap(),
            RuntimeAccount::new(provider.clone(), BTreeSet::new()),
        ),
        (
            ProviderAccountId::new("acct_other").unwrap(),
            RuntimeAccount::new(provider.clone(), BTreeSet::new()),
        ),
    ])));
    RuntimeSnapshot::new(
        ConfigRevision::new(if enabled { 1 } else { 2 }).unwrap(),
        gateway_core::settings::SettingsValues::new(2, "smart", Default::default(), None, None)
            .with_codex_public_origin(Some("https://configured.example.test:8443".into())),
        vec![provider.clone()],
        vec![ProviderModel::new(
            provider,
            UpstreamModelId::new("gpt-native").unwrap(),
            ModelCapabilities::new(BTreeSet::from([OperationKind::Generate]), None),
        )],
        vec![
            ClientPolicy::new(
                ClientApiKeyId::new("key-native").unwrap(),
                PlaintextClientApiKey::new(KEY).unwrap(),
                Arc::new(FrozenAccountScope::new(
                    directory.clone(),
                    ClientRoutingScope::all_accounts(),
                )),
                true,
                RateLimits::unlimited(),
            )
            .with_codex_access(Some(CodexAccessBinding {
                enabled,
                mode: CodexAccessMode::Fixed,
                account_id: ProviderAccountId::new("acct_native").unwrap(),
            })),
        ],
    )
    .unwrap()
    .with_account_directory(directory)
}

async fn fixture() -> Fixture {
    let provider = Arc::new(NativeProvider {
        gateway: Arc::default(),
        versions: Mutex::default(),
        operations: Mutex::default(),
    });
    let admissions = Arc::new(Admissions::default());
    let snapshots = RuntimeSnapshotHandle::new(snapshot(true));
    let execution = Arc::new(DefaultExecutionService::new(
        snapshots.clone(),
        Arc::new(ModelStore),
        ProviderRegistry::new([provider.clone() as Arc<dyn Provider>]).unwrap(),
        admissions.clone(),
        Arc::new(super::UnusedContinuation),
        Arc::new(super::IgnoredClientApiKeyUsage),
        Arc::new(crate::support::RecordingDiagnostics::default()),
    ));
    let app = super::api_router(execution as Arc<dyn ExecutionService>).await;
    Fixture {
        app,
        provider,
        admissions,
        snapshots,
    }
}

fn request(method: Method, path: &str) -> axum::http::request::Builder {
    Request::builder()
        .method(method)
        .uri(path)
        .header("authorization", format!("Bearer {KEY}"))
}

#[tokio::test]
async fn identity_exposes_only_real_bound_account_and_configured_origin() {
    let fixture = fixture().await;
    let response = fixture
        .app
        .clone()
        .oneshot(
            request(Method::GET, "/v1/user-auth-credential/whoami")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(
        body,
        json!({"email":"test@example.test","chatgpt_user_id":"user-real","chatgpt_account_id":"workspace-real","chatgpt_plan_type":"plus","chatgpt_account_is_fedramp":true})
    );
    let response = fixture
        .app
        .clone()
        .oneshot(
            request(Method::GET, "/backend-api/wham/accounts/check")
                .header("host", "attacker.test")
                .header("x-forwarded-host", "attacker.test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(
        body,
        json!({"accounts":[{"id":"workspace-real","plan_type":"plus","workspace_backend_origin":"https://configured.example.test:8443","account_routing_override":"NO_CONSTRAINT"}],"account_ordering":["workspace-real"],"default_account_id":"workspace-real"})
    );
    assert_eq!(fixture.admissions.active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn auxiliary_preserves_bytes_query_multivalue_headers_and_upstream_errors() {
    let fixture = fixture().await;
    for (method, path, status) in [
        (
            Method::POST,
            "/backend-api/codex/alpha/notes/v2/append_to_file?cursor=%2f&cursor=+",
            StatusCode::CREATED,
        ),
        (
            Method::GET,
            "/backend-api/wham/config/bundle?cursor=%2f&cursor=+",
            StatusCode::NOT_FOUND,
        ),
    ] {
        let response = fixture
            .app
            .clone()
            .oneshot(
                request(method, path)
                    .header("cookie", "client-private")
                    .header("chatgpt-account-id", "client-account")
                    .header("x-openai-fedramp", "false")
                    .header("x-openai-account-routing-override", "client-routing")
                    .header("connection", "x-hop")
                    .header("x-hop", "private")
                    .header("x-business", "first")
                    .header("x-business", Bytes::from_static(b"\x80second").as_ref())
                    .header("thread-id", "thread-native")
                    .body(Body::from(Bytes::from_static(b"opaque\x00\xff")))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), status);
        assert!(!response.headers().contains_key("set-cookie"));
        assert!(!response.headers().contains_key("chatgpt-account-id"));
        assert_eq!(
            response
                .headers()
                .get_all("x-business")
                .iter()
                .map(|header| header.as_bytes())
                .collect::<Vec<_>>(),
            [b"one".as_slice(), b"\x80two".as_slice()]
        );
        assert_eq!(
            fixture.admissions.active.load(Ordering::SeqCst),
            1,
            "headers retain the admission lease"
        );
        assert_eq!(
            to_bytes(response.into_body(), usize::MAX)
                .await
                .unwrap()
                .as_ref(),
            b"first\x00\xfflast"
        );
        assert_eq!(fixture.admissions.active.load(Ordering::SeqCst), 0);
    }
    let requests = fixture.provider.gateway.requests.lock().unwrap();
    assert_eq!(
        requests.len(),
        2,
        "writes and upstream errors are never replayed"
    );
    for captured in requests.iter() {
        assert_eq!(captured.query.as_deref(), Some("cursor=%2f&cursor=+"));
        assert_eq!(captured.body.as_ref(), b"opaque\x00\xff");
        let headers = &captured.headers;
        for forbidden in [
            "authorization",
            "cookie",
            "chatgpt-account-id",
            "x-openai-fedramp",
            "x-openai-account-routing-override",
            "connection",
            "x-hop",
        ] {
            assert!(
                !headers.iter().any(|header| header.name() == forbidden),
                "{forbidden}"
            );
        }
        assert_eq!(
            headers
                .iter()
                .filter(|header| header.name() == "x-business")
                .map(|header| header.value().as_ref())
                .collect::<Vec<_>>(),
            [b"first".as_slice(), b"\x80second".as_slice()]
        );
        assert!(
            headers.iter().any(|header| header.name() == "thread-id"
                && header.value().as_ref() == b"thread-native")
        );
    }
}

#[tokio::test]
async fn all_auxiliary_routes_select_only_the_declared_operation_and_method() {
    let fixture = fixture().await;
    for (path, operation) in [
        (
            "history/v2/list_windows",
            CodexAuxiliaryOperation::HistoryListWindows,
        ),
        (
            "history/v2/list_items",
            CodexAuxiliaryOperation::HistoryListItems,
        ),
        (
            "history/v2/read_item",
            CodexAuxiliaryOperation::HistoryReadItem,
        ),
        (
            "history/v2/search_contents",
            CodexAuxiliaryOperation::HistorySearchContents,
        ),
        (
            "notes/v2/thread_hint",
            CodexAuxiliaryOperation::NotesThreadHint,
        ),
        (
            "notes/v2/list_files_by_prefix",
            CodexAuxiliaryOperation::NotesListFilesByPrefix,
        ),
        ("notes/v2/read_file", CodexAuxiliaryOperation::NotesReadFile),
        (
            "notes/v2/search_contents",
            CodexAuxiliaryOperation::NotesSearchContents,
        ),
        (
            "notes/v2/append_to_file",
            CodexAuxiliaryOperation::NotesAppendToFile,
        ),
        (
            "notes/v2/write_file",
            CodexAuxiliaryOperation::NotesWriteFile,
        ),
    ] {
        let path = format!("/backend-api/codex/alpha/{path}");
        let response = fixture
            .app
            .clone()
            .oneshot(request(Method::POST, &path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            fixture
                .provider
                .gateway
                .requests
                .lock()
                .unwrap()
                .last()
                .unwrap()
                .operation,
            operation
        );
        let response = fixture
            .app
            .clone()
            .oneshot(request(Method::GET, &path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
    }
    for (path, operation) in [
        ("usage", CodexAuxiliaryOperation::Usage),
        ("profiles/me", CodexAuxiliaryOperation::Profile),
        ("settings/user", CodexAuxiliaryOperation::UserSettings),
        (
            "workspace-messages",
            CodexAuxiliaryOperation::WorkspaceMessages,
        ),
        ("config/bundle", CodexAuxiliaryOperation::ConfigBundle),
    ] {
        let path = format!("/backend-api/wham/{path}");
        let response = fixture
            .app
            .clone()
            .oneshot(request(Method::GET, &path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(
            response.status().as_u16(),
            if operation == CodexAuxiliaryOperation::ConfigBundle {
                404
            } else {
                201
            }
        );
        to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            fixture
                .provider
                .gateway
                .requests
                .lock()
                .unwrap()
                .last()
                .unwrap()
                .operation,
            operation
        );
        for method in [Method::POST, Method::HEAD] {
            let response = fixture
                .app
                .clone()
                .oneshot(request(method, &path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        }
    }
    for path in [
        "/auth",
        "/auth/unknown",
        "/v1/user-auth-credential",
        "/v1/user-auth-credential/unknown",
        "/backend-api",
        "/backend-api/connectors",
        "/backend-api/codex/alpha/history/v2/delete",
        // 迁移前的原生前缀不能落到管理端 SPA
        "/codex",
        "/codex/backend-api/codex/models",
        "/codex/v1/user-auth-credential/whoami",
    ] {
        let response = fixture
            .app
            .clone()
            .oneshot(request(Method::GET, path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }
}

#[tokio::test]
async fn model_catalog_uses_native_scope_and_client_version_and_disabled_binding_leaves_v1_available()
 {
    let fixture = fixture().await;
    let response = fixture
        .app
        .clone()
        .oneshot(
            request(
                Method::GET,
                "/backend-api/codex/models?client_version=0.163.0-alpha.10.2",
            )
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value =
        serde_json::from_slice(&to_bytes(response.into_body(), usize::MAX).await.unwrap()).unwrap();
    assert_eq!(
        body,
        json!({"models":[{"slug":"gpt-native","future":{"nested":null}}]})
    );
    assert_eq!(
        *fixture.provider.versions.lock().unwrap(),
        ["0.163.0-alpha.10.2"]
    );
    fixture.snapshots.publish(snapshot(false));
    for (method, path) in [
        (Method::GET, "/backend-api/codex/models"),
        (Method::GET, "/v1/user-auth-credential/whoami"),
        (Method::POST, "/backend-api/codex/responses"),
        (Method::POST, "/backend-api/codex/alpha/search"),
        (
            Method::POST,
            "/backend-api/codex/alpha/history/v2/read_item",
        ),
    ] {
        let response = fixture
            .app
            .clone()
            .oneshot(request(method, path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED, "{path}");
    }
    let response = fixture
        .app
        .clone()
        .oneshot(
            request(Method::GET, "/v1/models")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}

struct ModelStore;
#[async_trait]
impl gateway_core::engine::ExecutionStore for ModelStore {
    async fn create_model_request(
        &self,
        _: gateway_core::engine::NewModelRequest,
    ) -> Result<(), gateway_core::error::StoreError> {
        Ok(())
    }
    async fn record_attempt(
        &self,
        _: gateway_core::engine::AttemptRecord,
    ) -> Result<(), gateway_core::error::StoreError> {
        Ok(())
    }
    async fn mark_send_state(
        &self,
        _: &gateway_core::engine::ModelRequestId,
        _: gateway_core::upstream::UpstreamSendState,
    ) -> Result<(), gateway_core::error::StoreError> {
        Ok(())
    }
    async fn mark_downstream_committed(
        &self,
        _: &gateway_core::engine::ModelRequestId,
        _: std::time::SystemTime,
        _: Option<u16>,
    ) -> Result<(), gateway_core::error::StoreError> {
        Ok(())
    }
    async fn record_client_status(
        &self,
        _: &gateway_core::engine::ModelRequestId,
        _: u16,
    ) -> Result<(), gateway_core::error::StoreError> {
        Ok(())
    }
    async fn record_intermediate_failure(
        &self,
        _: gateway_core::engine::IntermediateFailure,
    ) -> Result<(), gateway_core::error::StoreError> {
        Ok(())
    }
    async fn finalize_model_request(
        &self,
        _: gateway_core::engine::ModelRequestFinalization,
    ) -> Result<(), gateway_core::error::StoreError> {
        Ok(())
    }
    async fn recover_expired(
        &self,
        _: std::time::SystemTime,
    ) -> Result<gateway_core::engine::RecoveryReport, gateway_core::error::StoreError> {
        unreachable!("no background recovery")
    }
}

#[tokio::test]
async fn native_http_sse_and_search_reuse_model_execution_and_wire_adapters() {
    let fixture = fixture().await;
    for stream in [false, true] {
        let response = fixture
            .app
            .clone()
            .oneshot(
                request(Method::POST, "/backend-api/codex/responses")
                    .header("content-type", "application/json")
                    .header("thread-id", "native-thread")
                    .body(Body::from(
                        json!({"model":"gpt-native","input":[],"stream":stream,"future":"kept"})
                            .to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        if stream {
            let body = std::str::from_utf8(&body).unwrap();
            assert!(body.contains("response.created"));
            assert!(body.contains("response.completed"));
            assert!(body.contains("[DONE]"));
        } else {
            let body: Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(body["id"], "resp_native");
            assert_eq!(body["future"]["opaque"], true);
        }
        assert_eq!(fixture.admissions.active.load(Ordering::SeqCst), 0);
    }
    let raw = Bytes::from_static(br#"{ "model":"gpt-native", "future":1, "future":2 }"#);
    let response = fixture
        .app
        .clone()
        .oneshot(
            request(Method::POST, "/backend-api/codex/alpha/search")
                .header("content-type", "application/json")
                .body(Body::from(raw.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap()
            .as_ref(),
        br#"{ "output":"native search", "future":1 }"#
    );
    let operations = fixture.provider.operations.lock().unwrap();
    assert_eq!(operations.len(), 3);
    let gateway_core::operation::Operation::Search(search) = &operations[2] else {
        panic!("search operation")
    };
    assert_eq!(search.payload().body(), &raw);
    for operation in &operations[..2] {
        let gateway_core::operation::Operation::Generate(generate) = operation else {
            panic!("generate operation")
        };
        assert_eq!(generate.protocol_payload().body()["future"], "kept");
    }
}

#[tokio::test]
async fn native_websocket_reauthenticates_binding_on_next_response_create() {
    use futures::{SinkExt as _, StreamExt as _};
    use tokio_tungstenite::{
        connect_async,
        tungstenite::{Message, client::IntoClientRequest},
    };
    let fixture = fixture().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = fixture.app;
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut opening = format!("ws://{address}/backend-api/codex/responses")
        .into_client_request()
        .unwrap();
    opening
        .headers_mut()
        .insert("authorization", format!("Bearer {KEY}").parse().unwrap());
    let (mut socket, response) = connect_async(opening).await.unwrap();
    assert_eq!(response.status(), StatusCode::SWITCHING_PROTOCOLS);
    let create = json!({"type":"response.create","model":"gpt-native","input":[]}).to_string();
    socket
        .send(Message::Text(create.clone().into()))
        .await
        .unwrap();
    loop {
        let message = tokio::time::timeout(std::time::Duration::from_secs(5), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if let Message::Text(text) = message {
            let event: Value = serde_json::from_str(&text).unwrap();
            assert_ne!(event["type"], "error", "{event}");
            if event["type"] == "response.completed" {
                break;
            }
        }
    }
    fixture.snapshots.publish(snapshot(false));
    socket.send(Message::Text(create.into())).await.unwrap();
    loop {
        let message = tokio::time::timeout(std::time::Duration::from_secs(5), socket.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        if let Message::Text(text) = message {
            let event: Value = serde_json::from_str(&text).unwrap();
            if event["type"] == "error" {
                assert_eq!(event["status"], 401);
                break;
            }
            assert_ne!(event["type"], "response.created");
        }
    }
    assert_eq!(fixture.provider.operations.lock().unwrap().len(), 1);
    socket.close(None).await.unwrap();
    server.abort();
}

#[tokio::test]
async fn dropping_auxiliary_http_response_releases_the_primed_body_lease() {
    let fixture = fixture().await;
    let response = fixture
        .app
        .clone()
        .oneshot(
            request(
                Method::POST,
                "/backend-api/codex/alpha/history/v2/read_item",
            )
            .body(Body::empty())
            .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    assert_eq!(fixture.admissions.active.load(Ordering::SeqCst), 1);
    drop(response);
    assert_eq!(fixture.admissions.active.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn native_missing_and_invalid_keys_never_reach_provider() {
    let fixture = fixture().await;
    for authorization in [None, Some("Bearer sk_invalid")] {
        let mut request = Request::get("/v1/user-auth-credential/whoami");
        if let Some(authorization) = authorization {
            request = request.header("authorization", authorization);
        }
        let response = fixture
            .app
            .clone()
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    }
    assert_eq!(
        fixture
            .provider
            .gateway
            .identity_calls
            .load(Ordering::SeqCst),
        0
    );
    assert_eq!(fixture.admissions.active.load(Ordering::SeqCst), 0);
}
