//! 原生授权收窄、每轮重认证与辅助准入及正文释放的行为测试

use super::*;
use gateway_core::{
    engine::{
        authentication::ClientAuthenticationPurpose,
        codex::{
            CodexAuthorizedContext, CodexAuxiliaryOperation, CodexAuxiliaryRequest, CodexGateway,
            CodexIdentity,
        },
        execution::{AuthenticatedClient, PreparedRootExecution},
        middleware::{
            MiddlewareBody, MiddlewareError, MiddlewareFrame, MiddlewareFraming, MiddlewareResponse,
        },
    },
    lifecycle::CancellationToken,
    policy::{CodexAccessBinding, CodexAccessMode},
};

fn binding(enabled: bool, account: &str) -> CodexAccessBinding {
    CodexAccessBinding {
        enabled,
        mode: CodexAccessMode::Fixed,
        account_id: ProviderAccountId::new(account).unwrap(),
    }
}

fn snapshot(revision: u64, binding: Option<CodexAccessBinding>, allowed: bool) -> RuntimeSnapshot {
    let openai = ProviderKind::new("openai").unwrap();
    let xai = ProviderKind::new("xai").unwrap();
    let directory = Arc::new(RuntimeAccountDirectory::new(BTreeMap::from([
        (
            ProviderAccountId::new("acct_start").unwrap(),
            RuntimeAccount::new(openai.clone(), BTreeSet::new()).with_model_access(
                gateway_core::account::AccountModelAccess::new(
                    gateway_core::account::AccountModelAccessMode::Denylist,
                    vec!["gpt-blocked".into()],
                )
                .unwrap(),
            ),
        ),
        (
            ProviderAccountId::new("acct_other").unwrap(),
            RuntimeAccount::new(openai.clone(), BTreeSet::new()),
        ),
        (
            ProviderAccountId::new("acct_xai").unwrap(),
            RuntimeAccount::new(xai.clone(), BTreeSet::new()),
        ),
    ])));
    let scope = FrozenAccountScope::new(directory.clone(), ClientRoutingScope::all_accounts());
    let scope = if allowed {
        scope
    } else {
        scope.excluding_account(ProviderAccountId::new("acct_start").unwrap())
    };
    RuntimeSnapshot::new(
        ConfigRevision::new(revision).unwrap(),
        gateway_core::settings::SettingsValues::new(2, "smart", Default::default(), None, None)
            .with_codex_public_origin(Some("https://codex.example.test".into())),
        vec![openai.clone(), xai],
        vec![ProviderModel::new(
            openai,
            UpstreamModelId::new("gpt-start").unwrap(),
            ModelCapabilities::new(
                BTreeSet::from([OperationKind::Generate, OperationKind::Search]),
                None,
            ),
        )],
        vec![
            ClientPolicy::new(
                ClientApiKeyId::new("key_start_test").unwrap(),
                PlaintextClientApiKey::new("sk_start_test").unwrap(),
                Arc::new(scope),
                true,
                RateLimits {
                    max_concurrency: 1,
                    requests_per_minute: 10,
                },
            )
            .with_codex_access(binding),
        ],
    )
    .unwrap()
    .with_account_directory(directory)
}

fn service_for(
    snapshot: RuntimeSnapshot,
    provider: Arc<dyn Provider>,
    admissions: Arc<dyn ClientAdmissionPort>,
) -> (DefaultExecutionService, RuntimeSnapshotHandle) {
    let snapshots = RuntimeSnapshotHandle::new(snapshot);
    let service = DefaultExecutionService::new(
        snapshots.clone(),
        Arc::new(TrackingExecutionStore::default()),
        ProviderRegistry::new([provider]).unwrap(),
        admissions,
        Arc::new(UnusedContinuation),
        Arc::new(RecordingClientApiKeyUsage::default()),
        Arc::new(crate::RecordingDiagnostics::default()),
    );
    (service, snapshots)
}

fn pool_binding(enabled: bool, account: &str) -> CodexAccessBinding {
    CodexAccessBinding {
        enabled,
        mode: CodexAccessMode::SessionPool,
        account_id: ProviderAccountId::new(account).unwrap(),
    }
}

async fn authenticate(
    service: &DefaultExecutionService,
) -> Result<AuthenticatedClient, gateway_core::engine::execution::ClientAuthenticationError> {
    service
        .authenticate_request(
            ClientAuthenticationRequest::bearer("sk_start_test")
                .unwrap()
                .with_purpose(ClientAuthenticationPurpose::Codex),
        )
        .await
}

async fn prepare(service: &DefaultExecutionService) -> PreparedRootExecution {
    service
        .prepare_execution(authenticate(service).await.unwrap())
        .await
        .unwrap()
}

/// 只记录 Core 交给 Provider 的池标志与账号范围，随后如实失败以结束本次执行
///
/// 会话键解析与拒绝规则属于 Provider，本文件的职责仅是确认 `session_pool` 与授权池
/// 经 Core 到达 attempt；fixed 与普通 `/v1` 必须继续拿到关闭的标志
/// `retry_round` 打开的用例先返回一个可重试的选号失败，让 coordinator 走既有重试
/// 决策进入第二轮，再由拒绝重试的失败收尾，因此两轮都经过真实执行路径
#[derive(Default)]
struct AttemptWiringProvider {
    observed: Mutex<Vec<AttemptWiring>>,
    retry_round: bool,
}

struct AttemptWiring {
    session_pool: bool,
    required_account: Option<String>,
    allows_entry: bool,
    allows_pool_peer: bool,
    allows_other_provider: bool,
    /// 本次 attempt 向 Core 声明的执行账号，用来确认池内改选仍被授权范围接受
    account: &'static str,
}

#[async_trait]
impl Provider for AttemptWiringProvider {
    fn name(&self) -> &'static str {
        "openai"
    }
    fn catalog_generation(&self) -> ProviderCatalogGeneration {
        ProviderCatalogGeneration::default()
    }
    async fn query_model_capabilities(
        &self,
    ) -> Result<Vec<ProviderModelCapabilities>, ProviderError> {
        Ok(Vec::new())
    }
    async fn execute(
        self: Arc<Self>,
        request: ProviderRequest,
        context: AttemptContext,
    ) -> Result<ProviderStream, ProviderError> {
        let scope = context.account_scope().expect("attempt account scope");
        let account = |id: &str| ProviderAccountId::new(id).unwrap();
        let retry_round = self.retry_round;
        let mut observed = self.observed.lock().unwrap();
        // 尚未认领的首请求发送前失败后，第二轮可在同一授权池继续选择
        let selected = if retry_round && !observed.is_empty() {
            "acct_other"
        } else {
            "acct_start"
        };
        observed.push(AttemptWiring {
            session_pool: context.codex_session_pool(),
            required_account: context
                .required_account()
                .map(|account| account.as_str().to_owned()),
            allows_entry: scope.allows(&account("acct_start")),
            allows_pool_peer: scope.allows(&account("acct_other")),
            allows_other_provider: scope.allows(&account("acct_xai")),
            account: selected,
        });
        let round = observed.len();
        drop(observed);
        if !retry_round {
            return Err(attempt_wiring_error());
        }
        // 第一轮如实给出可重试的本地选号失败，第二轮才用拒绝重试的失败终止执行
        let error = if round == 1 {
            ProviderError::new(
                ProviderErrorKind::NoEligibleAccount,
                UpstreamSendState::NotSent,
            )
        } else {
            attempt_wiring_error()
        };
        let metadata = ProviderCallMetadata::new(
            request.candidate().provider().clone(),
            request.candidate().upstream_model().unwrap().clone(),
            account(selected),
            UpstreamTransport::new("websocket").unwrap(),
        );
        Ok(ProviderStream::new(
            metadata,
            futures::stream::iter([Err(error)]),
            (),
        ))
    }
}

fn attempt_wiring_error() -> ProviderError {
    ProviderError::new(
        ProviderErrorKind::NoEligibleAccount,
        UpstreamSendState::NotSent,
    )
    .with_retry_prohibited()
}

async fn start_attempt_wiring(
    service: &DefaultExecutionService,
    codex_request: bool,
) -> gateway_core::engine::execution::StartedExecution {
    let client = if codex_request {
        authenticate(service).await.unwrap()
    } else {
        service.authenticate("sk_start_test").unwrap()
    };
    service
        .start(StartExecution {
            client,
            public_model: PublicModelId::new("gpt-start").unwrap(),
            operation: start_operation(),
            metadata: ExecutionRequestMetadata {
                protocol: "openai".into(),
                endpoint: if codex_request {
                    "/backend-api/codex/responses"
                } else {
                    "/v1/responses"
                }
                .into(),
                transport: ClientTransport::WebSocket,
                stream: true,
                client_ip: None,
                user_agent: None,
                previous_response_id: None,
            },
        })
        .await
        .unwrap()
}

async fn assert_attempt_wiring(binding: CodexAccessBinding, codex_request: bool, pool: bool) {
    let provider = Arc::new(AttemptWiringProvider::default());
    let (service, _) = service_for(
        snapshot(1, Some(binding), true),
        provider.clone(),
        Arc::new(UnusedAdmissions),
    );
    let mut started = start_attempt_wiring(&service, codex_request).await;
    assert!(started.session.next_event().await.is_err());
    started.session.detach_finalize().await;
    let observed = provider.observed.lock().unwrap();
    assert_eq!(observed.len(), 1, "one attempt per execution");
    let attempt = &observed[0];
    let required = (codex_request && !pool).then_some("acct_start");
    assert_eq!(attempt.session_pool, pool);
    assert_eq!(attempt.required_account.as_deref(), required);
    assert!(attempt.allows_entry);
    // 会话池只保留同一 Provider 的授权池；普通 `/v1` 仍持有 Key 的完整范围
    assert_eq!(attempt.allows_pool_peer, !codex_request || pool);
    assert_eq!(attempt.allows_other_provider, !codex_request);
}

#[test]
fn codex_binding_mode_reaches_the_attempt_as_pool_flag_and_authorized_scope() {
    block_on(async {
        assert_attempt_wiring(binding(true, "acct_start"), true, false).await;
        assert_attempt_wiring(pool_binding(true, "acct_start"), true, true).await;
        assert_attempt_wiring(pool_binding(true, "acct_start"), false, false).await;
    });
}

#[test]
fn session_pool_retry_keeps_pool_flag_and_authorized_scope_in_both_rounds() {
    block_on(async {
        let provider = Arc::new(AttemptWiringProvider {
            retry_round: true,
            ..Default::default()
        });
        let (service, _) = service_for(
            snapshot(1, Some(pool_binding(true, "acct_start")), true),
            provider.clone(),
            Arc::new(UnusedAdmissions),
        );
        let mut started = start_attempt_wiring(&service, true).await;
        // Core 不把会话池入口账号当作固定执行账号，首轮发送前失败后仍保留授权池
        let result = started.session.next_event().await;
        assert!(matches!(
            result,
            Err(EngineError::Provider(ref error))
                if error.kind() == ProviderErrorKind::NoEligibleAccount
        ));
        started.session.detach_finalize().await;
        let observed = provider.observed.lock().unwrap();
        assert_eq!(
            observed.len(),
            2,
            "second round ends on the retry-prohibited failure"
        );
        for attempt in observed.iter() {
            assert!(attempt.session_pool);
            assert!(attempt.required_account.is_none());
            assert!(attempt.allows_entry);
            assert!(attempt.allows_pool_peer);
            assert!(!attempt.allows_other_provider);
        }
        assert_eq!(observed[0].account, "acct_start");
        assert_eq!(
            observed[1].account, "acct_other",
            "round two reselects inside the pool the client was authorized for"
        );
    });
}

fn request() -> CodexAuxiliaryRequest {
    CodexAuxiliaryRequest {
        operation: CodexAuxiliaryOperation::NotesAppendToFile,
        query: Some("opaque=%2F&opaque=+".into()),
        headers: Vec::new(),
        body: bytes::Bytes::from_static(b"opaque body"),
    }
}

#[derive(Default)]
struct NativeGateway {
    calls: AtomicUsize,
    contexts: Mutex<Vec<CodexAuthorizedContext>>,
    pending: AtomicBool,
    fail: AtomicBool,
}

impl NativeGateway {
    fn observe(&self, context: &CodexAuthorizedContext) {
        assert_eq!(context.entry_account_id().as_str(), "acct_start");
        assert!(context.account_scope().allows(context.entry_account_id()));
        assert!(
            !context
                .account_scope()
                .allows(&ProviderAccountId::new("acct_other").unwrap())
        );
        self.contexts.lock().unwrap().push(context.clone());
        self.calls.fetch_add(1, Ordering::SeqCst);
    }
}

impl CodexGateway for NativeGateway {
    fn identity<'a>(
        &'a self,
        context: &'a CodexAuthorizedContext,
    ) -> BoxFuture<'a, Result<CodexIdentity, ProviderError>> {
        Box::pin(async move {
            self.observe(context);
            Ok(CodexIdentity {
                user_id: "user_real".into(),
                account_id: "workspace_real".into(),
                email: None,
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
            self.observe(context);
            assert_eq!(
                request.operation,
                CodexAuxiliaryOperation::NotesAppendToFile
            );
            assert_eq!(request.query.as_deref(), Some("opaque=%2F&opaque=+"));
            assert_eq!(request.body.as_ref(), b"opaque body");
            if self.pending.load(Ordering::SeqCst) {
                futures::future::pending::<()>().await;
            }
            if self.fail.load(Ordering::SeqCst) {
                return Err(ProviderError::new(
                    ProviderErrorKind::Transport,
                    UpstreamSendState::Ambiguous,
                ));
            }
            Ok(MiddlewareResponse::new(
                "openai".into(),
                201,
                Vec::new(),
                Box::new(NativeBody { remaining: 2 }),
            ))
        })
    }
}

struct NativeBody {
    remaining: usize,
}
impl MiddlewareBody for NativeBody {
    fn next_frame(&mut self) -> BoxFuture<'_, Result<Option<MiddlewareFrame>, MiddlewareError>> {
        Box::pin(async move {
            if self.remaining == 0 {
                return Ok(None);
            }
            self.remaining -= 1;
            Ok(Some(MiddlewareFrame::new(
                bytes::Bytes::from_static(b"chunk"),
                MiddlewareFraming::RawBytes,
                self.remaining == 0,
            )))
        })
    }
    fn close(self: Box<Self>) -> BoxFuture<'static, ()> {
        Box::pin(async move { drop(self) })
    }
}

#[derive(Default)]
struct NativeProvider {
    gateway: Arc<NativeGateway>,
    attempts: Mutex<Vec<String>>,
    recover_transport: AtomicBool,
    hold_generation: AtomicBool,
}
#[async_trait]
impl Provider for NativeProvider {
    fn name(&self) -> &'static str {
        "openai"
    }
    fn catalog_generation(&self) -> ProviderCatalogGeneration {
        ProviderCatalogGeneration::default()
    }
    async fn query_model_capabilities(
        &self,
    ) -> Result<Vec<ProviderModelCapabilities>, ProviderError> {
        Ok(Vec::new())
    }
    fn codex_gateway(&self) -> Option<Arc<dyn CodexGateway>> {
        Some(self.gateway.clone())
    }
    async fn query_client_model_catalog(
        &self,
        scope: &FrozenAccountScope,
        protocol: &str,
        version: &str,
    ) -> Result<
        Option<Vec<gateway_core::routing::ProviderModelDescriptor>>,
        gateway_core::routing::ProviderCatalogUnavailable,
    > {
        assert_eq!((protocol, version), ("codex", "0.163.0-alpha.10.2"));
        assert!(scope.allows(&ProviderAccountId::new("acct_start").unwrap()));
        assert!(!scope.allows(&ProviderAccountId::new("acct_other").unwrap()));
        Ok(Some(Vec::new()))
    }
    async fn execute(
        self: Arc<Self>,
        request: ProviderRequest,
        context: AttemptContext,
    ) -> Result<ProviderStream, ProviderError> {
        assert_eq!(
            context.required_account().map(ProviderAccountId::as_str),
            Some("acct_start")
        );
        assert!(!context.is_diagnostic_required_account());
        assert!(
            !context
                .account_scope()
                .unwrap()
                .allows(&ProviderAccountId::new("acct_other").unwrap())
        );
        let attempt = {
            let mut attempts = self.attempts.lock().unwrap();
            attempts.push("acct_start".into());
            attempts.len()
        };
        if self.hold_generation.load(Ordering::SeqCst) {
            use futures::StreamExt as _;
            let metadata = ProviderCallMetadata::new(
                request.candidate().provider().clone(),
                request.candidate().upstream_model().unwrap().clone(),
                ProviderAccountId::new("acct_start").unwrap(),
                UpstreamTransport::new("http").unwrap(),
            );
            let started = ProviderEvent::canonical(GatewayEvent::Started(ResponseMeta::new(
                "resp_in_flight",
                "gpt-start",
            )));
            return Ok(ProviderStream::new(
                metadata,
                futures::stream::iter([Ok(started)]).chain(futures::stream::pending()),
                (),
            ));
        }
        if self.recover_transport.load(Ordering::SeqCst) && attempt <= 2 {
            let error =
                ProviderError::new(ProviderErrorKind::Transport, UpstreamSendState::Ambiguous);
            let error = if attempt == 1 {
                error.with_pre_delivery_transport_fallback()
            } else {
                error.with_pre_delivery_transport_retry(
                    std::num::NonZeroU32::new(1).unwrap(),
                    Duration::ZERO,
                )
            };
            let metadata = ProviderCallMetadata::new(
                request.candidate().provider().clone(),
                request.candidate().upstream_model().unwrap().clone(),
                ProviderAccountId::new("acct_start").unwrap(),
                UpstreamTransport::new("websocket").unwrap(),
            );
            return Ok(ProviderStream::new(
                metadata,
                futures::stream::iter([Err(error)]),
                (),
            ));
        }
        Err(ProviderError::new(
            ProviderErrorKind::NoEligibleAccount,
            UpstreamSendState::NotSent,
        ))
    }
}

#[test]
fn native_authorization_requires_enabled_binding_valid_intersection_and_origin() {
    block_on(async {
        for (binding, allowed) in [
            (None, true),
            (Some(binding(false, "acct_start")), true),
            (Some(binding(true, "acct_start")), false),
            (Some(binding(true, "acct_xai")), true),
            (Some(binding(true, "acct_missing")), true),
        ] {
            let (service, _) = service_for(
                snapshot(1, binding, allowed),
                Arc::<NativeProvider>::default(),
                Arc::new(UnusedAdmissions),
            );
            assert!(authenticate(&service).await.is_err());
            assert!(
                service.authenticate("sk_start_test").is_ok(),
                "ordinary v1 authentication is independent of Codex binding"
            );
        }
        let snapshot = snapshot(1, Some(binding(true, "acct_start")), true);
        let snapshot = snapshot
            .with_settings(&snapshot.settings().clone().with_codex_public_origin(None))
            .unwrap();
        let (service, _) = service_for(
            snapshot,
            Arc::<NativeProvider>::default(),
            Arc::new(UnusedAdmissions),
        );
        assert!(authenticate(&service).await.is_err());
        assert!(service.authenticate("sk_start_test").is_ok());
    });
}

#[test]
fn native_scope_survives_settings_and_catalog_while_v1_keeps_full_scope() {
    block_on(async {
        let (service, _) = service_for(
            snapshot(1, Some(binding(true, "acct_start")), true),
            Arc::<NativeProvider>::default(),
            Arc::new(UnusedAdmissions),
        );
        let client = authenticate(&service).await.unwrap();
        let mut prepared = service.prepare_verified_execution(client).unwrap();
        let settings = prepared.request_settings();
        let mut values = settings.execution_values().unwrap();
        values.client_limits.max_concurrency = 7;
        let settings = settings.replace_execution(&values, "native-test").unwrap();
        prepared.apply_settings(&settings).unwrap();
        assert_eq!(prepared.client().policy().limits().max_concurrency, 7);
        let scope = prepared.client().policy().account_scope();
        assert!(!scope.allows(&ProviderAccountId::new("acct_other").unwrap()));
        assert!(!scope.allows(&ProviderAccountId::new("acct_xai").unwrap()));
        let provider = ProviderKind::new("openai").unwrap();
        assert!(!scope.allows_provider_model(&provider, "gpt-blocked"));
        service
            .client_model_catalog(prepared.client(), "codex", "0.163.0-alpha.10.2")
            .await
            .unwrap();
        let ordinary = service.authenticate("sk_start_test").unwrap();
        assert!(
            ordinary
                .policy()
                .account_scope()
                .allows_provider_model(&provider, "gpt-blocked")
        );
        assert!(
            ordinary
                .policy()
                .account_scope()
                .allows(&ProviderAccountId::new("acct_other").unwrap())
        );
        assert!(
            ordinary
                .policy()
                .account_scope()
                .allows(&ProviderAccountId::new("acct_xai").unwrap())
        );
    });
}

#[test]
fn next_round_reauthenticates_binding_and_scope_but_started_request_keeps_snapshot() {
    block_on(async {
        for lost_scope in [false, true] {
            let admissions = Arc::new(Admissions::default());
            let (service, snapshots) = service_for(
                snapshot(1, Some(binding(true, "acct_start")), true),
                Arc::<NativeProvider>::default(),
                admissions.clone(),
            );
            let client = authenticate(&service).await.unwrap();
            let started = service.prepare_execution(client.clone()).await.unwrap();
            snapshots.publish(snapshot(
                2,
                Some(binding(lost_scope, "acct_start")),
                !lost_scope,
            ));
            assert!(service.prepare_execution(client).await.is_err());
            let facts = service.codex_identity(started).await.unwrap();
            assert_eq!(facts.plan_type.as_deref(), Some("plus"));
            assert!(!admissions.active.load(Ordering::SeqCst));
        }
    });
}

#[test]
fn native_model_attempts_keep_required_account_and_never_use_diagnostics() {
    block_on(async {
        let provider = Arc::new(NativeProvider::default());
        provider.recover_transport.store(true, Ordering::SeqCst);
        let (service, _) = service_for(
            snapshot(1, Some(binding(true, "acct_start")), true),
            provider.clone(),
            Arc::new(Admissions::default()),
        );
        let mut started = service
            .start(StartExecution {
                client: authenticate(&service).await.unwrap(),
                public_model: PublicModelId::new("gpt-start").unwrap(),
                operation: start_operation(),
                metadata: ExecutionRequestMetadata {
                    protocol: "openai".into(),
                    endpoint: "/backend-api/codex/responses".into(),
                    transport: ClientTransport::WebSocket,
                    stream: true,
                    client_ip: None,
                    user_agent: None,
                    previous_response_id: None,
                },
            })
            .await
            .unwrap();
        assert!(started.session.next_event().await.is_err());
        assert_eq!(provider.attempts.lock().unwrap().len(), 3);
        assert!(
            provider
                .attempts
                .lock()
                .unwrap()
                .iter()
                .all(|id| id == "acct_start")
        );
    });
}

#[test]
fn auxiliary_ignores_model_budget_and_holds_concurrency_until_eof_close_or_drop() {
    block_on(async {
        for completion in ["eof", "close", "drop", "cancel"] {
            let admissions = Arc::new(Admissions::default());
            let budget = Arc::new(Budget {
                reject: true,
                active: admissions.active.clone(),
                ..Default::default()
            });
            let provider = Arc::new(NativeProvider::default());
            let (service, _) = service_for(
                snapshot(1, Some(binding(true, "acct_start")), true),
                provider.clone(),
                admissions.clone(),
            );
            let service = service.with_budget(budget.clone());
            let prepared = prepare(&service).await;
            let cancellation = prepared.cancellation();
            let response = service.codex_auxiliary(prepared, request()).await.unwrap();
            assert!(admissions.active.load(Ordering::SeqCst));
            let (_, _, _, mut body, _) = response.into_parts();
            assert!(body.next_frame().await.unwrap().is_some());
            assert!(admissions.active.load(Ordering::SeqCst));
            match completion {
                "eof" => {
                    assert!(body.next_frame().await.unwrap().is_some());
                    assert!(body.next_frame().await.unwrap().is_none());
                }
                "close" => body.close().await,
                "drop" => drop(body),
                "cancel" => {
                    cancellation.cancel();
                    assert!(body.next_frame().await.is_err());
                }
                _ => unreachable!(),
            }
            assert!(!admissions.active.load(Ordering::SeqCst));
            assert_eq!(admissions.releases.load(Ordering::SeqCst), 1);
            assert!(budget.charges.lock().unwrap().is_empty());
            assert!(
                provider.gateway.contexts.lock().unwrap()[0]
                    .cancellation()
                    .is_cancelled()
            );
        }
    });
}

#[test]
fn auxiliary_failure_is_not_replayed_and_dropped_opening_releases_admission() {
    block_on(async {
        let admissions = Arc::new(Admissions::default());
        let provider = Arc::new(NativeProvider::default());
        provider.gateway.fail.store(true, Ordering::SeqCst);
        let (service, _) = service_for(
            snapshot(1, Some(binding(true, "acct_start")), true),
            provider.clone(),
            admissions.clone(),
        );
        assert!(
            service
                .codex_auxiliary(prepare(&service).await, request())
                .await
                .is_err()
        );
        assert_eq!(provider.gateway.calls.load(Ordering::SeqCst), 1);
        assert!(!admissions.active.load(Ordering::SeqCst));
        provider.gateway.pending.store(true, Ordering::SeqCst);
        let prepared = prepare(&service).await;
        let cancellation: CancellationToken = prepared.cancellation();
        let mut call = service.codex_auxiliary(prepared, request());
        assert!(futures::poll!(call.as_mut()).is_pending());
        assert!(admissions.active.load(Ordering::SeqCst));
        drop(call);
        assert!(!admissions.active.load(Ordering::SeqCst));
        assert!(
            !cancellation.is_cancelled(),
            "outer middleware remains usable"
        );
        assert!(
            provider
                .gateway
                .contexts
                .lock()
                .unwrap()
                .last()
                .unwrap()
                .cancellation()
                .is_cancelled()
        );
    });
}

struct NoModelBudget;
impl ClientBudgetPort for NoModelBudget {
    fn admit(&self, _: ClientApiKeyId) -> BoxFuture<'_, Result<(), GatewayError>> {
        Box::pin(async { panic!("auxiliary admission must not inspect model budget") })
    }
    fn settle(&self, _: ClientBudgetCharge) -> BoxFuture<'_, Result<(), ClientBudgetError>> {
        Box::pin(async { panic!("auxiliary requests must not create model charges") })
    }
}

#[test]
fn auxiliary_queue_and_rpm_use_existing_admission_without_model_budget() {
    block_on(async {
        let admissions = Arc::new(BoundedAdmissions::default());
        let snapshot = snapshot(1, Some(binding(true, "acct_start")), true);
        let snapshot = snapshot
            .with_settings(
                &snapshot
                    .settings()
                    .clone()
                    .with_concurrency_queues(2, 0, 10),
            )
            .unwrap();
        let provider = Arc::new(NativeProvider::default());
        let (service, _) = service_for(snapshot, provider.clone(), admissions.clone());
        let service = service.with_budget(Arc::new(NoModelBudget));
        let first = service
            .codex_auxiliary(prepare(&service).await, request())
            .await
            .unwrap();
        let mut second = service.codex_auxiliary(prepare(&service).await, request());
        assert!(futures::poll!(second.as_mut()).is_pending());
        assert_eq!(provider.gateway.calls.load(Ordering::SeqCst), 1);
        first.into_parts().3.close().await;
        let second = second.await.unwrap();
        assert_eq!(provider.gateway.calls.load(Ordering::SeqCst), 2);
        second.into_parts().3.close().await;
        assert!(admissions.active.lock().unwrap().is_empty());
        admissions.rpm_exhausted.store(true, Ordering::SeqCst);
        assert!(
            service
                .codex_auxiliary(prepare(&service).await, request())
                .await
                .is_err()
        );
        assert_eq!(provider.gateway.calls.load(Ordering::SeqCst), 2);
        assert!(admissions.active.lock().unwrap().is_empty());
    });
}

#[test]
fn cancellation_during_admission_reclaims_granted_but_unobserved_lease() {
    block_on(async {
        let admissions = Arc::new(BoundedAdmissions::default());
        admissions.hold_acquisition.store(true, Ordering::SeqCst);
        let (service, _) = service_for(
            snapshot(1, Some(binding(true, "acct_start")), true),
            Arc::<NativeProvider>::default(),
            admissions.clone(),
        );
        let prepared = prepare(&service).await;
        let cancellation = prepared.cancellation();
        let mut call = service.codex_auxiliary(prepared, request());
        assert!(futures::poll!(call.as_mut()).is_pending());
        assert_eq!(admissions.active.lock().unwrap().len(), 1);
        cancellation.cancel();
        assert!(call.await.is_err());
        assert!(admissions.active.lock().unwrap().is_empty());
    });
}

struct FixedContinuation(NativeContinuationPin);
impl NativeContinuationPort for FixedContinuation {
    fn resolve<'a>(
        &'a self,
        _: &'a ClientApiKeyId,
        _: &'a PreviousResponseId,
    ) -> BoxFuture<'a, Result<Option<NativeContinuationPin>, NativeContinuationStoreError>> {
        Box::pin(async move { Ok(Some(self.0.clone())) })
    }
    fn record<'a>(
        &'a self,
        _: NativeContinuationPin,
    ) -> BoxFuture<'a, Result<(), NativeContinuationStoreError>> {
        Box::pin(async { Ok(()) })
    }
}

#[test]
fn continuation_from_another_account_is_rejected_before_admission() {
    block_on(async {
        let provider = Arc::new(NativeProvider::default());
        let service = DefaultExecutionService::new(
            RuntimeSnapshotHandle::new(snapshot(1, Some(binding(true, "acct_start")), true)),
            Arc::new(TrackingExecutionStore::default()),
            ProviderRegistry::new([provider.clone() as Arc<dyn Provider>]).unwrap(),
            Arc::new(UnusedAdmissions),
            Arc::new(FixedContinuation(NativeContinuationPin::new(
                PreviousResponseId::new("resp_other"),
                PreviousResponseId::new("resp_other"),
                ClientApiKeyId::new("key_start_test").unwrap(),
                ProviderKind::new("openai").unwrap(),
                ProviderAccountId::new("acct_other").unwrap(),
            ))),
            Arc::new(RecordingClientApiKeyUsage::default()),
            Arc::new(crate::RecordingDiagnostics::default()),
        );
        let result = service
            .start(StartExecution {
                client: authenticate(&service).await.unwrap(),
                public_model: PublicModelId::new("gpt-start").unwrap(),
                operation: start_operation(),
                metadata: ExecutionRequestMetadata {
                    protocol: "openai".into(),
                    endpoint: "/backend-api/codex/responses".into(),
                    transport: ClientTransport::WebSocket,
                    stream: true,
                    client_ip: None,
                    user_agent: None,
                    previous_response_id: Some(PreviousResponseId::new("resp_other")),
                },
            })
            .await;
        assert!(matches!(result, Err(error) if error.kind() == GatewayErrorKind::PolicyDenied));
        assert!(provider.attempts.lock().unwrap().is_empty());
    });
}

async fn start_in_flight_model(
    service: &DefaultExecutionService,
) -> gateway_core::engine::execution::StartedExecution {
    let mut started = service
        .start(StartExecution {
            client: authenticate(service).await.unwrap(),
            public_model: PublicModelId::new("gpt-start").unwrap(),
            operation: start_operation(),
            metadata: ExecutionRequestMetadata {
                protocol: "openai".into(),
                endpoint: "/backend-api/codex/responses".into(),
                transport: ClientTransport::HttpSse,
                stream: true,
                client_ip: None,
                user_agent: None,
                previous_response_id: None,
            },
        })
        .await
        .unwrap();
    assert!(started.session.next_event().await.unwrap().is_some());
    started.session.commit_downstream(Some(200)).await.unwrap();
    assert!(!started.session.is_finalized());
    started
}

#[test]
fn auxiliary_during_model_sse_is_capacity_rejected_when_shared_key_queue_is_disabled() {
    use futures::FutureExt as _;
    block_on(async {
        let provider = Arc::new(NativeProvider::default());
        provider.hold_generation.store(true, Ordering::SeqCst);
        let admissions = Arc::new(BoundedAdmissions::default());
        let snapshot = snapshot(1, Some(binding(true, "acct_start")), true);
        let snapshot = snapshot
            .with_settings(
                &snapshot
                    .settings()
                    .clone()
                    .with_concurrency_queues(0, 0, 10),
            )
            .unwrap();
        let (service, _) = service_for(snapshot, provider.clone(), admissions.clone());
        let model = start_in_flight_model(&service).await;
        assert_eq!(
            *admissions.active.lock().unwrap(),
            BTreeSet::from([model.request_id.clone()])
        );
        // 官方工具可在 SSE 终态前调用 history/notes，不能为此绕过同 Key 并发
        let result = service
            .codex_auxiliary(prepare(&service).await, request())
            .now_or_never()
            .expect("queue-disabled admission must return without waiting for model completion");
        assert!(matches!(result, Err(error) if error.kind() == GatewayErrorKind::RateLimited));
        assert_eq!(provider.gateway.calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            *admissions.active.lock().unwrap(),
            BTreeSet::from([model.request_id.clone()])
        );
        assert!(!model.session.is_finalized());
        model.session.cancel();
        model.session.detach_finalize().await;
        assert!(admissions.active.lock().unwrap().is_empty());
        let auxiliary = service
            .codex_auxiliary(prepare(&service).await, request())
            .await
            .unwrap();
        auxiliary.into_parts().3.close().await;
        assert_eq!(provider.gateway.calls.load(Ordering::SeqCst), 1);
        assert!(admissions.active.lock().unwrap().is_empty());
    });
}

#[test]
fn auxiliary_queue_tail_can_cancel_without_releasing_the_model_or_head_slot() {
    block_on(async {
        let provider = Arc::new(NativeProvider::default());
        provider.hold_generation.store(true, Ordering::SeqCst);
        let admissions = Arc::new(BoundedAdmissions::default());
        let snapshot = snapshot(1, Some(binding(true, "acct_start")), true);
        let snapshot = snapshot
            .with_settings(
                &snapshot
                    .settings()
                    .clone()
                    .with_concurrency_queues(2, 0, 10),
            )
            .unwrap();
        let (service, _) = service_for(snapshot, provider.clone(), admissions.clone());
        let model = start_in_flight_model(&service).await;
        let mut head = service.codex_auxiliary(prepare(&service).await, request());
        assert!(futures::poll!(head.as_mut()).is_pending());
        let prepared = prepare(&service).await;
        let cancellation = prepared.cancellation();
        let mut waiting = service.codex_auxiliary(prepared, request());
        assert!(futures::poll!(waiting.as_mut()).is_pending());
        assert_eq!(provider.gateway.calls.load(Ordering::SeqCst), 0);
        cancellation.cancel();
        let timeout = Box::pin(futures_timer::Delay::new(Duration::from_secs(2)));
        match futures::future::select(waiting, timeout).await {
            futures::future::Either::Left((result, _)) => {
                assert!(
                    matches!(result, Err(error) if error.kind() == GatewayErrorKind::Cancelled)
                );
            }
            futures::future::Either::Right(_) => {
                panic!("auxiliary cancellation must not wait for the model to finish")
            }
        }
        assert_eq!(
            *admissions.active.lock().unwrap(),
            BTreeSet::from([model.request_id.clone()])
        );
        assert!(!model.session.is_finalized());
        assert!(futures::poll!(head.as_mut()).is_pending());
        // 队尾位置已回收；模型和队首继续持有各自位置，后续请求可复用队尾位置
        let mut replacement = service.codex_auxiliary(prepare(&service).await, request());
        assert!(futures::poll!(replacement.as_mut()).is_pending());
        let mut overflow = service.codex_auxiliary(prepare(&service).await, request());
        assert!(matches!(
            futures::poll!(overflow.as_mut()),
            std::task::Poll::Ready(Err(error)) if error.kind() == GatewayErrorKind::ConcurrencyQueueFull
        ));
        drop(replacement);
        assert_eq!(provider.gateway.calls.load(Ordering::SeqCst), 0);
        model.session.cancel();
        model.session.detach_finalize().await;
        assert!(admissions.active.lock().unwrap().is_empty());
        let auxiliary = head.await.unwrap();
        auxiliary.into_parts().3.close().await;
        assert_eq!(provider.gateway.calls.load(Ordering::SeqCst), 1);
        assert!(admissions.active.lock().unwrap().is_empty());
    });
}
