//! 原生 Codex 的真实身份投影与有限辅助请求交付，不参与模型调度或业务重放

use std::sync::Arc;

use bytes::Bytes;
use futures::{future::BoxFuture, lock::OwnedMutexGuard};
use gateway_core::{
    account::{ProviderAccount, ProviderAccountId, scope::FrozenAccountScope},
    engine::{
        codex::{
            CodexAuthorizedContext, CodexAuxiliaryRequest, CodexGateway, CodexIdentity,
            CodexSessionAuthorization,
        },
        codex_history::CODEX_ROOT_AGENT_NAME,
        codex_notes::{CodexNotesDomain, CodexNotesError, CodexNotesSessionExpectation},
        middleware::{
            MiddlewareBody, MiddlewareError, MiddlewareFrame, MiddlewareFraming, MiddlewareHeader,
            MiddlewareResponse,
        },
    },
    error::{ProviderDiagnostic, ProviderError, ProviderErrorKind},
    lifecycle::{CancellationToken, Deadline},
    policy::ClientApiKeyId,
    provider_ports::{
        CodexNotesOperationKind, CodexNotesPublishOutcome, CodexSessionClaim,
        CodexSessionClaimOutcome, ProviderSessionAffinityKey,
    },
    upstream::UpstreamSendState,
};
// 池会话身份在协议层判定，与 transport 的同名 CodexSessionIdentity 必须别名区分
use gateway_protocol::openai::CodexSessionIdentity as PoolSessionIdentity;
use secrecy::ExposeSecret as _;
use serde_json::{Map, Value};
use tokio::sync::oneshot;

use super::{
    CodexProvider,
    failure::{map_client_error, pool_session_error, provider_error},
    notes_delivery::{
        NOTES_PREPARE_TIMEOUT, NotesDeliveryEntry, NotesDeliveryWork, lease_covers_delivery,
    },
    observation::map_selection_error,
};
use crate::{
    buffered_body::BufferedBody,
    credential::{
        CodexCredentialLease, CodexOAuthSecret, CodexSelectionModel, NotesLeaseLifecycle,
        SelectCodexProviderEndpointCredential, derive_pool_session_affinity, oauth_metadata,
        retry_account_snapshot,
    },
    native_history::{history_tool, local_history_response},
    native_notes::{
        declared_agent_name, is_notes_operation, map_outbound_domain, notes_body_error,
        replay_request, write_kind,
    },
    request_identity::pool_session_identity,
    transport::{CodexClientError, CodexRequestContext},
};

impl CodexGateway for CodexProvider {
    fn identity<'a>(
        &'a self,
        context: &'a CodexAuthorizedContext,
    ) -> BoxFuture<'a, Result<CodexIdentity, ProviderError>> {
        Box::pin(async move {
            tokio::select! {
                biased;
                () = context.cancellation().cancelled() => Err(provider_error(ProviderErrorKind::Cancelled, UpstreamSendState::NotSent)),
                () = context.deadline().wait() => Err(provider_error(ProviderErrorKind::Timeout, UpstreamSendState::NotSent)),
                result = async {
                    // 身份投影与账号级辅助固定使用入口账号，session_pool 不改变它
                    let account_id = context.entry_account_id();
                    let (account, runtime) = self.selector.native_credential(context.account_scope(), account_id).await.map_err(map_selection_error)?;
                    let secret = runtime.authentication.oauth().ok_or_else(invalid_identity)?;
                    self.native_identity(&account, secret).await
                } => result,
            }
        })
    }

    fn auxiliary<'a>(
        &'a self,
        context: &'a CodexAuthorizedContext,
        request: CodexAuxiliaryRequest,
    ) -> BoxFuture<'a, Result<MiddlewareResponse, ProviderError>> {
        Box::pin(async move {
            // 从读取凭据到取得响应头都受同一取消和期限约束；不创建后台请求任务
            let outcome = tokio::select! {
                biased;
                () = context.cancellation().cancelled() => return Err(provider_error(ProviderErrorKind::Cancelled, UpstreamSendState::Ambiguous)),
                () = context.deadline().wait() => return Err(provider_error(ProviderErrorKind::Timeout, UpstreamSendState::Ambiguous)),
                result = async {
                    // history 四工具在读取凭据前分流：本地 history 不需要账号、whoami 或推理额度
                    if let Some(local) = self.local_history(context, &request).await? {
                        return Ok(AuxiliaryOutcome::Ready(local));
                    }
                    // notes 六端点按会话已发布落点交付，写类还要记录顺序与原始响应
                    if self.notes_applicable(context, &request) {
                        let response = self.native_notes_auxiliary(context, request).await?;
                        return Ok(AuxiliaryOutcome::Ready(response));
                    }
                    // 账号解析（含 session_pool 首绑）先于发送；固定账号仍只做资格与凭据复验
                    let target = self.auxiliary_target(context, &request, None).await?;
                    let response = self.client.for_account(&target.account)
                        .map_err(|error| map_client_error(error, UpstreamSendState::NotSent, false).error)?
                        .native_auxiliary(request, CodexRequestContext::auxiliary(
                            &target.authorization, Some(&target.identity.account_id), "codex_auxiliary", None,
                        ), target.identity.is_fedramp).await
                        .map_err(|error| map_client_error(error, UpstreamSendState::Ambiguous, false).error)?;
                    Ok(AuxiliaryOutcome::Upstream {
                        response,
                        lease: target.lease,
                    })
                } => result?,
            };
            match outcome {
                AuxiliaryOutcome::Ready(response) => Ok(response),
                AuxiliaryOutcome::Upstream { response, lease } => {
                    Ok(self.forwarded_response(context, response, lease))
                }
            }
        })
    }
}

impl CodexProvider {
    /// notes 六端点是否由已发布副本处理
    ///
    /// fixed 与普通 /v1 不改变现有转发；未装配协调服务时同样保持原行为
    fn notes_applicable(
        &self,
        context: &CodexAuthorizedContext,
        request: &CodexAuxiliaryRequest,
    ) -> bool {
        self.native_notes.is_some()
            && context.session_pool()
            && is_notes_operation(request.operation)
    }

    /// 按会话已发布 notes 落点交付一次 notes 调用
    ///
    /// 写类先占用交付容量，再取得会话互斥并在锁内复读落点，最后决定写入目标；
    /// 读类只取一次快照，不参与互斥、容量与交付租约
    async fn native_notes_auxiliary(
        &self,
        context: &CodexAuthorizedContext,
        request: CodexAuxiliaryRequest,
    ) -> Result<MiddlewareResponse, ProviderError> {
        let service = self.native_notes.as_deref().ok_or_else(|| {
            provider_error(ProviderErrorKind::Unavailable, UpstreamSendState::NotSent)
        })?;
        let affinity = self.auxiliary_session_affinity(context, &request)?;
        let session_key = affinity.key().clone();
        let kind = write_kind(request.operation);
        // 锁外探测只用于分流：决定本次写是否占用交付容量。已知 legacy 写沿既有转发，
        // 未认领会话按同一准入规则判断认领后是否会记录，不另建第二套来源判定
        let probe = match kind {
            Some(_) => service
                .domain(context.client_api_key_id(), &session_key)
                .await
                .map_err(|error| notes_coordination_error(error, UpstreamSendState::NotSent))?,
            None => None,
        };
        let records_write = kind.filter(|_| {
            probe.as_ref().map_or(
                service.records_new_sessions(),
                CodexNotesDomain::records_operations,
            )
        });
        // 容量必须先于会话互斥等待：同会话等待者同样计入在途上限，第 N+1 个立即拒绝
        let entry = match records_write {
            Some(_) => Some(
                self.notes_delivery
                    .as_deref()
                    .ok_or_else(notes_delivery_error)?
                    .begin()?,
            ),
            None => None,
        };
        // 写类在整个发送与落库生命周期内持有会话互斥；读类不参与。
        // 交接后互斥随工作一起交给交付 owner，不能随调用方结束释放
        let guard = match kind {
            Some(_) => Some(
                service
                    .lock_session(context.client_api_key_id(), &session_key)
                    .await,
            ),
            None => None,
        };
        // 锁内复读落点；锁外探测不参与发送决策，避免用旧 owner 写回其它域
        let domain = service
            .domain(context.client_api_key_id(), &session_key)
            .await
            .map_err(|error| notes_coordination_error(error, UpstreamSendState::NotSent))?;
        // 锁内已确认 legacy 时，交付接管不会发生：在申请交付租约之前就释放容量，
        // 本次请求按调用方期限与取消走既有转发
        let entry = entry.filter(|_| {
            domain
                .as_ref()
                .is_none_or(CodexNotesDomain::records_operations)
        });
        // 只有确实要由交付 owner 接管的写才申请交付租约与强制准备窗口
        let takeover = entry.is_some();
        let lease = entry.as_ref().map(NotesDeliveryEntry::lease);
        // 同一份业务准备：接管的写受有界准备窗口约束，读类与 legacy 沿调用方原预算
        let prepare = async {
            // 账号租约续期必须属于交付生命周期：调用方退出后仍要约束账号容量
            let target = self
                .auxiliary_target(context, &request, lease.clone())
                .await?;
            // 首次 notes 调用由目标解析完成认领；随后复读才能看到新会话状态
            let domain = match domain {
                Some(domain) => domain,
                None => service
                    .domain(context.client_api_key_id(), &session_key)
                    .await
                    .map_err(|error| notes_coordination_error(error, UpstreamSendState::NotSent))?
                    .ok_or_else(|| {
                        notes_coordination_error(
                            CodexNotesError::SessionAbsent,
                            UpstreamSendState::NotSent,
                        )
                    })?,
            };
            // 读类不持互斥，读 owner 与读 notes 域之间可能刚好发布切换；
            // 最终凭据必须与所读域属于同一账号，不能用旧 owner 的凭据访问新域
            let target = if target.account.id() == domain.replica.account() {
                target
            } else {
                self.unleased_auxiliary_target(context.account_scope(), domain.replica.account())
                    .await?
            };
            let body = map_outbound_domain(&request.body, domain.replica.generation())?;
            Ok::<_, ProviderError>((target, domain, body))
        };
        let prepared = if takeover {
            tokio::time::timeout_at(tokio::time::Instant::now() + NOTES_PREPARE_TIMEOUT, prepare)
                .await
                .map_err(|_| notes_prepare_timeout())??
        } else {
            prepare.await?
        };
        let (target, domain, body) = prepared;
        // 首绑异步准备后才判定为 legacy，却已经持有交付专用租约：交付接管不成立，
        // 租约期限也无法恢复成调用方生命周期，因此在任何意图与发送之前保护性拒绝
        if takeover && !domain.records_operations() && target.lease.is_some() {
            return Err(notes_source_changed_before_handoff());
        }
        // 交接前复验：申请期限的剩余量必须覆盖完整交付预算；不覆盖就不写意图也不发送
        if target.lease.is_some()
            && let Some(lease) = lease.as_ref()
            && !lease_covers_delivery(lease)
        {
            return Err(notes_lease_not_covering_delivery());
        }
        // 最终不由交付接管时释放容量并沿既有转发
        let entry = entry.filter(|_| domain.records_operations());
        let call = NotesCall {
            context,
            key_id: context.client_api_key_id().clone(),
            session_key,
            domain,
            target,
            request,
            body,
            guard,
            entry,
        };
        // legacy 会话沿既有原生域转发，不新增日志，也不把旧状态迁到新域
        let records = call.domain.records_operations();
        match kind {
            Some(kind) if records => self.notes_write(call, kind).await,
            _ => self.notes_passthrough(call).await,
        }
    }

    /// 读类与 legacy 写类：域映射后单次发送，正文沿既有惰性生命周期交付
    async fn notes_passthrough(
        &self,
        call: NotesCall<'_>,
    ) -> Result<MiddlewareResponse, ProviderError> {
        let NotesCall {
            context,
            target,
            mut request,
            body,
            ..
        } = call;
        request.body = body;
        let AuxiliaryTarget {
            account,
            identity,
            authorization,
            lease,
        } = target;
        let response = self
            .client
            .for_account(&account)
            .map_err(|error| map_client_error(error, UpstreamSendState::NotSent, false).error)?
            .native_auxiliary(
                request,
                CodexRequestContext::auxiliary(
                    &authorization,
                    Some(&identity.account_id),
                    "codex_auxiliary",
                    None,
                ),
                identity.is_fedramp,
            )
            .await
            .map_err(|error| map_client_error(error, UpstreamSendState::Ambiguous, false).error)?;
        Ok(self.forwarded_response(context, response, lease))
    }

    /// 托管写类：发送前完成全部可确定准备，再把交付整体交给受监督 owner
    ///
    /// 交接成功即所有权转移：调用方取消或 future 被丢弃只停止等待结果，
    /// append、start、发送、完整读取响应与落库仍由交付 owner 持有
    async fn notes_write(
        &self,
        call: NotesCall<'_>,
        kind: CodexNotesOperationKind,
    ) -> Result<MiddlewareResponse, ProviderError> {
        let NotesCall {
            context: _,
            key_id,
            session_key,
            domain,
            target,
            mut request,
            body,
            guard,
            entry,
        } = call;
        let delivery = self
            .notes_delivery
            .as_deref()
            .ok_or_else(notes_delivery_error)?;
        let service = self
            .native_notes
            .as_ref()
            .ok_or_else(notes_delivery_error)?;
        let entry = entry.ok_or_else(notes_delivery_error)?;
        let guard = guard.ok_or_else(notes_delivery_error)?;
        // 日志保存客户端原始字节；agent 只用于按 agent 解释顺序与路径
        let agent_name =
            declared_agent_name(&request.body).unwrap_or_else(|| CODEX_ROOT_AGENT_NAME.to_owned());
        let stored = std::str::from_utf8(&request.body)
            .map_err(|_| notes_body_error())?
            .to_owned();
        let AuxiliaryTarget {
            account,
            identity,
            authorization,
            lease,
        } = target;
        request.body = body;
        let client = self
            .client
            .for_account(&account)
            .map_err(|error| map_client_error(error, UpstreamSendState::NotSent, false).error)?;
        // 可确定的头、地址与正文构造错误必须在记录写意图之前暴露
        let prepared = client
            .prepare_native_auxiliary(
                request,
                CodexRequestContext::auxiliary(
                    &authorization,
                    Some(&identity.account_id),
                    "codex_auxiliary",
                    None,
                ),
                identity.is_fedramp,
            )
            .map_err(|error| map_client_error(error, UpstreamSendState::NotSent, false).error)?;
        let (completion, result) = oneshot::channel();
        let NotesDeliveryEntry { permit, lifecycle } = entry;
        delivery.submit(NotesDeliveryWork {
            completion,
            permit,
            lifecycle,
            service: Arc::clone(service),
            guard,
            key_id,
            session_key,
            replica: domain.replica,
            kind,
            agent_name,
            stored,
            prepared,
            lease,
        })?;
        // 接收端消失只表示调用方离开；工作本身已经由 owner 观察并落库
        result.await.map_err(|_| notes_delivery_error())?
    }

    /// 上游响应投影为下游惰性正文；租约随正文生命周期释放
    fn forwarded_response(
        &self,
        context: &CodexAuthorizedContext,
        response: reqwest::Response,
        lease: Option<Box<CodexCredentialLease>>,
    ) -> MiddlewareResponse {
        MiddlewareResponse::new(
            "openai".to_owned(),
            response.status().as_u16(),
            forwardable_headers(&response),
            Box::new(NativeBody {
                response: Some(response),
                // 辅助首绑取得的账号租约随正文生命周期释放，不能提前结束占用
                _lease: lease,
                cancellation: context.cancellation().clone(),
                deadline: context.deadline(),
            }),
        )
    }

    /// 将会话 notes 重建到目标账号的新域并发布
    ///
    /// 只由模型 attempt 在选出目标账号、发出模型请求之前调用；重放与进度推进
    /// 在本方法内完成，发布由 Core 以原代次 CAS 裁决，失败不发布也不重试 append
    /// 授权来自同一 attempt 的 Key、账号范围、取消与期限，不伪造入口身份
    ///
    /// # Errors
    ///
    /// 未装配 notes 协调、目标账号不可用、日志存在未终结操作、重放失败、
    /// 发布事务失败或 attempt 取消/超时
    pub async fn rebuild_notes(
        &self,
        authorization: &CodexSessionAuthorization,
        session_key: &ProviderSessionAffinityKey,
        target: &ProviderAccountId,
        credential_revision: u64,
        expected: &CodexNotesSessionExpectation,
    ) -> Result<CodexNotesPublishOutcome, ProviderError> {
        tokio::select! {
            biased;
            () = authorization.cancellation().cancelled() => Err(provider_error(
                ProviderErrorKind::Cancelled,
                UpstreamSendState::NotSent,
            )),
            () = authorization.deadline().wait() => Err(provider_error(
                ProviderErrorKind::Timeout,
                UpstreamSendState::NotSent,
            )),
            result = self.rebuild_notes_inner(
                authorization,
                session_key,
                target,
                credential_revision,
                expected,
            ) => result,
        }
    }

    async fn rebuild_notes_inner(
        &self,
        authorization: &CodexSessionAuthorization,
        session_key: &ProviderSessionAffinityKey,
        target: &ProviderAccountId,
        credential_revision: u64,
        expected: &CodexNotesSessionExpectation,
    ) -> Result<CodexNotesPublishOutcome, ProviderError> {
        let service = self.native_notes.as_deref().ok_or_else(|| {
            provider_error(ProviderErrorKind::Unavailable, UpstreamSendState::NotSent)
        })?;
        // 正常写入与重建共用同一会话互斥，避免重建期间旧写入落到旧域
        let _guard = service
            .lock_session(authorization.client_api_key_id(), session_key)
            .await;
        let target_account = self
            .unleased_auxiliary_target(authorization.account_scope(), target)
            .await?;
        // 准备阶段看到的身份已变化时重读状态重新准备，不用旧凭据重放
        if target_account.account.revision().get() != credential_revision {
            return Ok(CodexNotesPublishOutcome::IdentityChanged);
        }
        // 锁内、保存重放目标之前复验选号时的 owner 与代次；过期意图零重放
        let mut rebuild = service
            .begin_rebuild(
                authorization.client_api_key_id(),
                session_key,
                target,
                expected,
            )
            .await
            .map_err(stale_migration_error)?;
        let generation = rebuild.generation.clone();
        let client = self
            .client
            .for_account(&target_account.account)
            .map_err(|error| map_client_error(error, UpstreamSendState::NotSent, false).error)?;
        // 步骤副本与冻结目标一致：重放期间同时推进 &mut 进度
        let steps = rebuild.steps.clone();
        for step in &steps {
            let request = replay_request(&generation, step)?;
            let response = client
                .native_auxiliary(
                    request,
                    CodexRequestContext::auxiliary(
                        &target_account.authorization,
                        Some(&target_account.identity.account_id),
                        "codex_auxiliary",
                        None,
                    ),
                    target_account.identity.is_fedramp,
                )
                .await
                .map_err(|error| {
                    map_client_error(error, UpstreamSendState::Ambiguous, false).error
                })?;
            let status = response.status();
            if !status.is_success() {
                // 明确失败就停止：不发布半套副本，也不重试同一步
                tracing::warn!(
                    seq = step.seq,
                    upstream_status_code = status.as_u16(),
                    "Codex notes replay was rejected by the upstream"
                );
                return Err(provider_error(
                    ProviderErrorKind::Unavailable,
                    UpstreamSendState::NotSent,
                )
                .with_status(status.as_u16())
                .with_retry_prohibited()
                .with_diagnostic(
                    ProviderDiagnostic::new("Codex notes replay was rejected by the upstream")
                        .with_classification("notes", "replay_rejected"),
                ));
            }
            // 只有完整读到响应才推进冻结进度
            response.bytes().await.map_err(|error| {
                map_client_error(
                    CodexClientError::HttpJson(error),
                    UpstreamSendState::Ambiguous,
                    false,
                )
                .error
            })?;
            service
                .advance_rebuild(
                    authorization.client_api_key_id(),
                    session_key,
                    &mut rebuild,
                    step.seq,
                )
                .await
                .map_err(|error| notes_coordination_error(error, UpstreamSendState::NotSent))?;
        }
        service
            .publish_rebuild(
                authorization.client_api_key_id(),
                session_key,
                &rebuild,
                credential_revision,
            )
            .await
            .map_err(|error| notes_coordination_error(error, UpstreamSendState::NotSent))
    }
}

impl CodexProvider {
    /// 本地回答 history 四工具
    ///
    /// 只有已装配原生 history 且 session_pool 的会话才可能本地回答；
    /// legacy 与来源未判定会话返回 None，继续既有上游行为
    async fn local_history(
        &self,
        context: &CodexAuthorizedContext,
        request: &CodexAuxiliaryRequest,
    ) -> Result<Option<MiddlewareResponse>, ProviderError> {
        let Some(service) = self.native_history.as_deref() else {
            return Ok(None);
        };
        if !context.session_pool() || history_tool(request.operation).is_none() {
            return Ok(None);
        }
        let affinity = self.auxiliary_session_affinity(context, request)?;
        local_history_response(
            service,
            context.client_api_key_id(),
            affinity.key(),
            request.operation,
            &request.headers,
            &request.body,
        )
        .await
    }

    /// 解析辅助请求的执行账号
    ///
    /// fixed 只复验入口账号；session_pool 与模型请求共用持久归属，首次先到时可完成认领
    async fn auxiliary_target(
        &self,
        context: &CodexAuthorizedContext,
        request: &CodexAuxiliaryRequest,
        lease_lifecycle: Option<NotesLeaseLifecycle>,
    ) -> Result<AuxiliaryTarget, ProviderError> {
        // fixed 与账号级辅助请求（usage/profile/settings 等）始终使用入口账号
        if !context.session_pool() || !request.operation.follows_codex_session() {
            let account_id = context.entry_account_id().clone();
            return self
                .unleased_auxiliary_target(context.account_scope(), &account_id)
                .await;
        }
        let lease_lifecycle = lease_lifecycle.as_ref();
        let affinity = self.auxiliary_session_affinity(context, request)?;
        let attempt = context.selection().ok_or_else(|| {
            provider_error(ProviderErrorKind::Unavailable, UpstreamSendState::NotSent)
        })?;
        // 与模型选号共用有界快照预算：账号行锁内发现凭据替换时重读归属、
        // 重新选号并重新认领，正常刷新不会让首绑永久失败
        let mut snapshot_retries = 0;
        loop {
            if let Some(owner) = self
                .selector
                .load_session_owner(context.client_api_key_id(), affinity.key())
                .await
                .map_err(map_selection_error)?
            {
                // 已有归属只使用 owner，不因模型额度或池内其他账号重新选号
                return self
                    .unleased_auxiliary_target(context.account_scope(), &owner)
                    .await;
            }
            // 辅助请求可能先于模型请求到达；复用模型选号规则完成原子认领，不产生模型费用
            let lease = self
                .selector
                .select_for_provider_endpoint(&SelectCodexProviderEndpointCredential {
                    // 辅助端点与 Responses 同源，URL 只参与 cookie 适用域判断
                    request_url: &self.responses_url,
                    attempt,
                    session_affinity: Some(&affinity),
                    model: CodexSelectionModel::Search(None),
                    requires_oauth: true,
                    // 首绑取得的账号租约随 notes 交付结束，而不是随调用方请求结束
                    lease_lifecycle,
                })
                .await
                .map_err(map_selection_error)?;
            let claim = CodexSessionClaim {
                key_id: context.client_api_key_id().clone(),
                session_key: affinity.key().clone(),
                account_id: lease.account_id().clone(),
                credential_revision: lease.account().revision().get(),
            };
            match self
                .selector
                .claim_session_owner(&claim)
                .await
                .map_err(map_selection_error)?
            {
                CodexSessionClaimOutcome::Owned => {
                    return self.leased_auxiliary_target(lease).await;
                }
                CodexSessionClaimOutcome::ConflictingOwner(owner) => {
                    // 并发首请求已有赢家；释放临时租约后按该 owner 执行
                    drop(lease);
                    return self
                        .unleased_auxiliary_target(context.account_scope(), &owner)
                        .await;
                }
                CodexSessionClaimOutcome::IdentityChanged => {
                    // 选号快照在账号行锁内已失效，释放临时租约后重跑整套首绑
                    let account = lease.account().clone();
                    drop(lease);
                    retry_account_snapshot(attempt, &account, &mut snapshot_retries)
                        .map_err(map_selection_error)?;
                }
                CodexSessionClaimOutcome::Unavailable => {
                    drop(lease);
                    return Err(provider_error(
                        ProviderErrorKind::NoEligibleAccount,
                        UpstreamSendState::NotSent,
                    ));
                }
            }
        }
    }

    /// session_pool 的辅助会话键；与模型请求共用同一解析与派生入口
    fn auxiliary_session_affinity(
        &self,
        context: &CodexAuthorizedContext,
        request: &CodexAuxiliaryRequest,
    ) -> Result<crate::credential::CodexSessionAffinity, ProviderError> {
        // 辅助请求没有连接上下文，只按最终请求头与正文声明解释逻辑会话
        let body = serde_json::from_slice::<Map<String, Value>>(&request.body)
            .map_err(|_| pool_session_error("session_key_invalid"))?;
        match pool_session_identity(&body, &Map::new(), &request.headers, None) {
            PoolSessionIdentity::Resolved(session_id) => {
                derive_pool_session_affinity(&session_id, context.client_api_key_id())
                    .ok_or_else(|| pool_session_error("session_key_invalid"))
            }
            PoolSessionIdentity::Missing => Err(pool_session_error("session_key_missing")),
            PoolSessionIdentity::Conflict => Err(pool_session_error("session_key_conflict")),
        }
    }

    async fn unleased_auxiliary_target(
        &self,
        scope: &FrozenAccountScope,
        account_id: &ProviderAccountId,
    ) -> Result<AuxiliaryTarget, ProviderError> {
        let (account, runtime) = self
            .selector
            .native_credential(scope, account_id)
            .await
            .map_err(map_selection_error)?;
        let secret = runtime
            .authentication
            .oauth()
            .ok_or_else(invalid_identity)?;
        let identity = self.native_identity(&account, secret).await?;
        let authorization = runtime
            .authentication
            .authorization_header()
            .map_err(|_| invalid_identity())?
            .expose_secret()
            .to_owned();
        Ok(AuxiliaryTarget {
            account,
            identity,
            authorization,
            lease: None,
        })
    }

    async fn leased_auxiliary_target(
        &self,
        lease: CodexCredentialLease,
    ) -> Result<AuxiliaryTarget, ProviderError> {
        let secret = lease
            .authentication()
            .oauth()
            .ok_or_else(invalid_identity)?;
        let identity = self.native_identity(lease.account(), secret).await?;
        let authorization = lease
            .authentication()
            .authorization_header()
            .map_err(|_| invalid_identity())?
            .expose_secret()
            .to_owned();
        Ok(AuxiliaryTarget {
            account: lease.account().clone(),
            identity,
            authorization,
            lease: Some(Box::new(lease)),
        })
    }

    async fn native_identity(
        &self,
        account: &ProviderAccount,
        secret: &CodexOAuthSecret,
    ) -> Result<CodexIdentity, ProviderError> {
        if secret
            .access_token
            .expose_secret()
            .trim()
            .starts_with("at-")
        {
            // PAT 没有可解析的 JWT；沿用官方 whoami 与当前账号出口取得真实事实
            let metadata = self
                .native_token_client
                .as_ref()
                .ok_or_else(invalid_identity)?
                .personal_access_token_metadata(
                    secret.access_token.expose_secret(),
                    account.outbound_proxy(),
                )
                .await
                .map_err(|error| invalid_identity().with_source(error))?;
            return Ok(CodexIdentity {
                user_id: metadata.chatgpt_user_id.ok_or_else(invalid_identity)?,
                account_id: metadata.chatgpt_account_id.ok_or_else(invalid_identity)?,
                email: metadata.email,
                plan_type: metadata.chatgpt_plan_type,
                is_fedramp: metadata.chatgpt_account_is_fedramp.unwrap_or(false),
            });
        }
        // 身份投影随凭据事务写入；正常 RT 刷新保留该事实，不新增身份网络请求
        let metadata = oauth_metadata(secret);
        Ok(CodexIdentity {
            user_id: account
                .upstream_user_id()
                .ok_or_else(invalid_identity)?
                .to_owned(),
            account_id: account
                .upstream_account_id()
                .ok_or_else(invalid_identity)?
                .to_owned(),
            email: account.email().map(str::to_owned),
            plan_type: account.plan_type().map(str::to_owned),
            is_fedramp: metadata.chatgpt_account_is_fedramp.unwrap_or(false),
        })
    }
}

fn invalid_identity() -> ProviderError {
    provider_error(ProviderErrorKind::Unavailable, UpstreamSendState::NotSent)
}

/// 辅助请求的两种完成方式：已就绪响应或待惰性交付的上游响应
enum AuxiliaryOutcome {
    /// 本地 history 结果、notes 写响应等已完整取得的结果
    Ready(MiddlewareResponse),
    Upstream {
        response: reqwest::Response,
        lease: Option<Box<CodexCredentialLease>>,
    },
}

/// 辅助请求解析出的执行账号；租约只在 session_pool 首绑时存在
struct AuxiliaryTarget {
    account: ProviderAccount,
    identity: CodexIdentity,
    authorization: String,
    lease: Option<Box<CodexCredentialLease>>,
}

/// 一次 notes 调用已解析的会话事实
struct NotesCall<'a> {
    context: &'a CodexAuthorizedContext,
    key_id: ClientApiKeyId,
    session_key: ProviderSessionAffinityKey,
    domain: CodexNotesDomain,
    target: AuxiliaryTarget,
    /// 客户端原始请求；日志按原字节保存，发送前只替换出站草稿的正文
    request: CodexAuxiliaryRequest,
    body: Bytes,
    /// 写类在整个交付期间持有的会话互斥；读类为 None
    guard: Option<OwnedMutexGuard<()>>,
    /// 记录日志的写类占用的交付容量与生命周期；legacy 写为 None
    entry: Option<NotesDeliveryEntry>,
}

/// 准备阶段超时：仍在写意图与发送之前失败
fn notes_prepare_timeout() -> ProviderError {
    provider_error(ProviderErrorKind::Timeout, UpstreamSendState::NotSent)
        .with_retry_prohibited()
        .with_diagnostic(
            ProviderDiagnostic::new("Codex notes preparation did not complete in time")
                .with_classification("notes", "prepare_timeout"),
        )
}

/// 首绑异步准备后才判定为 legacy，而账号租约已经按交付生命周期取得
///
/// 租约期限无法窄化回调用方生命周期，交付接管也不成立：在任何写意图与发送之前拒绝，
/// 释放容量、互斥与租约，让后续请求按已知 legacy 路径正常准备
fn notes_source_changed_before_handoff() -> ProviderError {
    provider_error(ProviderErrorKind::Unavailable, UpstreamSendState::NotSent)
        .with_retry_prohibited()
        .with_diagnostic(
            ProviderDiagnostic::new(
                "Codex notes session switched to legacy after the delivery lease was taken",
            )
            .with_classification("notes", "source_changed_before_handoff"),
        )
}

/// 账号租约剩余量已不能覆盖完整交付，按保护性失败处理而不是带着过期租约发送
fn notes_lease_not_covering_delivery() -> ProviderError {
    provider_error(ProviderErrorKind::Unavailable, UpstreamSendState::NotSent)
        .with_retry_prohibited()
        .with_diagnostic(
            ProviderDiagnostic::new("Codex notes account lease no longer covers the delivery")
                .with_classification("notes", "lease_not_covering_delivery"),
        )
}

/// 未装配交付 owner 时不能把真实写入降级成无观察的转发
fn notes_delivery_error() -> ProviderError {
    provider_error(ProviderErrorKind::Unavailable, UpstreamSendState::NotSent)
        .with_retry_prohibited()
        .with_diagnostic(
            ProviderDiagnostic::new("Codex notes delivery is not assembled")
                .with_classification("notes", "delivery_unavailable"),
        )
}

/// 上游响应头投影到下游；只保留正文类型与官方允许转发的字段
pub(super) fn forwardable_headers(response: &reqwest::Response) -> Vec<MiddlewareHeader> {
    let connection_options = response
        .headers()
        .get_all("connection")
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .map(|value| value.trim().to_ascii_lowercase())
        .collect::<Vec<_>>();
    response
        .headers()
        .iter()
        .filter(|(name, _)| {
            name.as_str() == "content-type"
                || gateway_protocol::openai::response_header_is_forwardable(
                    name.as_str(),
                    &connection_options,
                )
        })
        .map(|(name, value)| {
            MiddlewareHeader::new(name.as_str(), Bytes::copy_from_slice(value.as_bytes()))
        })
        .collect()
}

/// 已完整读取的上游响应；正文只交付原始字节，content-type 由响应头声明
pub(super) fn buffered_response(
    status: u16,
    headers: Vec<MiddlewareHeader>,
    body: Bytes,
) -> MiddlewareResponse {
    MiddlewareResponse::new(
        "openai".to_owned(),
        status,
        headers,
        Box::new(BufferedBody::new(body)),
    )
}

/// 重建入口的过期意图复用已有冲突语义：拒绝本次旧 attempt，不发模型请求
fn stale_migration_error(error: CodexNotesError) -> ProviderError {
    if matches!(error, CodexNotesError::MigrationStale) {
        return provider_error(ProviderErrorKind::Unavailable, UpstreamSendState::NotSent)
            .with_retry_prohibited()
            .with_diagnostic(
                ProviderDiagnostic::new("Codex notes migration intent is stale")
                    .with_classification("notes", "replay_generation_changed"),
            );
    }
    notes_coordination_error(error, UpstreamSendState::NotSent)
}

/// notes 协调失败映射为稳定错误
///
/// 会话状态不完整、日志未终结与重放越序都不允许改用其它账号重试
pub(super) fn notes_coordination_error(
    error: CodexNotesError,
    send_state: UpstreamSendState,
) -> ProviderError {
    let code = match &error {
        CodexNotesError::Store(_) => "store_unavailable",
        CodexNotesError::SessionAbsent => "session_absent",
        CodexNotesError::LegacySession => "legacy_session",
        CodexNotesError::OperationUnsettled { .. } => "operation_unsettled",
        CodexNotesError::ReplayOutOfOrder => "replay_out_of_order",
        CodexNotesError::ReplayNotComplete => "replay_not_complete",
        CodexNotesError::MigrationStale => "migration_stale",
    };
    provider_error(ProviderErrorKind::Unavailable, send_state)
        .with_retry_prohibited()
        .with_diagnostic(
            ProviderDiagnostic::new("Codex notes coordination is unavailable")
                .with_classification("notes", code),
        )
}

struct NativeBody {
    response: Option<reqwest::Response>,
    _lease: Option<Box<CodexCredentialLease>>,
    cancellation: CancellationToken,
    deadline: Deadline,
}

impl MiddlewareBody for NativeBody {
    fn next_frame(&mut self) -> BoxFuture<'_, Result<Option<MiddlewareFrame>, MiddlewareError>> {
        Box::pin(async move {
            let Some(response) = self.response.as_mut() else {
                return Ok(None);
            };
            let result = tokio::select! {
                biased;
                () = self.cancellation.cancelled() => Err(provider_error(ProviderErrorKind::Cancelled, UpstreamSendState::Sent)),
                () = self.deadline.wait() => Err(provider_error(ProviderErrorKind::Timeout, UpstreamSendState::Sent)),
                result = response.chunk() => result.map_err(|error| map_client_error(CodexClientError::HttpJson(error), UpstreamSendState::Sent, false).error),
            };
            match result {
                Ok(Some(bytes)) => Ok(Some(MiddlewareFrame::new(
                    bytes,
                    MiddlewareFraming::RawBytes,
                    false,
                ))),
                Ok(None) => {
                    self.response.take();
                    Ok(None)
                }
                Err(error) => {
                    self.response.take();
                    Err(MiddlewareError::Provider(error))
                }
            }
        })
    }

    fn close(self: Box<Self>) -> BoxFuture<'static, ()> {
        Box::pin(async move {
            drop(self);
        })
    }
}
