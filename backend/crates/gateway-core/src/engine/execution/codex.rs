//! 原生 Codex 辅助调用的授权、Key 准入与惰性正文租约生命周期

use std::{collections::BTreeSet, num::NonZeroU32, sync::Arc};

use futures::{FutureExt as _, future::BoxFuture, pin_mut, select_biased};

use super::{
    DefaultExecutionService, PreparedRootExecution,
    admission::{AdmissionLease, AdmissionPurpose},
    failure::authentication_gateway_error,
};
use crate::{
    concurrency::ConcurrencyWaitBudget,
    engine::{
        AccountAttemptContext, AttemptContext, RequestAttemptContext,
        authentication::ClientAuthenticationPurpose,
        codex::{CodexAuthorizedContext, CodexAuxiliaryRequest, CodexGateway, CodexIdentity},
        execution::ClientTransport,
        middleware::{MiddlewareBody, MiddlewareError, MiddlewareFrame, MiddlewareResponse},
    },
    error::{GatewayError, GatewayErrorKind},
    lifecycle::{CancellationToken, Deadline},
    policy::CodexAccessMode,
};

impl DefaultExecutionService {
    async fn prepare_codex_call(
        &self,
        mut prepared: PreparedRootExecution,
    ) -> Result<
        (
            Arc<dyn CodexGateway>,
            CodexAuthorizedContext,
            CodexCallLifetime,
        ),
        GatewayError,
    > {
        if prepared.client.authentication_purpose() != ClientAuthenticationPurpose::Codex {
            return Err(GatewayError::new(
                GatewayErrorKind::PolicyDenied,
                "Codex authorization is required",
            ));
        }
        prepared
            .client
            .restrict_codex_access()
            .map_err(authentication_gateway_error)?;
        let binding = prepared.client.policy.codex_access().ok_or_else(|| {
            GatewayError::new(GatewayErrorKind::PolicyDenied, "Codex access is disabled")
        })?;
        let scope = Arc::clone(prepared.client.policy.account_scope());
        let gateway = scope
            .account_provider(&binding.account_id)
            .and_then(|kind| self.providers.get(kind))
            .and_then(|provider| provider.codex_gateway())
            .ok_or_else(|| {
                GatewayError::new(GatewayErrorKind::Unsupported, "Codex access is unavailable")
            })?;
        // 辅助调用完成只取消自身资源，外层中间件仍可加工身份或响应
        let cancellation = prepared.cancellation().child_token();
        let session_pool = binding.mode == CodexAccessMode::SessionPool;
        // session_pool 的辅助请求可能先于模型请求到达；首次认领复用同一份冻结选号事实
        let selection = if session_pool {
            Some(Arc::new(auxiliary_selection(
                &prepared,
                Arc::clone(&scope),
                cancellation.clone(),
            )?))
        } else {
            None
        };
        let context = CodexAuthorizedContext {
            client_api_key_id: prepared.client.policy.key_id().clone(),
            // 入口账号始终存在：身份投影与账号级辅助沿用它，只有会话级执行改由 owner 决定
            entry_account_id: binding.account_id.clone(),
            session_pool,
            account_scope: scope,
            selection,
            cancellation,
            deadline: prepared.deadline_at(),
        };
        // 在准入等待前持有取消责任，覆盖取得响应头前请求 future 被丢弃的出口
        let mut lifetime = CodexCallLifetime {
            cancellation: context.cancellation().clone(),
            admission: None,
        };
        lifetime.admission = Some({
            let wait_budget = ConcurrencyWaitBudget::default();
            let admission = self
                .acquire_client_admission(
                    &prepared.client,
                    prepared.request_id(),
                    context.deadline(),
                    context.cancellation().clone(),
                    &wait_budget,
                    AdmissionPurpose::Auxiliary,
                )
                .fuse();
            let cancelled = context.cancellation().cancelled().fuse();
            pin_mut!(admission, cancelled);
            // 非队首等待也须响应取消，丢弃准入 future 沿 RAII 回收等待位置和租约
            select_biased! {
                () = cancelled => return Err(cancelled_error()),
                result = admission => result?,
            }
        });
        Ok((gateway, context, lifetime))
    }

    pub(super) async fn codex_identity_inner(
        &self,
        prepared: PreparedRootExecution,
    ) -> Result<CodexIdentity, GatewayError> {
        let (gateway, context, mut lifetime) = self.prepare_codex_call(prepared).await?;
        let result = {
            let call = gateway.identity(&context).fuse();
            let cancelled = context.cancellation().cancelled().fuse();
            let timeout = context.deadline().wait().fuse();
            pin_mut!(call, cancelled, timeout);
            select_biased! {
                () = cancelled => Err(cancelled_error()),
                () = timeout => Err(timeout_error()),
                result = call => result.map_err(|error| GatewayError::from_provider(&error)),
            }
        };
        lifetime.release().await;
        result
    }

    pub(super) async fn codex_auxiliary_inner(
        &self,
        prepared: PreparedRootExecution,
        request: CodexAuxiliaryRequest,
    ) -> Result<MiddlewareResponse, GatewayError> {
        let (gateway, context, mut lifetime) = self.prepare_codex_call(prepared).await?;
        let result = {
            let call = gateway.auxiliary(&context, request).fuse();
            let cancelled = context.cancellation().cancelled().fuse();
            let timeout = context.deadline().wait().fuse();
            pin_mut!(call, cancelled, timeout);
            select_biased! {
                () = cancelled => Err(cancelled_error()),
                () = timeout => Err(timeout_error()),
                result = call => result.map_err(|error| GatewayError::from_provider(&error)),
            }
        };
        let response = match result {
            Ok(response) => response,
            Err(error) => {
                lifetime.release().await;
                return Err(error);
            }
        };
        let (protocol, status, headers, body, envelope) = response.into_parts();
        let mut response = MiddlewareResponse::new(
            protocol,
            status,
            headers,
            Box::new(CodexBody {
                inner: Some(body),
                lifetime,
                deadline: context.deadline(),
            }),
        );
        if let Some(envelope) = envelope {
            response = response.with_envelope(envelope);
        }
        Ok(response)
    }
}

struct CodexCallLifetime {
    cancellation: CancellationToken,
    admission: Option<AdmissionLease>,
}

impl CodexCallLifetime {
    async fn release(&mut self) {
        self.cancellation.cancel();
        if let Some(admission) = self.admission.take() {
            admission.release().await;
        }
    }
}

impl Drop for CodexCallLifetime {
    fn drop(&mut self) {
        self.cancellation.cancel();
        // AdmissionLease::drop 接管异步 abandon，不能在取得上游 headers 时释放并发
    }
}

struct CodexBody {
    inner: Option<Box<dyn MiddlewareBody>>,
    lifetime: CodexCallLifetime,
    deadline: Deadline,
}

impl CodexBody {
    async fn finish(&mut self) {
        self.lifetime.cancellation.cancel();
        if let Some(body) = self.inner.take() {
            body.close().await;
        }
        self.lifetime.release().await;
    }
}

impl MiddlewareBody for CodexBody {
    fn next_frame(&mut self) -> BoxFuture<'_, Result<Option<MiddlewareFrame>, MiddlewareError>> {
        Box::pin(async move {
            let Some(inner) = self.inner.as_mut() else {
                return Ok(None);
            };
            let result = {
                let next = inner.next_frame().fuse();
                let cancelled = self.lifetime.cancellation.cancelled().fuse();
                let timeout = self.deadline.wait().fuse();
                pin_mut!(next, cancelled, timeout);
                select_biased! {
                    () = cancelled => Err(MiddlewareError::Gateway(cancelled_error())),
                    () = timeout => Err(MiddlewareError::Gateway(timeout_error())),
                    result = next => result,
                }
            };
            if !matches!(result, Ok(Some(_))) {
                self.finish().await;
            }
            result
        })
    }

    fn commit_downstream(
        &mut self,
        status: Option<u16>,
    ) -> BoxFuture<'_, Result<(), MiddlewareError>> {
        Box::pin(async move {
            match self.inner.as_mut() {
                Some(inner) => inner.commit_downstream(status).await,
                None => Ok(()),
            }
        })
    }

    fn record_client_status(&mut self, status: u16) -> BoxFuture<'_, Result<(), MiddlewareError>> {
        Box::pin(async move {
            match self.inner.as_mut() {
                Some(inner) => inner.record_client_status(status).await,
                None => Ok(()),
            }
        })
    }

    fn is_finalized(&self) -> bool {
        self.lifetime.admission.is_none()
    }

    fn close(mut self: Box<Self>) -> BoxFuture<'static, ()> {
        Box::pin(async move { self.finish().await })
    }
}

fn cancelled_error() -> GatewayError {
    GatewayError::new(GatewayErrorKind::Cancelled, "Codex request was cancelled")
}

fn timeout_error() -> GatewayError {
    GatewayError::new(GatewayErrorKind::Timeout, "Codex request deadline elapsed")
}

/// 构造与模型 attempt 同形的辅助选号事实
///
/// 辅助请求不产生模型费用，但首次认领仍须复用现有可用性、调度策略与账号租约，
/// 因此只冻结选择所需的上下文，不伪造模型请求或 usage
fn auxiliary_selection(
    prepared: &PreparedRootExecution,
    scope: Arc<crate::account::scope::FrozenAccountScope>,
    cancellation: CancellationToken,
) -> Result<AttemptContext, GatewayError> {
    let account_group_ids = scope
        .routing_snapshot()
        .groups_snapshot()
        .iter()
        .map(|group| group.id().clone())
        .collect::<Vec<_>>();
    let request = RequestAttemptContext::new(
        prepared.request_id().clone(),
        prepared.client.policy.key_id().clone(),
    )
    .with_middleware(
        None,
        Arc::from(account_group_ids),
        "codex_auxiliary".to_owned(),
        ClientTransport::HttpJson,
    );
    Ok(AttemptContext::new(
        request,
        NonZeroU32::new(1).expect("constant auxiliary attempt index is non-zero"),
        prepared.deadline_at(),
        prepared.client.snapshot.account_selection_policy(),
        AccountAttemptContext::new(BTreeSet::new(), None, None).with_account_scope(scope),
        None,
        cancellation,
    ))
}
