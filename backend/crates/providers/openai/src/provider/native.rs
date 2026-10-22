//! 原生 Codex 的真实身份投影与有限辅助请求交付，不参与模型调度或业务重放

use bytes::Bytes;
use futures::future::BoxFuture;
use gateway_core::{
    account::{ProviderAccount, ProviderAccountId},
    engine::{
        codex::{CodexAuthorizedContext, CodexAuxiliaryRequest, CodexGateway, CodexIdentity},
        middleware::{
            MiddlewareBody, MiddlewareError, MiddlewareFrame, MiddlewareFraming, MiddlewareHeader,
            MiddlewareResponse,
        },
    },
    error::{ProviderError, ProviderErrorKind},
    lifecycle::{CancellationToken, Deadline},
    provider_ports::{CodexSessionClaim, CodexSessionClaimOutcome},
    upstream::UpstreamSendState,
};
// 池会话身份在协议层判定，与 transport 的同名 CodexSessionIdentity 必须别名区分
use gateway_protocol::openai::CodexSessionIdentity as PoolSessionIdentity;
use secrecy::ExposeSecret as _;
use serde_json::{Map, Value};

use super::{
    CodexProvider,
    failure::{map_client_error, pool_session_error, provider_error},
    observation::map_selection_error,
};
use crate::{
    credential::{
        CodexCredentialLease, CodexOAuthSecret, CodexSelectionModel,
        SelectCodexProviderEndpointCredential, derive_pool_session_affinity, oauth_metadata,
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
                    let (account, runtime) = self.selector.native_credential(context, account_id).await.map_err(map_selection_error)?;
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
            let response = tokio::select! {
                biased;
                () = context.cancellation().cancelled() => return Err(provider_error(ProviderErrorKind::Cancelled, UpstreamSendState::Ambiguous)),
                () = context.deadline().wait() => return Err(provider_error(ProviderErrorKind::Timeout, UpstreamSendState::Ambiguous)),
                result = async {
                    // 账号解析（含 session_pool 首绑）先于发送；固定账号仍只做资格与凭据复验
                    let target = self.auxiliary_target(context, &request).await?;
                    let response = self.client.for_account(&target.account)
                        .map_err(|error| map_client_error(error, UpstreamSendState::NotSent, false).error)?
                        .native_auxiliary(request, CodexRequestContext::auxiliary(
                            &target.authorization, Some(&target.identity.account_id), "codex_auxiliary", None,
                        ), target.identity.is_fedramp).await
                        .map_err(|error| map_client_error(error, UpstreamSendState::Ambiguous, false).error)?;
                    Ok((response, target.lease))
                } => result?,
            };
            let (response, lease) = response;
            let status = response.status().as_u16();
            let connection_options = response
                .headers()
                .get_all("connection")
                .iter()
                .filter_map(|value| value.to_str().ok())
                .flat_map(|value| value.split(','))
                .map(|value| value.trim().to_ascii_lowercase())
                .collect::<Vec<_>>();
            let headers = response
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
                .collect();
            Ok(MiddlewareResponse::new(
                "openai".to_owned(),
                status,
                headers,
                Box::new(NativeBody {
                    response: Some(response),
                    // 辅助首绑取得的账号租约随正文生命周期释放，不能提前结束占用
                    _lease: lease,
                    cancellation: context.cancellation().clone(),
                    deadline: context.deadline(),
                }),
            ))
        })
    }
}

impl CodexProvider {
    /// 解析辅助请求的执行账号
    ///
    /// fixed 只复验入口账号；session_pool 与模型请求共用持久归属，首次先到时可完成认领
    async fn auxiliary_target(
        &self,
        context: &CodexAuthorizedContext,
        request: &CodexAuxiliaryRequest,
    ) -> Result<AuxiliaryTarget, ProviderError> {
        // fixed 与账号级辅助请求（usage/profile/settings 等）始终使用入口账号
        if !context.session_pool() || !request.operation.follows_codex_session() {
            let account_id = context.entry_account_id().clone();
            return self.unleased_auxiliary_target(context, &account_id).await;
        }
        let affinity = self.auxiliary_session_affinity(context, request)?;
        let owner = self
            .selector
            .load_session_owner(context.client_api_key_id(), affinity.key())
            .await
            .map_err(map_selection_error)?;
        if let Some(owner) = owner {
            // 已有归属只使用 owner，不因模型额度或池内其他账号重新选号
            return self.unleased_auxiliary_target(context, &owner).await;
        }
        // 辅助请求可能先于模型请求到达；复用模型选号规则完成原子认领，不产生模型费用
        let attempt = context.selection().ok_or_else(|| {
            provider_error(ProviderErrorKind::Unavailable, UpstreamSendState::NotSent)
        })?;
        let lease = self
            .selector
            .select_for_provider_endpoint(&SelectCodexProviderEndpointCredential {
                // 辅助端点与 Responses 同源，URL 只参与 cookie 适用域判断
                request_url: &self.responses_url,
                attempt,
                session_affinity: Some(&affinity),
                model: CodexSelectionModel::Search(None),
                requires_oauth: true,
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
            CodexSessionClaimOutcome::Owned => self.leased_auxiliary_target(lease).await,
            CodexSessionClaimOutcome::ConflictingOwner(owner) => {
                // 并发首请求已有赢家；释放临时租约后按该 owner 执行
                drop(lease);
                self.unleased_auxiliary_target(context, &owner).await
            }
            CodexSessionClaimOutcome::IdentityChanged => {
                drop(lease);
                Err(provider_error(
                    ProviderErrorKind::Unavailable,
                    UpstreamSendState::NotSent,
                ))
            }
            CodexSessionClaimOutcome::Unavailable => {
                drop(lease);
                Err(provider_error(
                    ProviderErrorKind::NoEligibleAccount,
                    UpstreamSendState::NotSent,
                ))
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
        context: &CodexAuthorizedContext,
        account_id: &ProviderAccountId,
    ) -> Result<AuxiliaryTarget, ProviderError> {
        let (account, runtime) = self
            .selector
            .native_credential(context, account_id)
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

/// 辅助请求解析出的执行账号；租约只在 session_pool 首绑时存在
struct AuxiliaryTarget {
    account: ProviderAccount,
    identity: CodexIdentity,
    authorization: String,
    lease: Option<Box<CodexCredentialLease>>,
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
