//! 原生 Codex 的真实身份投影与有限辅助请求交付，不参与模型调度或业务重放

use bytes::Bytes;
use futures::future::BoxFuture;
use gateway_core::{
    account::ProviderAccount,
    engine::{
        codex::{CodexAuthorizedContext, CodexAuxiliaryRequest, CodexGateway, CodexIdentity},
        middleware::{
            MiddlewareBody, MiddlewareError, MiddlewareFrame, MiddlewareFraming, MiddlewareHeader,
            MiddlewareResponse,
        },
    },
    error::{ProviderError, ProviderErrorKind},
    lifecycle::{CancellationToken, Deadline},
    upstream::UpstreamSendState,
};
use secrecy::ExposeSecret as _;

use super::{
    CodexProvider,
    failure::{map_client_error, provider_error},
    observation::map_selection_error,
};
use crate::{
    credential::{CodexOAuthSecret, oauth_metadata},
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
                    let (account, runtime) = self.selector.native_credential(context).await.map_err(map_selection_error)?;
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
                    let (account, runtime) = self.selector.native_credential(context).await.map_err(map_selection_error)?;
                    let secret = runtime.authentication.oauth().ok_or_else(invalid_identity)?;
                    let identity = self.native_identity(&account, secret).await?;
                    let authorization = runtime.authentication.authorization_header().map_err(|_| invalid_identity())?;
                    self.client.for_account(&account)
                        .map_err(|error| map_client_error(error, UpstreamSendState::NotSent, false).error)?
                        .native_auxiliary(request, CodexRequestContext::auxiliary(
                            authorization.expose_secret(), Some(&identity.account_id), "codex_auxiliary", None,
                        ), identity.is_fedramp).await
                        .map_err(|error| map_client_error(error, UpstreamSendState::Ambiguous, false).error)
                } => result?,
            };
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
                    cancellation: context.cancellation().clone(),
                    deadline: context.deadline(),
                }),
            ))
        })
    }
}

impl CodexProvider {
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

struct NativeBody {
    response: Option<reqwest::Response>,
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
