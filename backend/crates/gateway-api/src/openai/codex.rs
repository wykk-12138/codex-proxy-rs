//! Codex 原生固定路由与身份投影，模型和辅助 HTTP 共用既有 adapter

use axum::{
    Extension, Json, Router,
    body::Bytes,
    extract::{OriginalUri, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse as _, Response},
    routing::{any, get, post},
};
use gateway_core::{
    engine::{
        authentication::ClientAuthenticationPurpose,
        codex::{CodexAuxiliaryOperation, CodexAuxiliaryRequest, CodexIdentity},
        execution::AuthenticatedClient,
        middleware::{MiddlewareError, MiddlewareHeader, MiddlewareResponse},
    },
    error::{GatewayError, GatewayErrorKind},
};
use serde_json::json;

use super::{
    auth::{authenticate_client_with_purpose, client_access_error_response},
    error::gateway_error_response,
    middleware::{
        ExpectedBody, RequestInput, buffered_response, error_response, into_http_response,
        invoke_request, request_parts,
    },
    responses::forwardable_request_headers,
};
use crate::ApiState;

pub(super) const RESPONSES_PATH: &str = "/backend-api/codex/responses";
pub(super) const MODELS_PATH: &str = "/backend-api/codex/models";
pub(super) const SEARCH_PATH: &str = "/backend-api/codex/alpha/search";

pub(super) fn responses_path(client: &AuthenticatedClient) -> &'static str {
    match client.authentication_purpose() {
        ClientAuthenticationPurpose::Codex => RESPONSES_PATH,
        ClientAuthenticationPurpose::Models => super::router::RESPONSES_PATH,
    }
}

#[derive(Clone, Copy)]
enum IdentityProjection {
    Whoami,
    AccountsCheck,
}

pub(super) fn router() -> Router<ApiState> {
    let mut router = Router::new()
        .route(
            "/v1/user-auth-credential/whoami",
            get(identity).layer(Extension(IdentityProjection::Whoami)),
        )
        .route(
            "/backend-api/wham/accounts/check",
            get(identity).layer(Extension(IdentityProjection::AccountsCheck)),
        )
        .route("/backend-api/codex/models", get(super::models::models))
        .route(
            "/backend-api/codex/responses",
            get(super::responses::responses_websocket).post(super::responses::responses),
        )
        .route(
            "/backend-api/codex/alpha/search",
            post(super::search::standalone_search),
        );
    for (path, operation) in [
        (
            "/backend-api/codex/alpha/history/v2/list_windows",
            CodexAuxiliaryOperation::HistoryListWindows,
        ),
        (
            "/backend-api/codex/alpha/history/v2/list_items",
            CodexAuxiliaryOperation::HistoryListItems,
        ),
        (
            "/backend-api/codex/alpha/history/v2/read_item",
            CodexAuxiliaryOperation::HistoryReadItem,
        ),
        (
            "/backend-api/codex/alpha/history/v2/search_contents",
            CodexAuxiliaryOperation::HistorySearchContents,
        ),
        (
            "/backend-api/codex/alpha/notes/v2/thread_hint",
            CodexAuxiliaryOperation::NotesThreadHint,
        ),
        (
            "/backend-api/codex/alpha/notes/v2/list_files_by_prefix",
            CodexAuxiliaryOperation::NotesListFilesByPrefix,
        ),
        (
            "/backend-api/codex/alpha/notes/v2/read_file",
            CodexAuxiliaryOperation::NotesReadFile,
        ),
        (
            "/backend-api/codex/alpha/notes/v2/search_contents",
            CodexAuxiliaryOperation::NotesSearchContents,
        ),
        (
            "/backend-api/codex/alpha/notes/v2/append_to_file",
            CodexAuxiliaryOperation::NotesAppendToFile,
        ),
        (
            "/backend-api/codex/alpha/notes/v2/write_file",
            CodexAuxiliaryOperation::NotesWriteFile,
        ),
    ] {
        router = router.route(path, post(auxiliary).layer(Extension(operation)));
    }
    for (path, operation) in [
        ("/backend-api/wham/usage", CodexAuxiliaryOperation::Usage),
        (
            "/backend-api/wham/profiles/me",
            CodexAuxiliaryOperation::Profile,
        ),
        (
            "/backend-api/wham/settings/user",
            CodexAuxiliaryOperation::UserSettings,
        ),
        (
            "/backend-api/wham/workspace-messages",
            CodexAuxiliaryOperation::WorkspaceMessages,
        ),
        (
            "/backend-api/wham/config/bundle",
            CodexAuxiliaryOperation::ConfigBundle,
        ),
    ] {
        router = router.route(path, get(auxiliary).layer(Extension(operation)));
    }
    router
        .route_layer(axum::middleware::from_fn(reject_head))
        .layer(Extension(ClientAuthenticationPurpose::Codex))
        // whoami 在根级 /v1 下，其余原生路径在 /backend-api 下；这些命名空间里的未知 API 必须
        // 明确 404，不能落入根路由的管理端 SPA。旧基址 /auth 与已迁移的 /codex 前缀同样返回 404，
        // 使配置错误的客户端得到明确失败，而不是 200 的 HTML 页面
        .route("/v1/user-auth-credential", any(not_found))
        .route("/v1/user-auth-credential/{*path}", any(not_found))
        .route("/auth", any(not_found))
        .route("/auth/{*path}", any(not_found))
        .route("/backend-api", any(not_found))
        .route("/backend-api/{*path}", any(not_found))
        .route("/codex", any(not_found))
        .route("/codex/{*path}", any(not_found))
}

async fn not_found() -> StatusCode {
    StatusCode::NOT_FOUND
}

// axum 的 GET 默认同时接受 HEAD；原生路由只开放合同列出的准确方法
async fn reject_head(request: axum::extract::Request, next: axum::middleware::Next) -> Response {
    if request.method() == axum::http::Method::HEAD {
        StatusCode::METHOD_NOT_ALLOWED.into_response()
    } else {
        next.run(request).await
    }
}

async fn identity(
    State(state): State<ApiState>,
    Extension(projection): Extension<IdentityProjection>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
) -> Response {
    let service = state.openai();
    let client = match authenticate_client_with_purpose(
        service,
        &headers,
        ClientAuthenticationPurpose::Codex,
    )
    .await
    {
        Ok(client) => client,
        Err(error) => return client_access_error_response(error),
    };
    let execution = service.execution();
    let prepared = match execution.prepare_execution(client).await {
        Ok(prepared) => prepared,
        Err(error) => return gateway_error_response(&error),
    };
    let terminal = execution.clone();
    let result = invoke_request(
        execution,
        prepared,
        RequestInput::query(uri.to_string(), None, &headers),
        Box::new(move |prepared, request| {
            Box::pin(async move {
                if request.protocol() != "openai" || !request.body().is_empty() {
                    return Err(MiddlewareError::Rejected);
                }
                let origin = prepared
                    .client()
                    .snapshot()
                    .settings()
                    .codex_public_origin()
                    .ok_or(MiddlewareError::InvalidState)?
                    .to_owned();
                let facts = terminal
                    .codex_identity(prepared)
                    .await
                    .map_err(MiddlewareError::Gateway)?;
                let response = identity_response(projection, facts, &origin)?;
                buffered_response("openai", response).await
            })
        }),
    )
    .await;
    match result {
        Ok(response) => into_http_response(response, ExpectedBody::SingleJson).await,
        Err(error) => error_response(error),
    }
}

fn identity_response(
    projection: IdentityProjection,
    facts: CodexIdentity,
    origin: &str,
) -> Result<Response, MiddlewareError> {
    let body = match projection {
        IdentityProjection::Whoami => {
            let plan = facts.plan_type.ok_or_else(|| {
                MiddlewareError::Gateway(GatewayError::new(
                    GatewayErrorKind::UpstreamUnavailable,
                    "Codex account plan is unavailable",
                ))
            })?;
            json!({
                "email": facts.email,
                "chatgpt_user_id": facts.user_id,
                "chatgpt_account_id": facts.account_id,
                "chatgpt_plan_type": plan,
                "chatgpt_account_is_fedramp": facts.is_fedramp,
            })
        }
        IdentityProjection::AccountsCheck => json!({
            "accounts": [{
                "id": facts.account_id,
                "plan_type": facts.plan_type,
                "workspace_backend_origin": origin,
                "account_routing_override": "NO_CONSTRAINT",
            }],
            "account_ordering": [facts.account_id],
            "default_account_id": facts.account_id,
        }),
    };
    Ok(([("cache-control", "private, no-store")], Json(body)).into_response())
}

async fn auxiliary(
    State(state): State<ApiState>,
    Extension(operation): Extension<CodexAuxiliaryOperation>,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let service = state.openai();
    let client = match authenticate_client_with_purpose(
        service,
        &headers,
        ClientAuthenticationPurpose::Codex,
    )
    .await
    {
        Ok(client) => client,
        Err(error) => return client_access_error_response(error),
    };
    let execution = service.execution();
    let prepared = match execution.prepare_execution(client).await {
        Ok(prepared) => prepared,
        Err(error) => return gateway_error_response(&error),
    };
    let terminal = execution.clone();
    let mut input = RequestInput::query(uri.to_string(), None, &headers);
    input.body = body;
    let query = uri.query().map(str::to_owned);
    let result = invoke_request(
        execution,
        prepared,
        input,
        Box::new(move |prepared, request| {
            Box::pin(async move {
                let (protocol, headers, body) = request_parts(request)?;
                if protocol != "openai" {
                    return Err(MiddlewareError::Rejected);
                }
                let headers = forwardable_request_headers(&headers)
                    .map(|(name, value)| {
                        MiddlewareHeader::new(
                            name.as_str(),
                            Bytes::copy_from_slice(value.as_bytes()),
                        )
                    })
                    .collect();
                let response = terminal
                    .codex_auxiliary(
                        prepared,
                        CodexAuxiliaryRequest {
                            operation,
                            query,
                            headers,
                            body,
                        },
                    )
                    .await
                    .map_err(MiddlewareError::Gateway)?;
                Ok(filter_response_headers(response))
            })
        }),
    )
    .await;
    match result {
        Ok(response) => into_http_response(response, ExpectedBody::RawStream).await,
        Err(error) => error_response(error),
    }
}

fn filter_response_headers(response: MiddlewareResponse) -> MiddlewareResponse {
    let (protocol, status, headers, body, envelope) = response.into_parts();
    let connection_options = headers
        .iter()
        .filter(|header| header.name().eq_ignore_ascii_case("connection"))
        .filter_map(|header| std::str::from_utf8(header.value()).ok())
        .flat_map(|value| value.split(','))
        .map(|value| value.trim().to_ascii_lowercase())
        .collect::<Vec<_>>();
    let headers = headers
        .into_iter()
        .filter(|header| {
            (header.name().eq_ignore_ascii_case("content-type")
                && !connection_options
                    .iter()
                    .any(|name| name.eq_ignore_ascii_case("content-type")))
                || gateway_protocol::openai::response_header_is_forwardable(
                    header.name(),
                    &connection_options,
                )
        })
        .collect();
    let mut response = MiddlewareResponse::new(protocol, status, headers, body);
    if let Some(envelope) = envelope {
        response = response.with_envelope(envelope);
    }
    response
}
