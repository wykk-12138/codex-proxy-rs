//! 原生 Codex 有限辅助操作的单次 HTTP 发送，正文与业务头保持原始字节

use gateway_core::engine::codex::{CodexAuxiliaryOperation, CodexAuxiliaryRequest};
use reqwest::{
    Method, RequestBuilder, Response,
    header::{CONTENT_TYPE, HeaderName, HeaderValue},
};

use super::{
    CodexBackendClient, CodexClientError, CodexClientResult, CodexRequestContext, endpoint_url,
};

/// 已构造完成、只待发送的辅助请求
///
/// 地址、身份头、业务头与正文都在准备阶段确定；准备失败时字节还没有离开网关，
/// 调用方可以据此在记录写意图之前返回真实失败
pub(crate) struct PreparedNativeAuxiliary {
    request: RequestBuilder,
}

impl PreparedNativeAuxiliary {
    /// 交给交付任务后只发送一次，不重试也不重新构造身份头
    pub(crate) async fn execute(self) -> CodexClientResult<Response> {
        self.request
            .send()
            .await
            .map_err(CodexClientError::HttpJson)
    }
}

impl CodexBackendClient {
    pub(crate) async fn native_auxiliary(
        &self,
        request: CodexAuxiliaryRequest,
        context: CodexRequestContext<'_>,
        is_fedramp: bool,
    ) -> CodexClientResult<Response> {
        self.prepare_native_auxiliary(request, context, is_fedramp)?
            .execute()
            .await
    }

    /// 只构造请求而不发送
    ///
    /// 可确定的地址、头与正文错误在这里返回；调用方必须在本步骤成功后才允许
    /// 记录持久写意图或让字节离开网关
    pub(crate) fn prepare_native_auxiliary(
        &self,
        request: CodexAuxiliaryRequest,
        context: CodexRequestContext<'_>,
        is_fedramp: bool,
    ) -> CodexClientResult<PreparedNativeAuxiliary> {
        let (method, path) = auxiliary_endpoint(request.operation);
        let mut url = endpoint_url(&self.base_url, path);
        if let Some(query) = &request.query {
            url.push('?');
            url.push_str(query);
        }
        let mut headers = if method == Method::POST {
            self.model_request_headers(&self.profile.snapshot(), context)?
        } else {
            self.account_request_headers(context)?
        };
        if method == Method::POST {
            headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        }
        // 下游只提供业务上下文；当前账号认证、画像与传输字段仍由 Provider 持有
        let mut replaced = std::collections::HashSet::new();
        for header in request.headers {
            let name = HeaderName::from_bytes(header.name().as_bytes())?;
            if super::headers::is_managed_identity_header(name.as_str())
                || gateway_protocol::openai::is_transport_managed_request_header(name.as_str())
            {
                continue;
            }
            if replaced.insert(name.clone()) {
                headers.remove(&name);
            }
            headers.append(name, HeaderValue::from_bytes(header.value())?);
        }
        if is_fedramp {
            headers.insert("x-openai-fedramp", HeaderValue::from_static("true"));
        }
        // 包括 notes 写入在内均只发送一次，上游错误状态也作为原始响应交付
        Ok(PreparedNativeAuxiliary {
            request: self
                .client
                .request(method, url)
                .headers(headers)
                .body(request.body),
        })
    }
}

fn auxiliary_endpoint(operation: CodexAuxiliaryOperation) -> (Method, &'static str) {
    use CodexAuxiliaryOperation::*;
    match operation {
        HistoryListWindows => (Method::POST, "/codex/alpha/history/v2/list_windows"),
        HistoryListItems => (Method::POST, "/codex/alpha/history/v2/list_items"),
        HistoryReadItem => (Method::POST, "/codex/alpha/history/v2/read_item"),
        HistorySearchContents => (Method::POST, "/codex/alpha/history/v2/search_contents"),
        NotesThreadHint => (Method::POST, "/codex/alpha/notes/v2/thread_hint"),
        NotesListFilesByPrefix => (Method::POST, "/codex/alpha/notes/v2/list_files_by_prefix"),
        NotesReadFile => (Method::POST, "/codex/alpha/notes/v2/read_file"),
        NotesSearchContents => (Method::POST, "/codex/alpha/notes/v2/search_contents"),
        NotesAppendToFile => (Method::POST, "/codex/alpha/notes/v2/append_to_file"),
        NotesWriteFile => (Method::POST, "/codex/alpha/notes/v2/write_file"),
        Usage => (Method::GET, "/wham/usage"),
        Profile => (Method::GET, "/wham/profiles/me"),
        UserSettings => (Method::GET, "/wham/settings/user"),
        WorkspaceMessages => (Method::GET, "/wham/workspace-messages"),
        ConfigBundle => (Method::GET, "/wham/config/bundle"),
    }
}
