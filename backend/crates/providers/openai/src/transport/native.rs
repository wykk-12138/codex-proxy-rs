//! 原生 Codex 有限辅助操作的单次 HTTP 发送，正文与业务头保持原始字节

use gateway_core::engine::codex::{CodexAuxiliaryOperation, CodexAuxiliaryRequest};
use reqwest::{
    Method, Response,
    header::{CONTENT_TYPE, HeaderName, HeaderValue},
};

use super::{
    CodexBackendClient, CodexClientError, CodexClientResult, CodexRequestContext, endpoint_url,
};

impl CodexBackendClient {
    pub(crate) async fn native_auxiliary(
        &self,
        request: CodexAuxiliaryRequest,
        context: CodexRequestContext<'_>,
        is_fedramp: bool,
    ) -> CodexClientResult<Response> {
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
        self.client
            .request(method, url)
            .headers(headers)
            .body(request.body)
            .send()
            .await
            .map_err(CodexClientError::HttpJson)
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
