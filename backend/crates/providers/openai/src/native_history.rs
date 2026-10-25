//! 原生 history 的 Provider 侧接线：请求/响应采集与本地四工具响应
//!
//! 本模块只解释 Codex 自己的 wire 与事件事实，把条目交给 Core 的 history 服务持久化，
//! 或把 Core 的查询结果编码成官方 JSON 结果
//! 采集使用隐私规则处理后的最终业务正文；条目不经过任何解密或伪造明文
//! 本地回答只覆盖 history 四工具，notes 仍由上游执行

use std::sync::Arc;

use bytes::Bytes;
use gateway_core::{
    engine::{
        codex::CodexAuxiliaryOperation,
        codex_history::{
            CODEX_ROOT_AGENT_NAME, CodexHistoryFilterQuery, CodexHistoryItemPage,
            CodexHistoryListItemsQuery, CodexHistoryQueryBudget, CodexHistoryQueryError,
            CodexHistoryReadItemOutcome, CodexHistoryReadQuery, CodexHistoryScope,
            CodexHistorySearchQuery, CodexHistoryService, CodexHistoryToolOutcome,
            CodexHistoryToolQuery, CodexHistoryToolResult, CodexHistoryWindowList,
            CodexHistoryWindowQuery,
        },
        codex_history_capture::{
            CodexHistoryCaptureError, CodexHistoryCaptureItem, CodexHistoryInitialization,
            CodexHistoryRequestFacts,
        },
        middleware::{MiddlewareHeader, MiddlewareResponse},
    },
    error::{ProviderError, ProviderErrorKind},
    event::ProviderEvent,
    policy::ClientApiKeyId,
    provider_ports::{
        CodexHistoryRole, CodexHistoryWindow, CodexNativeWindowId, ProviderSessionAffinityKey,
        ProviderStoreError,
    },
    upstream::UpstreamSendState,
};
use gateway_protocol::openai::{
    CodexHistoryInitialization as WireInitialization, CodexHistoryItemEntry,
    CodexHistoryItemListResult, CodexHistoryItemNotFoundResult, CodexHistoryReadItemResult,
    CodexHistoryTool, CodexHistoryToolArguments, CodexHistoryToolRequest, CodexHistoryWindowEntry,
    CodexHistoryWindowListResult, CodexHistoryWireError, CodexNativeStateMetadata,
    CodexSessionSources, CodexToolOutputTruncationPolicy, TOOL_OUTPUT_TRUNCATION_POLICY_HEADER,
    codex_history_declared_item_id, codex_history_item_attachments, codex_history_item_projection,
    codex_history_item_semantics, codex_native_state_metadata, parse_codex_history_tool_request,
    project_local_history_search_query,
};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;

use crate::buffered_body::BufferedBody;
use crate::provider::failure::continuation_replay_required_error;

/// 一次模型请求的采集句柄
///
/// 同一次请求的 HTTP 与 WS 发送路径共用同一句柄：本地检索声明投影与请求采集都发生在
/// 实际发送路径，WS 回退到 HTTP 时只有真正发送的那条路径写入一次
pub(crate) struct CodexNativeHistoryRequest {
    service: Arc<CodexHistoryService>,
    facts: CodexHistoryRequestFacts,
    /// 本地 history 裁决结果；发送前由 `begin` 写入，同一请求只读一次持久来源
    capture: std::sync::OnceLock<bool>,
    state: Mutex<CaptureState>,
}

#[derive(Default)]
struct CaptureState {
    response_id: Option<String>,
    anchored: bool,
    /// 本次请求的输入是否已经落库；同一请求的 WS 与 HTTP 发送路径只写一次
    committed: bool,
    /// 发送前保存的逻辑输入引用；响应锚定时沿用同一请求句柄的来源
    input: Vec<String>,
}

impl CodexNativeHistoryRequest {
    /// 从最终模型正文与请求上下文解析原生窗口事实
    ///
    /// 窗口缺失或形状不符时不建立句柄：不能猜测条目归属，保持既有转发行为
    pub(crate) fn from_request(
        service: Arc<CodexHistoryService>,
        key_id: &ClientApiKeyId,
        session_key: &ProviderSessionAffinityKey,
        body: &Map<String, Value>,
        context: &Map<String, Value>,
    ) -> Option<Self> {
        let metadata = codex_native_state_metadata(CodexSessionSources {
            body,
            context,
            turn_metadata: None,
            session_id: None,
            endpoint_session: None,
        });
        let history_initialization = initialization(&metadata);
        let window_id = CodexNativeWindowId::try_new(metadata.window_id?).ok()?;
        Some(Self {
            service,
            facts: CodexHistoryRequestFacts {
                key_id: key_id.clone(),
                session_key: session_key.clone(),
                window: CodexHistoryWindow {
                    window_id,
                    agent_name: metadata.agent_name,
                    context_window_id: metadata.context_window_id,
                },
                previous_response_id: metadata.previous_response_id,
                request_kind: metadata.request_kind,
                history_initialization,
            },
            capture: std::sync::OnceLock::new(),
            state: Mutex::new(CaptureState::default()),
        })
    }

    /// 在发送前判定采集并校验续链父响应
    ///
    /// 首次未判定会话在这里完成来源判定并被缓存；父响应未知或属于其它窗口时
    /// 直接返回既有 replay-required 错误，不发送本次请求
    ///
    /// # Errors
    ///
    /// 读取会话来源失败、父响应未知或属于其它窗口
    pub(crate) async fn begin(&self) -> Result<(), ProviderError> {
        if !self.captures().await? {
            return Ok(());
        }
        if self.facts.previous_response_id.is_some() {
            self.service
                .validate_parent(&self.facts)
                .await
                .map_err(|error| map_capture_error(error, UpstreamSendState::NotSent))?;
        }
        Ok(())
    }

    /// 在隐私规则之后适配出站检索声明
    ///
    /// 改写的正是即将发送的正文：managed 会话的 history 检索查询变成明文，
    /// notes 的加密字段与其它工具完全不动
    pub(crate) fn prepare(&self, body: &mut Map<String, Value>) {
        if !self.capture.get().copied().unwrap_or_default() {
            return;
        }
        // 是否改写过只对测试有意义：搜索声明已适配时重复投影是 no-op
        let _ = project_local_history_search_query(body);
    }

    /// 在发送前保存本次请求输入与逻辑输入引用
    ///
    /// 落库失败直接返回错误，由传输层在发送前中止；不把错误推迟到已发送后
    ///
    /// # Errors
    ///
    /// 读取会话来源失败或持久化失败
    pub(crate) async fn commit_request(
        &self,
        body: &Map<String, Value>,
    ) -> Result<(), ProviderError> {
        if !self.captures().await? {
            return Ok(());
        }
        let mut state = self.state.lock().await;
        if state.committed {
            return Ok(());
        }
        let items = body
            .get("input")
            .and_then(Value::as_array)
            .map(|entries| entries.iter().filter_map(capture_item).collect::<Vec<_>>())
            .unwrap_or_default();
        let captured = self
            .service
            .capture_request(&self.facts, &items)
            .await
            .map_err(|error| map_capture_error(error, UpstreamSendState::NotSent))?;
        state.input = captured.input;
        state.committed = true;
        Ok(())
    }

    /// 在交付客户端之前保存本批响应条目
    ///
    /// 首个响应级事件先建立 response 锚（预热与零输出响应同样保留），
    /// 并把发送前确定的逻辑输入引用写进该响应；每个 `response.output_item.done`
    /// 条目先落库再随本批事件交付
    ///
    /// # Errors
    ///
    /// 读取会话来源失败或持久化失败
    pub(crate) async fn observe_events(
        &self,
        events: &[ProviderEvent],
    ) -> Result<(), ProviderError> {
        if !self.captures().await? {
            return Ok(());
        }
        let Some((response_id, anchors, input)) = self.track_response(events).await else {
            return Ok(());
        };
        if anchors {
            // 空批次提交同时写下窗口、父响应、逻辑输入与 response ID
            self.service
                .capture_response(&self.facts, &response_id, &input, &[])
                .await
                .map_err(|error| map_capture_error(error, UpstreamSendState::Sent))?;
        }
        let items = events
            .iter()
            .filter_map(ProviderEvent::wire_event)
            .filter(|wire| wire.event_type() == Some("response.output_item.done"))
            .filter_map(|wire| wire.data().get("item").and_then(capture_item))
            .collect::<Vec<_>>();
        if !items.is_empty() {
            self.service
                .capture_response(&self.facts, &response_id, &input, &items)
                .await
                .map_err(|error| map_capture_error(error, UpstreamSendState::Sent))?;
        }
        Ok(())
    }

    /// 记录本次响应 ID，并报告本批是否需要先建立锚及逻辑输入引用
    async fn track_response(
        &self,
        events: &[ProviderEvent],
    ) -> Option<(String, bool, Vec<String>)> {
        let mut state = self.state.lock().await;
        if state.response_id.is_none() {
            state.response_id = events
                .iter()
                .filter_map(ProviderEvent::wire_event)
                .filter_map(|wire| wire.data().pointer("/response/id"))
                .filter_map(Value::as_str)
                .find(|id| !id.is_empty())
                .map(ToOwned::to_owned);
        }
        let response_id = state.response_id.clone()?;
        let anchor = !state.anchored;
        state.anchored = true;
        Some((response_id, anchor, state.input.clone()))
    }

    /// 是否由本地采集；只有 managed 会话为真
    ///
    /// 首次未判定会话在 `begin` 阶段完成来源判定，结论在同一请求内只读一次
    async fn captures(&self) -> Result<bool, ProviderError> {
        if let Some(capture) = self.capture.get() {
            return Ok(*capture);
        }
        let capture = self
            .service
            .begin_request(&self.facts)
            .await
            .map_err(|error| map_capture_error(error, UpstreamSendState::NotSent))?;
        let _ = self.capture.set(capture);
        Ok(capture)
    }
}

/// 官方的 history 初始化声明投影到 Core 的三类事实
fn initialization(metadata: &CodexNativeStateMetadata) -> CodexHistoryInitialization {
    match metadata.history_initialization {
        Some(WireInitialization::New) => CodexHistoryInitialization::New,
        Some(
            WireInitialization::Cleared
            | WireInitialization::ColdResume
            | WireInitialization::WarmFork
            | WireInitialization::ColdFork
            | WireInitialization::SuppliedHistory,
        ) => CodexHistoryInitialization::Supplied,
        None => CodexHistoryInitialization::Undeclared,
    }
}

/// 一条官方 response item 的采集投影
///
/// 只有对象条目能按官方结构保存；原始 JSON 由解析后的事件数据重新序列化，
/// 结构、密文与附件字段完整保留
fn capture_item(value: &Value) -> Option<CodexHistoryCaptureItem> {
    value.as_object()?;
    let raw = serde_json::to_string(value).ok()?;
    let projection = codex_history_item_projection(&raw);
    Some(CodexHistoryCaptureItem {
        declared_item_id: codex_history_declared_item_id(&raw),
        role: projection.role.and_then(CodexHistoryRole::from_wire),
        tool_namespace: projection.tool_namespace,
        tool_name: projection.tool_name,
        content_type: projection.content_type,
        identity: history_item_identity(&raw)?,
        raw,
        text: projection.text,
    })
}

/// 条目完整语义身份摘要
///
/// 协议投影忽略 id/status 与文本分段类型等承载差异，密文与图片字段参与其中；
/// 摘要只用于同一位置的对象比较，不参与全局去重，也不替代已保存的原对象
fn history_item_identity(raw: &str) -> Option<String> {
    let semantics = codex_history_item_semantics(raw)?;
    Some(hex::encode(Sha256::digest(semantics.as_bytes())))
}

fn map_capture_error(
    error: CodexHistoryCaptureError,
    send_state: UpstreamSendState,
) -> ProviderError {
    match error {
        CodexHistoryCaptureError::ParentResponseUnknown => {
            continuation_replay_required_error("history_parent_unknown")
        }
        CodexHistoryCaptureError::ParentWindowChanged => {
            continuation_replay_required_error("history_parent_window_changed")
        }
        CodexHistoryCaptureError::Store(source) => history_store_error(source, send_state),
    }
}

fn history_store_error(
    _source: ProviderStoreError,
    send_state: UpstreamSendState,
) -> ProviderError {
    ProviderError::new(ProviderErrorKind::Unavailable, send_state)
}

/// 四个 history 工具对应的官方辅助操作
pub(crate) fn history_tool(operation: CodexAuxiliaryOperation) -> Option<CodexHistoryTool> {
    match operation {
        CodexAuxiliaryOperation::HistoryListWindows => Some(CodexHistoryTool::ListWindows),
        CodexAuxiliaryOperation::HistoryListItems => Some(CodexHistoryTool::ListItems),
        CodexAuxiliaryOperation::HistoryReadItem => Some(CodexHistoryTool::ReadItem),
        CodexAuxiliaryOperation::HistorySearchContents => Some(CodexHistoryTool::SearchContents),
        _ => None,
    }
}

/// 本地回答一次 history 辅助请求
///
/// 返回 None 表示该会话不由本地历史回答，继续既有上游行为；
/// 本地回答不读取凭据、不做 whoami，也不受推理额度影响
///
/// # Errors
///
/// 参数不在官方合同内、读取持久事实失败或结果编码失败
pub(crate) async fn local_history_response(
    service: &CodexHistoryService,
    key_id: &ClientApiKeyId,
    session_key: &ProviderSessionAffinityKey,
    operation: CodexAuxiliaryOperation,
    headers: &[MiddlewareHeader],
    body: &[u8],
) -> Result<Option<MiddlewareResponse>, ProviderError> {
    let Some(tool) = history_tool(operation) else {
        return Ok(None);
    };
    let request = parse_codex_history_tool_request(tool, body).map_err(history_argument_error)?;
    let scope = CodexHistoryScope {
        key_id,
        session_key,
        current_agent_name: request
            .current_agent_name
            .as_deref()
            .unwrap_or(CODEX_ROOT_AGENT_NAME),
    };
    let query = history_query(&request)?;
    let budget = history_budget(headers);
    // 未命中结果需要回显客户端传入的完整窗口与条目 ID
    let read_ids = match &query {
        CodexHistoryToolQuery::ReadItem(read) => {
            Some((read.window_id.clone(), read.item_id.clone()))
        }
        _ => None,
    };
    match service.execute(&scope, &query, budget).await {
        Ok(CodexHistoryToolOutcome::Completed(result)) => {
            history_response(tool, read_ids, result).map(Some)
        }
        Ok(CodexHistoryToolOutcome::Upstream) => Ok(None),
        Err(error) => Err(history_query_error(error)),
    }
}

/// 官方参数映射到 Core 查询；未知 role 不静默忽略过滤
fn history_query(
    request: &CodexHistoryToolRequest,
) -> Result<CodexHistoryToolQuery, ProviderError> {
    let query = match &request.arguments {
        CodexHistoryToolArguments::ListWindows(arguments) => {
            CodexHistoryToolQuery::ListWindows(CodexHistoryWindowQuery {
                agent_name: arguments.agent_name.clone(),
                limit: arguments.limit,
                recent_first: arguments.recent_first.unwrap_or(false),
            })
        }
        CodexHistoryToolArguments::ListItems(arguments) => {
            let mut filter = CodexHistoryFilterQuery {
                agent_name: arguments.agent_name.clone(),
                window_id: arguments.window_id.clone(),
                role: None,
                tool_name: arguments.tool_name.clone(),
                tool_namespace: arguments.tool_namespace.clone(),
                limit: arguments.limit,
                recent_first: arguments.recent_first.unwrap_or(false),
            };
            filter
                .set_role(arguments.role.as_deref())
                .map_err(history_query_error)?;
            CodexHistoryToolQuery::ListItems(CodexHistoryListItemsQuery {
                filter,
                max_chars_per_item: arguments.max_chars_per_item,
            })
        }
        CodexHistoryToolArguments::ReadItem(arguments) => {
            CodexHistoryToolQuery::ReadItem(CodexHistoryReadQuery {
                agent_name: arguments.agent_name.clone(),
                window_id: arguments.window_id.clone(),
                item_id: arguments.item_id.clone(),
                offset_chars: arguments.offset_chars,
                limit_chars: arguments.limit_chars,
            })
        }
        CodexHistoryToolArguments::SearchContents(arguments) => {
            let mut filter = CodexHistoryFilterQuery {
                agent_name: arguments.agent_name.clone(),
                window_id: arguments.window_id.clone(),
                role: None,
                tool_name: arguments.tool_name.clone(),
                tool_namespace: arguments.tool_namespace.clone(),
                limit: arguments.limit,
                recent_first: arguments.recent_first.unwrap_or(false),
            };
            filter
                .set_role(arguments.role.as_deref())
                .map_err(history_query_error)?;
            CodexHistoryToolQuery::SearchContents(CodexHistorySearchQuery {
                query: arguments.query.clone(),
                filter,
            })
        }
    };
    Ok(query)
}

/// 客户端给出的输出截断策略；缺失时使用官方模型默认预算
fn history_budget(headers: &[MiddlewareHeader]) -> CodexHistoryQueryBudget {
    headers
        .iter()
        .find(|header| {
            header
                .name()
                .eq_ignore_ascii_case(TOOL_OUTPUT_TRUNCATION_POLICY_HEADER)
        })
        .and_then(|header| std::str::from_utf8(header.value()).ok())
        .and_then(CodexToolOutputTruncationPolicy::parse)
        .map_or_else(CodexHistoryQueryBudget::default_output, |policy| {
            CodexHistoryQueryBudget::new(policy.byte_budget())
        })
}

fn history_argument_error(_error: CodexHistoryWireError) -> ProviderError {
    ProviderError::new(
        ProviderErrorKind::InvalidRequest,
        UpstreamSendState::NotSent,
    )
}

fn history_query_error(error: CodexHistoryQueryError) -> ProviderError {
    match error {
        CodexHistoryQueryError::InvalidArgument(_) => ProviderError::new(
            ProviderErrorKind::InvalidRequest,
            UpstreamSendState::NotSent,
        ),
        CodexHistoryQueryError::Store(source) => {
            history_store_error(source, UpstreamSendState::NotSent)
        }
    }
}

/// 本地查询结果编码成官方 JSON 结果
fn history_response(
    tool: CodexHistoryTool,
    read_ids: Option<(String, String)>,
    result: CodexHistoryToolResult,
) -> Result<MiddlewareResponse, ProviderError> {
    let encoded = match result {
        CodexHistoryToolResult::Windows(windows) => serde_json::to_vec(&window_list(windows)),
        CodexHistoryToolResult::Items(page) => serde_json::to_vec(&item_list(tool, page)),
        CodexHistoryToolResult::Read(outcome) => read_item(read_ids, outcome),
    }
    .map_err(|_| ProviderError::new(ProviderErrorKind::Protocol, UpstreamSendState::NotSent))?;
    Ok(MiddlewareResponse::new(
        "openai".to_owned(),
        200,
        vec![MiddlewareHeader::new(
            "content-type",
            Bytes::from_static(b"application/json"),
        )],
        Box::new(BufferedBody::new(encoded.into())),
    ))
}

fn window_list(windows: CodexHistoryWindowList) -> CodexHistoryWindowListResult {
    CodexHistoryWindowListResult {
        agent_name: windows.agent_name,
        window_count: windows.window_count,
        total_windows: windows.total_windows,
        truncated: windows.truncated,
        windows: windows
            .windows
            .into_iter()
            .map(|window| CodexHistoryWindowEntry {
                window_id: window.window_id,
                agent_name: window.agent_name,
                item_count: window.item_count,
            })
            .collect(),
    }
}

/// list_items 与 search_contents 共用的结果形状；只有 list_items 回显条目字符上限
fn item_list(tool: CodexHistoryTool, page: CodexHistoryItemPage) -> CodexHistoryItemListResult {
    CodexHistoryItemListResult {
        window_id: page.window_id,
        item_count: page.item_count,
        returned: page.returned,
        truncated: page.truncated,
        max_chars_per_item: (tool == CodexHistoryTool::ListItems)
            .then_some(page.max_chars_per_item),
        items: page
            .items
            .into_iter()
            .map(|item| {
                CodexHistoryItemEntry::new(
                    item.item_id,
                    item.window_id,
                    item.agent_name,
                    item.role.map(|role| role.as_str().to_owned()),
                    item.content_type,
                    item.tool_namespace,
                    item.tool_name,
                    item.ordinal,
                    item.readable,
                    item.n_chars,
                    item.truncated_content,
                )
            })
            .collect(),
    }
}

/// 精确读取结果；未命中与不可读对象都不用可读正文冒充成功
fn read_item(
    read_ids: Option<(String, String)>,
    outcome: CodexHistoryReadItemOutcome,
) -> serde_json::Result<Vec<u8>> {
    let (window_id, item_id) = read_ids.unwrap_or_default();
    let read = match outcome {
        CodexHistoryReadItemOutcome::Read(read) => read,
        CodexHistoryReadItemOutcome::NotFound { window_found } => {
            return serde_json::to_vec(&CodexHistoryItemNotFoundResult::new(
                window_id,
                item_id,
                window_found,
            ));
        }
    };
    let attachments = codex_history_item_attachments(&read.raw_object);
    let result = match read.content {
        Some(content) => CodexHistoryReadItemResult::readable(
            read.window_id,
            read.item_id,
            read.content_type,
            read.n_chars,
            read.offset_chars,
            read.next_offset_chars,
            content,
            attachments,
        ),
        None => CodexHistoryReadItemResult::opaque(
            read.window_id,
            read.item_id,
            read.content_type,
            attachments,
        ),
    };
    serde_json::to_vec(&result)
}
