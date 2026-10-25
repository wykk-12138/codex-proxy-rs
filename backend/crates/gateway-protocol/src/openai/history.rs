//! 官方 history 四工具的参数、输出预算与结果编码，以及条目明文投影与附件提取
//!
//! 本模块只解释官方 wire 形状，不决定本地还是上游、授权范围或分页顺序
//! 不可读对象只给出类型与不可读事实，不生成明文，也不把 Base64 计入正文

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// 官方工具输出截断策略请求头
pub const TOOL_OUTPUT_TRUNCATION_POLICY_HEADER: &str = "x-openai-tool-output-truncation-policy";

/// 官方 history 工具所在 namespace
pub const HISTORY_TOOL_NAMESPACE: &str = "history";

/// 官方 token 到字节的近似换算；与客户端 `approx_bytes_for_tokens` 一致
const APPROX_BYTES_PER_TOKEN: u64 = 4;

/// 官方 history 四个工具
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodexHistoryTool {
    ListWindows,
    ListItems,
    ReadItem,
    SearchContents,
}

impl CodexHistoryTool {
    /// 官方工具名；与 `history` namespace 合用时即模型可见名称
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::ListWindows => "list_windows",
            Self::ListItems => "list_items",
            Self::ReadItem => "read_item",
            Self::SearchContents => "search_contents",
        }
    }

    /// 按官方工具名解析；未知名称不猜测
    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        match name {
            "list_windows" => Some(Self::ListWindows),
            "list_items" => Some(Self::ListItems),
            "read_item" => Some(Self::ReadItem),
            "search_contents" => Some(Self::SearchContents),
            _ => None,
        }
    }
}

/// history 工具参数或条目原文不在官方合同内
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CodexHistoryWireError {
    #[error("history tool arguments are not valid JSON")]
    InvalidJson,
    #[error("history tool arguments must be a JSON object")]
    NotAnObject,
    #[error("history.{0} arguments are invalid")]
    InvalidArguments(&'static str),
}

/// `list_windows` 参数
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct CodexHistoryListWindowsArguments {
    #[serde(default)]
    pub agent_name: Option<String>,
    #[serde(default)]
    pub limit: Option<u64>,
    #[serde(default)]
    pub recent_first: Option<bool>,
}

/// `list_items` 参数
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct CodexHistoryListItemsArguments {
    #[serde(default)]
    pub agent_name: Option<String>,
    #[serde(default)]
    pub limit: Option<u64>,
    #[serde(default)]
    pub recent_first: Option<bool>,
    #[serde(default)]
    pub role: Option<String>,
    #[serde(default)]
    pub tool_name: Option<String>,
    #[serde(default)]
    pub tool_namespace: Option<String>,
    #[serde(default)]
    pub window_id: Option<String>,
    #[serde(default)]
    pub max_chars_per_item: Option<u64>,
}

/// `read_item` 参数；窗口与条目 ID 是必填的官方字段
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct CodexHistoryReadItemArguments {
    #[serde(default)]
    pub agent_name: Option<String>,
    pub item_id: String,
    #[serde(default)]
    pub offset_chars: Option<u64>,
    #[serde(default)]
    pub limit_chars: Option<u64>,
    pub window_id: String,
}

/// `search_contents` 参数
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct CodexHistorySearchContentsArguments {
    #[serde(default)]
    pub agent_name: Option<String>,
    #[serde(default)]
    pub limit: Option<u64>,
    #[serde(default)]
    pub recent_first: Option<bool>,
    #[serde(default)]
    pub role: Option<String>,
    #[serde(default)]
    pub tool_name: Option<String>,
    #[serde(default)]
    pub tool_namespace: Option<String>,
    #[serde(default)]
    pub window_id: Option<String>,
    pub query: String,
}

/// 解析后的四工具参数
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodexHistoryToolArguments {
    ListWindows(CodexHistoryListWindowsArguments),
    ListItems(CodexHistoryListItemsArguments),
    ReadItem(CodexHistoryReadItemArguments),
    SearchContents(CodexHistorySearchContentsArguments),
}

/// 一次 history 工具调用的 wire 事实
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexHistoryToolRequest {
    pub tool: CodexHistoryTool,
    /// 客户端注入的 `context.current_agent_name`；缺失时由 Core 按根 agent 解释
    pub current_agent_name: Option<String>,
    pub arguments: CodexHistoryToolArguments,
}

/// 解析 history 工具调用正文
///
/// 官方客户端会把空参数正文替换为 `{}`，并注入 `context`；
/// 这里保持同一读法，`context` 只用于当前 agent 投影
///
/// # Errors
///
/// 正文不是 JSON 对象，或所需字段类型与必填项不符合官方 schema
pub fn parse_codex_history_tool_request(
    tool: CodexHistoryTool,
    body: &[u8],
) -> Result<CodexHistoryToolRequest, CodexHistoryWireError> {
    let value: Value = if body.trim_ascii().is_empty() {
        Value::Object(Map::new())
    } else {
        serde_json::from_slice(body).map_err(|_| CodexHistoryWireError::InvalidJson)?
    };
    if !value.is_object() {
        return Err(CodexHistoryWireError::NotAnObject);
    }
    let current_agent_name = value
        .get("context")
        .and_then(Value::as_object)
        .and_then(|context| context.get("current_agent_name"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    let arguments = match tool {
        CodexHistoryTool::ListWindows => {
            CodexHistoryToolArguments::ListWindows(decode_arguments(tool, value)?)
        }
        CodexHistoryTool::ListItems => {
            CodexHistoryToolArguments::ListItems(decode_arguments(tool, value)?)
        }
        CodexHistoryTool::ReadItem => {
            CodexHistoryToolArguments::ReadItem(decode_arguments(tool, value)?)
        }
        CodexHistoryTool::SearchContents => {
            CodexHistoryToolArguments::SearchContents(decode_arguments(tool, value)?)
        }
    };
    Ok(CodexHistoryToolRequest {
        tool,
        current_agent_name,
        arguments,
    })
}

fn decode_arguments<T: for<'de> Deserialize<'de>>(
    tool: CodexHistoryTool,
    value: Value,
) -> Result<T, CodexHistoryWireError> {
    serde_json::from_value(value).map_err(|_| CodexHistoryWireError::InvalidArguments(tool.name()))
}

/// 工具输出预算；官方以 `{"mode":...,"limit":...}` 传到下游
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodexToolOutputTruncationPolicy {
    Bytes(u64),
    Tokens(u64),
}

impl CodexToolOutputTruncationPolicy {
    /// 官方 wire 值只有 `bytes` 与 `tokens`
    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        #[derive(Deserialize)]
        struct Wire {
            mode: String,
            limit: i64,
        }
        let wire: Wire = serde_json::from_str(value).ok()?;
        let limit = u64::try_from(wire.limit).ok()?;
        match wire.mode.as_str() {
            "bytes" => Some(Self::Bytes(limit)),
            "tokens" => Some(Self::Tokens(limit)),
            _ => None,
        }
    }

    /// 官方把 token 预算按固定比例折算成字节预算
    #[must_use]
    pub const fn byte_budget(self) -> u64 {
        match self {
            Self::Bytes(bytes) => bytes,
            Self::Tokens(tokens) => tokens.saturating_mul(APPROX_BYTES_PER_TOKEN),
        }
    }

    /// 原始预算值，模式由 [`Self::byte_budget`] 解释
    #[must_use]
    pub const fn limit(self) -> u64 {
        match self {
            Self::Bytes(limit) | Self::Tokens(limit) => limit,
        }
    }
}

/// `list_windows` 结果
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CodexHistoryWindowListResult {
    pub agent_name: String,
    pub window_count: usize,
    pub total_windows: usize,
    pub truncated: bool,
    pub windows: Vec<CodexHistoryWindowEntry>,
}

/// 一个窗口的 wire 投影
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CodexHistoryWindowEntry {
    pub window_id: String,
    pub agent_name: String,
    pub item_count: u64,
}

/// `list_items` 与 `search_contents` 结果
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CodexHistoryItemListResult {
    pub window_id: Option<String>,
    pub item_count: usize,
    pub returned: usize,
    pub truncated: bool,
    /// 只有 `list_items` 回显该参数
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_chars_per_item: Option<u64>,
    pub items: Vec<CodexHistoryItemEntry>,
}

/// 一个条目的 wire 投影
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CodexHistoryItemEntry {
    pub item_id: String,
    pub window_id: String,
    pub agent_name: String,
    pub role: Option<String>,
    pub content_type: String,
    pub tool_namespace: Option<String>,
    pub tool_name: Option<String>,
    pub ordinal: u64,
    pub readable: bool,
    pub n_chars: u64,
    pub truncated_content: Option<String>,
}

impl CodexHistoryItemEntry {
    /// 官方 description 要求条目内容后附 `[id: ...]` 标记；read_item 原样接受该 ID
    ///
    /// 不可读对象没有内容，因此也没有标记；截断只作用于内容，不切割 JSON
    // 参数与官方结果字段一一对应，另建分组结构只会复制同一组字段
    #[expect(clippy::too_many_arguments)]
    #[must_use]
    pub fn new(
        item_id: String,
        window_id: String,
        agent_name: String,
        role: Option<String>,
        content_type: String,
        tool_namespace: Option<String>,
        tool_name: Option<String>,
        ordinal: u64,
        readable: bool,
        n_chars: u64,
        truncated_content: Option<String>,
    ) -> Self {
        let truncated_content =
            truncated_content.map(|content| format!("{content}\n[id: {item_id}]"));
        Self {
            item_id,
            window_id,
            agent_name,
            role,
            content_type,
            tool_namespace,
            tool_name,
            ordinal,
            readable,
            n_chars,
            truncated_content,
        }
    }
}

/// `read_item` 的可读结果
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CodexHistoryReadItemResult {
    pub window_id: String,
    pub item_id: String,
    pub content_type: String,
    pub readable: bool,
    pub n_chars: u64,
    pub offset_chars: u64,
    pub next_offset_chars: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
    /// 附件与正文分离；客户端把它们转成 `input_image`
    #[serde(skip_serializing_if = "Option::is_none")]
    pub images: Option<Vec<CodexHistoryImageAttachment>>,
    /// 无法内联承载的图片引用数，例如只有 file id 的对象
    #[serde(skip_serializing_if = "Option::is_none")]
    pub omitted_images: Option<u32>,
    /// 不可读对象的说明；可读结果不带该字段
    #[serde(skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

impl CodexHistoryReadItemResult {
    /// 可读结果；图片附件由调用方从原始条目投影后传入
    // 参数与官方结果字段一一对应，附件已单独成组
    #[expect(clippy::too_many_arguments)]
    #[must_use]
    pub fn readable(
        window_id: String,
        item_id: String,
        content_type: String,
        n_chars: u64,
        offset_chars: u64,
        next_offset_chars: u64,
        content: String,
        attachments: CodexHistoryAttachments,
    ) -> Self {
        let CodexHistoryAttachments { images, omitted } = attachments;
        Self {
            window_id,
            item_id,
            content_type,
            readable: true,
            n_chars,
            offset_chars,
            next_offset_chars,
            content: Some(content),
            images: (!images.is_empty()).then_some(images),
            omitted_images: (omitted > 0).then_some(omitted),
            note: None,
        }
    }

    /// 不可读对象；只报告类型与不可读事实，不伪造明文
    #[must_use]
    pub fn opaque(
        window_id: String,
        item_id: String,
        content_type: String,
        attachments: CodexHistoryAttachments,
    ) -> Self {
        let CodexHistoryAttachments { images, omitted } = attachments;
        Self {
            window_id,
            item_id,
            content_type,
            readable: false,
            n_chars: 0,
            offset_chars: 0,
            next_offset_chars: 0,
            content: None,
            images: (!images.is_empty()).then_some(images),
            omitted_images: (omitted > 0).then_some(omitted),
            note: Some(OPAQUE_ITEM_NOTE.to_owned()),
        }
    }
}

/// 不可读对象的固定说明
pub const OPAQUE_ITEM_NOTE: &str = "opaque object retained without a readable plaintext projection";

/// `read_item` 未命中；不跨窗口兜底
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CodexHistoryItemNotFoundResult {
    pub error: &'static str,
    pub window_id: String,
    pub item_id: String,
    pub window_found: bool,
    pub item_found: bool,
}

impl CodexHistoryItemNotFoundResult {
    #[must_use]
    pub fn new(window_id: String, item_id: String, window_found: bool) -> Self {
        Self {
            error: "item_not_found",
            window_id,
            item_id,
            window_found,
            item_found: false,
        }
    }
}

/// 官方 `images` 条目；客户端用 `data:{mime_type};base64,{data}` 还原附件
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CodexHistoryImageAttachment {
    pub data: String,
    pub mime_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// 条目内联图片投影
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CodexHistoryAttachments {
    pub images: Vec<CodexHistoryImageAttachment>,
    /// 无法内联承载的图片引用数
    pub omitted: u32,
}

/// 从原始条目 JSON 提取内联图片
///
/// 只接受 `data:<mime>;base64,<payload>` 形式的 `input_image`；
/// file 引用与远程 URL 不能由官方 `images` 结构承载，按省略计数
#[must_use]
pub fn codex_history_item_attachments(raw_object: &str) -> CodexHistoryAttachments {
    let mut attachments = CodexHistoryAttachments::default();
    if let Ok(value) = serde_json::from_str::<Value>(raw_object) {
        collect_image_attachments(&value, &mut attachments);
    }
    attachments
}

fn collect_image_attachments(value: &Value, attachments: &mut CodexHistoryAttachments) {
    match value {
        Value::Array(items) => {
            for item in items {
                collect_image_attachments(item, attachments);
            }
        }
        Value::Object(object) => {
            if object.get("type").and_then(Value::as_str) == Some("input_image") {
                match inline_image_attachment(object) {
                    Some(image) => attachments.images.push(image),
                    None => attachments.omitted = attachments.omitted.saturating_add(1),
                }
            }
            for child in object.values() {
                collect_image_attachments(child, attachments);
            }
        }
        _ => {}
    }
}

fn inline_image_attachment(object: &Map<String, Value>) -> Option<CodexHistoryImageAttachment> {
    let image_url = object.get("image_url").and_then(Value::as_str)?;
    let (mime_type, data) = image_url
        .strip_prefix("data:")
        .and_then(|rest| rest.split_once(";base64,"))?;
    if mime_type.is_empty() || data.is_empty() {
        return None;
    }
    let detail = object
        .get("detail")
        .and_then(Value::as_str)
        .filter(|detail| matches!(*detail, "auto" | "low" | "high" | "original"))
        .map(ToOwned::to_owned);
    Some(CodexHistoryImageAttachment {
        data: data.to_owned(),
        mime_type: mime_type.to_owned(),
        detail,
    })
}

/// 条目的完整语义身份
///
/// 官方客户端把响应产出条目回填到下一轮输入时会去掉 `id`/`status`，并把文本分段类型
/// 在 `output_text` 与 `input_text` 之间切换；这些只是同一对象的承载差异
/// 其余字段全部参与比较：密文、图片 `image_url`、工具参数与附件引用都保持原样，
/// 因此两份可读投影相同但图片或密文不同的对象不会被当成同一条
/// 返回值只用于同一位置的对象比较，不能用于全局去重或替换已保存的原对象
#[must_use]
pub fn codex_history_item_semantics(raw_object: &str) -> Option<String> {
    let value = serde_json::from_str::<Value>(raw_object).ok()?;
    serde_json::to_string(&normalized_semantics(value)).ok()
}

/// 去掉已知承载字段并统一文本分段类型；空值不改变语义
fn normalized_semantics(value: Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(items.into_iter().map(normalized_semantics).collect()),
        Value::Object(mut object) => {
            let _ = object.remove("id");
            let _ = object.remove("status");
            let text_part = matches!(
                object.get("type").and_then(Value::as_str),
                Some("input_text" | "output_text")
            );
            if text_part {
                // 官方文本分段在请求与响应两端使用不同类型名，注解与 logprobs 不承载业务内容
                let _ = object.insert("type".to_owned(), Value::String("text".to_owned()));
                let _ = object.remove("annotations");
                let _ = object.remove("logprobs");
            }
            // 键序不是语义：按名称排序后序列化，比较与摘要不受 JSON 字段顺序影响
            let normalized = object
                .into_iter()
                .filter(|(_, value)| !value.is_null())
                .map(|(name, value)| (name, normalized_semantics(value)))
                .collect::<BTreeMap<_, _>>();
            Value::Object(normalized.into_iter().collect())
        }
        other => other,
    }
}

/// 条目声明的官方不透明 ID
///
/// 上游声明了就原样返回；没有 ID 的对象由采集方按窗口位置生成稳定 ID，不自造上游身份
#[must_use]
pub fn codex_history_declared_item_id(raw_object: &str) -> Option<String> {
    serde_json::from_str::<Value>(raw_object)
        .ok()?
        .get("id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

/// 把 managed 会话的 history 检索查询声明改成明文
///
/// 只删除官方 `history` namespace 中 `search_contents` 的 `query.encrypted` 标记，
/// 让模型产生本地可检索的明文词；不改工具名、描述、其它字段，也不动 notes 三个加密字段
/// 返回是否真的改写了出站正文
#[must_use]
pub fn project_local_history_search_query(body: &mut Map<String, Value>) -> bool {
    let Some(tools) = body.get_mut("tools").and_then(Value::as_array_mut) else {
        return false;
    };
    let mut changed = false;
    for tool in tools.iter_mut() {
        if tool.get("type").and_then(Value::as_str) != Some("namespace")
            || tool.get("name").and_then(Value::as_str) != Some(HISTORY_TOOL_NAMESPACE)
        {
            continue;
        }
        let Some(functions) = tool.get_mut("tools").and_then(Value::as_array_mut) else {
            continue;
        };
        for function in functions.iter_mut() {
            if function.get("name").and_then(Value::as_str)
                != Some(CodexHistoryTool::SearchContents.name())
            {
                continue;
            }
            let Some(query) = function
                .pointer_mut("/parameters/properties/query")
                .and_then(Value::as_object_mut)
            else {
                continue;
            };
            changed |= query.remove("encrypted").is_some();
        }
    }
    changed
}

/// 条目的明文可读投影与检索元数据
///
/// `text` 只含明文部分；整段需要解密的对象为 None，原始 JSON 由调用方按原值保存
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexHistoryItemProjection {
    /// 官方条目类型；缺失但带 role 时按 message 解释
    pub content_type: String,
    /// 官方角色 wire 值；无法归属的对象为 None
    pub role: Option<&'static str>,
    pub tool_namespace: Option<String>,
    pub tool_name: Option<String>,
    /// 明文可读投影，用于检索与 read_item 的范围读取
    pub text: Option<String>,
}

/// 投影一条原始条目
#[must_use]
pub fn codex_history_item_projection(raw_object: &str) -> CodexHistoryItemProjection {
    let value: Value = serde_json::from_str(raw_object).unwrap_or(Value::Null);
    let object = value.as_object();
    let declared_type = object
        .and_then(|object| object.get("type"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let content_type = declared_type
        .map(ToOwned::to_owned)
        .or_else(|| {
            object
                .and_then(|object| object.get("role"))
                .and_then(Value::as_str)
                .map(|_| "message".to_owned())
        })
        .unwrap_or_else(|| "unknown".to_owned());
    let role = match content_type.as_str() {
        "message" => object
            .and_then(|object| object.get("role"))
            .and_then(Value::as_str)
            .and_then(history_role),
        "agent_message" => Some("assistant"),
        "function_call"
        | "custom_tool_call"
        | "local_shell_call"
        | "tool_search_call"
        | "web_search_call"
        | "image_generation_call"
        | "reasoning" => Some("assistant"),
        "function_call_output" | "custom_tool_call_output" => Some("tool"),
        _ => None,
    };
    let tool_namespace = object
        .and_then(|object| object.get("namespace"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);
    let tool_name = match content_type.as_str() {
        "function_call" | "custom_tool_call" => object
            .and_then(|object| object.get("name"))
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned),
        _ => None,
    };
    let text = object.and_then(|object| {
        projected_text(
            object,
            content_type.as_str(),
            tool_namespace.as_deref(),
            tool_name.as_deref(),
        )
    });
    CodexHistoryItemProjection {
        content_type,
        role,
        tool_namespace,
        tool_name,
        text,
    }
}

fn projected_text(
    object: &Map<String, Value>,
    content_type: &str,
    tool_namespace: Option<&str>,
    tool_name: Option<&str>,
) -> Option<String> {
    match content_type {
        "message" => message_text(object),
        "agent_message" => agent_message_text(object),
        "reasoning" => reasoning_summary_text(object),
        "function_call" => function_call_text(object, tool_namespace, tool_name),
        "function_call_output" | "custom_tool_call_output" => {
            function_output_text(object.get("output"))
        }
        "custom_tool_call" => string_field(object.get("input")),
        _ => None,
    }
}

fn message_text(object: &Map<String, Value>) -> Option<String> {
    match object.get("content") {
        Some(Value::String(text)) => non_empty_text(text),
        Some(Value::Array(parts)) => join_text_parts(parts.iter()),
        _ => None,
    }
}

fn agent_message_text(object: &Map<String, Value>) -> Option<String> {
    let parts = object.get("content")?.as_array()?;
    // 与非 OpenAI provider 的客户端规则一致：存在密文分段时没有完整的明文投影
    if parts
        .iter()
        .any(|part| part.get("type").and_then(Value::as_str) == Some("encrypted_content"))
    {
        return None;
    }
    join_text_parts(parts.iter())
}

fn reasoning_summary_text(object: &Map<String, Value>) -> Option<String> {
    let summary = object.get("summary")?.as_array()?;
    join_text_parts(summary.iter())
}

fn function_call_text(
    object: &Map<String, Value>,
    tool_namespace: Option<&str>,
    tool_name: Option<&str>,
) -> Option<String> {
    let arguments = object.get("arguments").and_then(Value::as_str)?;
    let declared: Vec<&str> = object
        .get("encrypted_function_args")
        .and_then(Value::as_array)
        .map(|names| names.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    let known = known_encrypted_arguments(tool_namespace, tool_name);
    if declared.is_empty() && known.is_empty() {
        return non_empty_text(arguments);
    }
    // 已声明或已知存在密文字段时，参数不是对象就无法证明该段可读
    let Ok(Value::Object(mut arguments)) = serde_json::from_str::<Value>(arguments) else {
        return None;
    };
    arguments
        .retain(|name, _| !declared.contains(&name.as_str()) && !known.contains(&name.as_str()));
    if arguments.is_empty() {
        return None;
    }
    serde_json::to_string(&Value::Object(arguments)).ok()
}

/// 固定 notes 工具 schema 中带 `encrypted` 声明的参数
///
/// 官方客户端不保证把这些名字重复写进 `encrypted_function_args`，因此按已知
/// namespace、工具名与参数名识别；未知工具不凭字符串形状猜测密文
fn known_encrypted_arguments(
    namespace: Option<&str>,
    tool: Option<&str>,
) -> &'static [&'static str] {
    match (namespace, tool) {
        (Some("notes"), Some("search_contents")) => &["query"],
        (Some("notes"), Some("write_file" | "append_to_file")) => &["text"],
        _ => &[],
    }
}

fn function_output_text(output: Option<&Value>) -> Option<String> {
    match output {
        Some(Value::String(text)) => non_empty_text(text),
        Some(Value::Array(parts)) => join_text_parts(parts.iter()),
        _ => None,
    }
}

/// 拼接明文文本分段；图片、音频与密文分段不进入正文
///
/// `summary_text` 是官方 reasoning summary 的分段类型，与消息文本同样可读
fn join_text_parts<'a>(parts: impl Iterator<Item = &'a Value>) -> Option<String> {
    let mut segments = Vec::new();
    for part in parts {
        let kind = part.get("type").and_then(Value::as_str);
        if !matches!(kind, Some("input_text" | "output_text" | "summary_text")) {
            continue;
        }
        if let Some(text) = part.get("text").and_then(Value::as_str) {
            segments.push(text);
        }
    }
    let joined = segments.join("\n");
    non_empty_text(&joined)
}

fn string_field(value: Option<&Value>) -> Option<String> {
    value.and_then(Value::as_str).and_then(non_empty_text)
}

fn non_empty_text(text: &str) -> Option<String> {
    (!text.trim().is_empty()).then(|| text.to_owned())
}

fn history_role(value: &str) -> Option<&'static str> {
    match value {
        "user" => Some("user"),
        "assistant" => Some("assistant"),
        "tool" => Some("tool"),
        "system" => Some("system"),
        "developer" => Some("developer"),
        _ => None,
    }
}
