//! Codex 本地 history 四工具的范围裁决、过滤、排序与字符范围读取
//!
//! 存储只提供按持久顺序排列的窗口与条目事实，本模块决定本地还是上游、
//! agent 与窗口的授权范围、页大小以及输出预算
//! 不可读对象只报告类型与不可读事实，不生成也不推断明文投影
//! 跨 agent 的读取始终限制在同一 Client Key 与逻辑会话内

use std::{collections::HashMap, fmt, sync::Arc};

use thiserror::Error;

use crate::{
    policy::ClientApiKeyId,
    provider_ports::{
        CodexHistoryItem, CodexHistoryRole, CodexHistoryStoredItem, CodexHistoryStoredWindow,
        CodexNativeWindowId, CodexSessionContentPort, CodexSessionStatePort,
        ProviderSessionAffinityKey, ProviderStoreError,
    },
};

/// 官方根 agent 名；窗口或条目未声明 agent 时按根 agent 解释
pub const CODEX_ROOT_AGENT_NAME: &str = "/root";
/// limit 缺省时的页大小
pub const DEFAULT_HISTORY_PAGE_LIMIT: u64 = 50;
/// 官方模型默认输出预算；客户端未给出截断策略时使用 bytes 10000
pub const DEFAULT_HISTORY_OUTPUT_BYTES: u64 = 10_000;

/// 结果外壳预留字节
const PAGE_RESULT_ENVELOPE_BYTES: u64 = 512;
/// 每条条目投影的固定开销估算，覆盖字段名与非内容值
const ITEM_RESULT_ENVELOPE_BYTES: u64 = 256;
/// 每个窗口投影的固定开销估算
const WINDOW_RESULT_ENVELOPE_BYTES: u64 = 128;

/// 查询参数问题与存储故障必须区分；存储故障不能降级成空结果
#[derive(Debug, Error)]
pub enum CodexHistoryQueryError {
    /// 模型给出的参数不在官方合同内
    #[error("invalid history tool argument `{0}`")]
    InvalidArgument(&'static str),
    /// 读取持久事实失败；这是错误而不是无匹配
    #[error(transparent)]
    Store(#[from] ProviderStoreError),
}

/// 本地回答还是维持既有上游行为
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodexHistoryRoute {
    /// 已确认从首次新建完整采集的会话，由本地持久事实回答
    Local,
    /// legacy、来源未判定与未认领会话继续使用既有上游 history
    Upstream,
}

/// 单次查询的输出预算
///
/// Protocol 把 `x-openai-tool-output-truncation-policy` 折算成字节上限；
/// 本层不解释 token，只按字符与 UTF-8 字节控制可读投影
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CodexHistoryQueryBudget {
    byte_limit: u64,
}

impl CodexHistoryQueryBudget {
    #[must_use]
    pub const fn new(byte_limit: u64) -> Self {
        Self { byte_limit }
    }

    /// 官方模型默认预算
    #[must_use]
    pub const fn default_output() -> Self {
        Self {
            byte_limit: DEFAULT_HISTORY_OUTPUT_BYTES,
        }
    }

    #[must_use]
    pub const fn byte_limit(self) -> u64 {
        self.byte_limit
    }

    /// 条目内容可用的字节上限；外壳与固定字段开销从这里预留
    fn content_bytes(self) -> usize {
        usize::try_from(self.byte_limit.saturating_sub(PAGE_RESULT_ENVELOPE_BYTES))
            .unwrap_or(usize::MAX)
    }

    /// 缺省内容上限按字符计；单个字符至少占一个字节，因此字符预算不会超过字节上限
    fn content_chars(self) -> usize {
        usize::try_from(self.byte_limit).unwrap_or(usize::MAX)
    }
}

/// 已认证的 history 读取范围
///
/// Key 与逻辑会话来自 Codex 辅助请求的认证事实，会话 ID 本身不是权限凭据
#[derive(Debug, Clone, Copy)]
pub struct CodexHistoryScope<'a> {
    pub key_id: &'a ClientApiKeyId,
    pub session_key: &'a ProviderSessionAffinityKey,
    /// 官方 `context.current_agent_name`；缺失时调用方传根 agent
    pub current_agent_name: &'a str,
}

/// list_windows 参数
#[derive(Debug, Clone, Default)]
pub struct CodexHistoryWindowQuery {
    /// 官方 agent 名；省略表示当前 agent，可以是绝对名或相对当前 agent 的名字
    pub agent_name: Option<String>,
    pub limit: Option<u64>,
    pub recent_first: bool,
}

/// list_items 与 search_contents 共用的范围、排序与页大小参数
#[derive(Debug, Clone, Default)]
pub struct CodexHistoryFilterQuery {
    pub agent_name: Option<String>,
    /// 官方完整窗口 ID；省略表示全部窗口
    pub window_id: Option<String>,
    pub role: Option<CodexHistoryRole>,
    pub tool_name: Option<String>,
    pub tool_namespace: Option<String>,
    pub limit: Option<u64>,
    pub recent_first: bool,
}

impl CodexHistoryFilterQuery {
    /// 设置官方 role 过滤
    ///
    /// 未知取值返回参数错误而不是静默忽略过滤；省略与 `null` 都表示不过滤
    ///
    /// # Errors
    ///
    /// role 不属于官方五个取值
    pub fn set_role(&mut self, role: Option<&str>) -> Result<(), CodexHistoryQueryError> {
        self.role = match role.map(str::trim).filter(|value| !value.is_empty()) {
            Some(value) => Some(
                CodexHistoryRole::from_wire(value)
                    .ok_or(CodexHistoryQueryError::InvalidArgument("role"))?,
            ),
            None => None,
        };
        Ok(())
    }
}

/// list_items 参数
#[derive(Debug, Clone, Default)]
pub struct CodexHistoryListItemsQuery {
    pub filter: CodexHistoryFilterQuery,
    /// 每条 `truncated_content` 的字符上限；同时受输出预算约束
    pub max_chars_per_item: Option<u64>,
}

/// read_item 参数
#[derive(Debug, Clone)]
pub struct CodexHistoryReadQuery {
    pub agent_name: Option<String>,
    /// 官方完整窗口 ID，不做跨窗口兜底
    pub window_id: String,
    /// 列表返回的稳定条目 ID，原样传入
    pub item_id: String,
    pub offset_chars: Option<u64>,
    pub limit_chars: Option<u64>,
}

/// search_contents 参数
#[derive(Debug, Clone)]
pub struct CodexHistorySearchQuery {
    /// 大小写敏感的明文字面子串；空串没有检索意义，按参数错误拒绝
    pub query: String,
    pub filter: CodexHistoryFilterQuery,
}

/// 一个窗口的投影
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexHistoryWindowSummary {
    pub window_id: String,
    pub agent_name: String,
    /// 窗口内全部持久条目数，含不可读对象
    pub item_count: u64,
}

/// list_windows 结果
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexHistoryWindowList {
    /// 实际查询的 agent
    pub agent_name: String,
    /// 返回的窗口数
    pub window_count: usize,
    /// 该 agent 的全部窗口数
    pub total_windows: usize,
    /// 是否因 limit 或输出预算截断
    pub truncated: bool,
    pub windows: Vec<CodexHistoryWindowSummary>,
}

/// 一个条目的列表投影
#[derive(Clone, PartialEq, Eq)]
pub struct CodexHistoryItemSummary {
    /// 稳定条目 ID，read_item 原样使用
    pub item_id: String,
    pub window_id: String,
    pub agent_name: String,
    pub role: Option<CodexHistoryRole>,
    pub content_type: String,
    pub tool_namespace: Option<String>,
    pub tool_name: Option<String>,
    /// 存储在会话内分配的持久顺序
    pub ordinal: u64,
    /// 是否有明文可读投影
    pub readable: bool,
    /// 完整可读投影的字符数；不可读对象为 0
    pub n_chars: u64,
    /// 截断后的可读投影；不可读对象为 None
    pub truncated_content: Option<String>,
}

impl fmt::Debug for CodexHistoryItemSummary {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CodexHistoryItemSummary")
            .field("item_id", &self.item_id)
            .field("window_id", &self.window_id)
            .field("agent_name", &self.agent_name)
            .field("role", &self.role)
            .field("content_type", &self.content_type)
            .field("tool_namespace", &self.tool_namespace)
            .field("tool_name", &self.tool_name)
            .field("ordinal", &self.ordinal)
            .field("readable", &self.readable)
            .field("n_chars", &self.n_chars)
            .field(
                "truncated_content",
                &self.truncated_content.as_ref().map(|_| "[REDACTED]"),
            )
            .finish()
    }
}

/// list_items 与 search_contents 的结果
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexHistoryItemPage {
    /// 请求指定的窗口；None 表示未按窗口过滤
    pub window_id: Option<String>,
    /// 过滤后匹配的条目总数
    pub item_count: usize,
    pub returned: usize,
    pub truncated: bool,
    /// 本次实际生效的每条内容字符上限
    pub max_chars_per_item: u64,
    pub items: Vec<CodexHistoryItemSummary>,
}

/// read_item 的可读范围结果
#[derive(Clone, PartialEq, Eq)]
pub struct CodexHistoryItemRead {
    pub window_id: String,
    pub item_id: String,
    pub content_type: String,
    pub readable: bool,
    /// 完整可读投影的字符数
    pub n_chars: u64,
    pub offset_chars: u64,
    /// 下一次读取起点；已到末尾时等于 n_chars
    pub next_offset_chars: u64,
    /// 本段可读投影；不可读对象为 None
    pub content: Option<String>,
    /// 原始条目 JSON，仅供 Protocol 投影图片附件；不进入文本结果
    pub raw_object: String,
}

impl fmt::Debug for CodexHistoryItemRead {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CodexHistoryItemRead")
            .field("window_id", &self.window_id)
            .field("item_id", &self.item_id)
            .field("content_type", &self.content_type)
            .field("readable", &self.readable)
            .field("n_chars", &self.n_chars)
            .field("offset_chars", &self.offset_chars)
            .field("next_offset_chars", &self.next_offset_chars)
            .field("content", &self.content.as_ref().map(|_| "[REDACTED]"))
            .field("raw_object", &"[REDACTED]")
            .finish()
    }
}

/// read_item 结果；窗口或条目不存在时如实报告，不跨窗口兜底
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodexHistoryReadItemOutcome {
    Read(CodexHistoryItemRead),
    NotFound {
        /// 请求的窗口是否属于该 agent
        window_found: bool,
    },
}

/// 一次 history 工具调用的 Core 侧输入；四个操作共用同一授权范围
#[derive(Debug, Clone)]
pub enum CodexHistoryToolQuery {
    ListWindows(CodexHistoryWindowQuery),
    ListItems(CodexHistoryListItemsQuery),
    ReadItem(CodexHistoryReadQuery),
    SearchContents(CodexHistorySearchQuery),
}

/// 本地工具结果
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodexHistoryToolResult {
    Windows(CodexHistoryWindowList),
    Items(CodexHistoryItemPage),
    Read(CodexHistoryReadItemOutcome),
}

/// 工具调用由本地回答，或按既有行为转发上游
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodexHistoryToolOutcome {
    Completed(CodexHistoryToolResult),
    /// legacy、来源未判定与未认领会话保持既有上游 history
    Upstream,
}

/// 已解析 agent 的窗口目录与按持久顺序排列的条目
struct CodexHistoryView<'a> {
    windows: Vec<ResolvedWindow<'a>>,
    items: &'a [CodexHistoryStoredItem],
}

struct ResolvedWindow<'a> {
    window_id: &'a CodexNativeWindowId,
    stored: &'a CodexHistoryStoredWindow,
    agent_name: String,
}

impl<'a> CodexHistoryView<'a> {
    /// 窗口按首个条目顺序排列；与条目混排时仍保持持久顺序
    fn new(windows: &'a [CodexHistoryStoredWindow], items: &'a [CodexHistoryStoredItem]) -> Self {
        let mut first_ordinal: HashMap<&str, u64> = HashMap::new();
        for item in items {
            let window_id = item.window_id.expose_to_store();
            first_ordinal
                .entry(window_id)
                .and_modify(|ordinal| *ordinal = (*ordinal).min(item.ordinal))
                .or_insert(item.ordinal);
        }
        let mut resolved: Vec<ResolvedWindow<'a>> = windows
            .iter()
            .map(|window| ResolvedWindow {
                window_id: &window.window.window_id,
                stored: window,
                agent_name: declared_agent_name(window.window.agent_name.as_deref()),
            })
            .collect();
        resolved.sort_by(|left, right| {
            let left_key = (
                first_ordinal
                    .get(left.window_id.expose_to_store())
                    .copied()
                    .unwrap_or(u64::MAX),
                left.window_id.expose_to_store(),
            );
            let right_key = (
                first_ordinal
                    .get(right.window_id.expose_to_store())
                    .copied()
                    .unwrap_or(u64::MAX),
                right.window_id.expose_to_store(),
            );
            left_key.cmp(&right_key)
        });
        Self {
            windows: resolved,
            items,
        }
    }

    fn window(&self, window_id: &CodexNativeWindowId) -> Option<&ResolvedWindow<'a>> {
        self.windows
            .iter()
            .find(|window| *window.window_id == *window_id)
    }

    fn agent_of(&self, window_id: &CodexNativeWindowId) -> Option<&str> {
        self.window(window_id)
            .map(|window| window.agent_name.as_str())
    }
}

/// 本地 history 查询服务；只有 Managed 会话由本地回答
pub struct CodexHistoryService {
    pub(super) content: Arc<dyn CodexSessionContentPort>,
    pub(super) state: Arc<dyn CodexSessionStatePort>,
    /// 是否允许新会话进入托管；关闭时未判定会话按 legacy 固定
    pub(super) managed_admission: bool,
}

impl CodexHistoryService {
    /// 组合根唯一的 history 服务实例
    ///
    /// `managed_admission` 只决定新会话能否成为 managed：关闭时已 managed 的会话
    /// 继续本地采集与本地回答，不会因为开关变化倒退回旧上游域
    #[must_use]
    pub const fn new(
        content: Arc<dyn CodexSessionContentPort>,
        state: Arc<dyn CodexSessionStatePort>,
        managed_admission: bool,
    ) -> Self {
        Self {
            content,
            state,
            managed_admission,
        }
    }

    /// 判定四个 history 工具的响应来源
    ///
    /// 只有已确认从首次新建完整采集的会话由本地持久事实回答；
    /// legacy、来源未判定与未认领会话维持既有上游行为
    ///
    /// # Errors
    ///
    /// 读取会话状态失败时返回存储错误
    pub async fn route(
        &self,
        scope: &CodexHistoryScope<'_>,
    ) -> Result<CodexHistoryRoute, ProviderStoreError> {
        let local = self
            .uses_local_history(scope.key_id, scope.session_key)
            .await?;
        Ok(if local {
            CodexHistoryRoute::Local
        } else {
            CodexHistoryRoute::Upstream
        })
    }

    /// 判定来源并执行一次 history 工具调用
    ///
    /// 来源裁决与查询由同一入口保证，调用方不需要也不允许自行选择本地分支
    ///
    /// # Errors
    ///
    /// 读取会话状态失败、参数不在官方合同内或读取持久事实失败
    pub async fn execute(
        &self,
        scope: &CodexHistoryScope<'_>,
        query: &CodexHistoryToolQuery,
        budget: CodexHistoryQueryBudget,
    ) -> Result<CodexHistoryToolOutcome, CodexHistoryQueryError> {
        if self.route(scope).await? == CodexHistoryRoute::Upstream {
            return Ok(CodexHistoryToolOutcome::Upstream);
        }
        let result = match query {
            CodexHistoryToolQuery::ListWindows(query) => {
                CodexHistoryToolResult::Windows(self.list_windows(scope, query, budget).await?)
            }
            CodexHistoryToolQuery::ListItems(query) => {
                CodexHistoryToolResult::Items(self.list_items(scope, query, budget).await?)
            }
            CodexHistoryToolQuery::ReadItem(query) => {
                CodexHistoryToolResult::Read(self.read_item(scope, query, budget).await?)
            }
            CodexHistoryToolQuery::SearchContents(query) => {
                CodexHistoryToolResult::Items(self.search_contents(scope, query, budget).await?)
            }
        };
        Ok(CodexHistoryToolOutcome::Completed(result))
    }

    /// 列出 agent 的真实窗口与条目数
    ///
    /// # Errors
    ///
    /// 参数不在官方合同内或读取持久事实失败
    pub async fn list_windows(
        &self,
        scope: &CodexHistoryScope<'_>,
        query: &CodexHistoryWindowQuery,
        budget: CodexHistoryQueryBudget,
    ) -> Result<CodexHistoryWindowList, CodexHistoryQueryError> {
        let agent = resolve_agent_name(scope.current_agent_name, query.agent_name.as_deref())?;
        let limit = page_limit(query.limit)?;
        let windows = self
            .content
            .load_history_windows(scope.key_id, scope.session_key)
            .await?;
        let items = self
            .content
            .load_history_items(scope.key_id, scope.session_key)
            .await?;
        let view = CodexHistoryView::new(&windows, &items);

        let mut matched: Vec<&ResolvedWindow<'_>> = view
            .windows
            .iter()
            .filter(|window| window.agent_name == agent)
            .collect();
        if query.recent_first {
            matched.reverse();
        }
        let total_windows = matched.len();
        let content_bytes = budget.content_bytes() as u64;
        let mut used = 0u64;
        let mut summaries = Vec::new();
        for window in matched {
            let cost = WINDOW_RESULT_ENVELOPE_BYTES
                .saturating_add(window.window_id.expose_to_store().len() as u64)
                .saturating_add(window.agent_name.len() as u64);
            if !summaries.is_empty()
                && (summaries.len() as u64 >= limit || used.saturating_add(cost) > content_bytes)
            {
                break;
            }
            used = used.saturating_add(cost);
            summaries.push(CodexHistoryWindowSummary {
                window_id: window.window_id.expose_to_store().to_owned(),
                agent_name: window.agent_name.clone(),
                item_count: window.stored.item_count,
            });
        }
        let window_count = summaries.len();
        Ok(CodexHistoryWindowList {
            agent_name: agent,
            window_count,
            total_windows,
            truncated: window_count < total_windows,
            windows: summaries,
        })
    }

    /// 按窗口、agent、role 与工具过滤列出条目
    ///
    /// # Errors
    ///
    /// 参数不在官方合同内或读取持久事实失败
    pub async fn list_items(
        &self,
        scope: &CodexHistoryScope<'_>,
        query: &CodexHistoryListItemsQuery,
        budget: CodexHistoryQueryBudget,
    ) -> Result<CodexHistoryItemPage, CodexHistoryQueryError> {
        let agent =
            resolve_agent_name(scope.current_agent_name, query.filter.agent_name.as_deref())?;
        let limit = page_limit(query.filter.limit)?;
        let window = requested_window(query.filter.window_id.as_deref())?;
        let content_chars = content_char_cap(query.max_chars_per_item, budget)?;
        self.item_page(
            scope,
            &agent,
            &query.filter,
            window.as_ref(),
            limit,
            budget,
            content_chars,
            |_| true,
        )
        .await
    }

    /// 在明文可读投影上做大小写敏感的 literal 子串检索
    ///
    /// 真正无匹配返回空页；存储失败返回错误，不返回全库让模型自行查找
    ///
    /// # Errors
    ///
    /// 查询词为空、参数不在官方合同内或读取持久事实失败
    pub async fn search_contents(
        &self,
        scope: &CodexHistoryScope<'_>,
        query: &CodexHistorySearchQuery,
        budget: CodexHistoryQueryBudget,
    ) -> Result<CodexHistoryItemPage, CodexHistoryQueryError> {
        if query.query.is_empty() {
            return Err(CodexHistoryQueryError::InvalidArgument("query"));
        }
        let agent =
            resolve_agent_name(scope.current_agent_name, query.filter.agent_name.as_deref())?;
        let limit = page_limit(query.filter.limit)?;
        let window = requested_window(query.filter.window_id.as_deref())?;
        let content_chars = content_char_cap(None, budget)?;
        self.item_page(
            scope,
            &agent,
            &query.filter,
            window.as_ref(),
            limit,
            budget,
            content_chars,
            |item| {
                item.text
                    .as_deref()
                    .is_some_and(|text| text.contains(query.query.as_str()))
            },
        )
        .await
    }

    /// 读取单个条目的字符范围；不跨窗口兜底
    ///
    /// # Errors
    ///
    /// 参数不在官方合同内或读取持久事实失败
    pub async fn read_item(
        &self,
        scope: &CodexHistoryScope<'_>,
        query: &CodexHistoryReadQuery,
        budget: CodexHistoryQueryBudget,
    ) -> Result<CodexHistoryReadItemOutcome, CodexHistoryQueryError> {
        let agent = resolve_agent_name(scope.current_agent_name, query.agent_name.as_deref())?;
        let window = CodexNativeWindowId::try_new(query.window_id.clone())
            .map_err(|_| CodexHistoryQueryError::InvalidArgument("window_id"))?;
        let offset = usize::try_from(query.offset_chars.unwrap_or(0)).unwrap_or(usize::MAX);
        let limit = match query.limit_chars {
            Some(0) => return Err(CodexHistoryQueryError::InvalidArgument("limit_chars")),
            Some(limit) => usize::try_from(limit).unwrap_or(usize::MAX),
            None => budget.content_chars(),
        };
        let limit = limit.min(budget.content_chars());
        let windows = self
            .content
            .load_history_windows(scope.key_id, scope.session_key)
            .await?;
        let items = self
            .content
            .load_history_items(scope.key_id, scope.session_key)
            .await?;
        let view = CodexHistoryView::new(&windows, &items);
        let Some(resolved) = view.window(&window) else {
            return Ok(CodexHistoryReadItemOutcome::NotFound {
                window_found: false,
            });
        };
        if resolved.agent_name != agent {
            // window_found 只描述请求 agent 的窗口范围，不暗示其它 agent 的档案
            return Ok(CodexHistoryReadItemOutcome::NotFound {
                window_found: false,
            });
        }
        let Some(item) = view
            .items
            .iter()
            .find(|item| item.window_id == window && item.item.item_id == query.item_id)
        else {
            return Ok(CodexHistoryReadItemOutcome::NotFound { window_found: true });
        };

        let Some(text) = item.item.text.as_deref() else {
            return Ok(CodexHistoryReadItemOutcome::Read(CodexHistoryItemRead {
                window_id: window.expose_to_store().to_owned(),
                item_id: item.item.item_id.clone(),
                content_type: item.item.content_type.clone(),
                readable: false,
                n_chars: 0,
                offset_chars: 0,
                next_offset_chars: 0,
                content: None,
                raw_object: item.item.raw.clone(),
            }));
        };
        let n_chars = text.chars().count();
        let start = offset.min(n_chars);
        let (content, next_offset_chars) = char_range(text, start, limit, budget.content_bytes());
        Ok(CodexHistoryReadItemOutcome::Read(CodexHistoryItemRead {
            window_id: window.expose_to_store().to_owned(),
            item_id: item.item.item_id.clone(),
            content_type: item.item.content_type.clone(),
            readable: true,
            n_chars: n_chars as u64,
            offset_chars: start as u64,
            next_offset_chars,
            content: Some(content),
            raw_object: item.item.raw.clone(),
        }))
    }

    /// 加载会话视图后按范围与检索谓词分页
    // 查询维度彼此独立，另建载体只会把同一组参数再搬一次
    #[expect(clippy::too_many_arguments)]
    async fn item_page(
        &self,
        scope: &CodexHistoryScope<'_>,
        agent: &str,
        filter: &CodexHistoryFilterQuery,
        window: Option<&CodexNativeWindowId>,
        limit: u64,
        budget: CodexHistoryQueryBudget,
        content_chars: usize,
        matches: impl Fn(&CodexHistoryItem) -> bool,
    ) -> Result<CodexHistoryItemPage, CodexHistoryQueryError> {
        let windows = self
            .content
            .load_history_windows(scope.key_id, scope.session_key)
            .await?;
        let items = self
            .content
            .load_history_items(scope.key_id, scope.session_key)
            .await?;
        let view = CodexHistoryView::new(&windows, &items);
        Ok(item_page_of(
            &view,
            agent,
            filter,
            window,
            limit,
            budget,
            content_chars,
            &matches,
        ))
    }
}

/// 在已加载视图上过滤、排序并截断出一页
// 与 [`CodexHistoryService::item_page`] 同参数；两者共用一个过滤语义
#[expect(clippy::too_many_arguments)]
fn item_page_of(
    view: &CodexHistoryView<'_>,
    agent: &str,
    filter: &CodexHistoryFilterQuery,
    window: Option<&CodexNativeWindowId>,
    limit: u64,
    budget: CodexHistoryQueryBudget,
    content_chars: usize,
    matches: &impl Fn(&CodexHistoryItem) -> bool,
) -> CodexHistoryItemPage {
    let window_id = window.map(|window| window.expose_to_store().to_owned());
    if let Some(window) = window
        && view.agent_of(window) != Some(agent)
    {
        return empty_page(window_id, content_chars);
    }
    let tool_name = filter_string(filter.tool_name.as_deref());
    let tool_namespace = filter_string(filter.tool_namespace.as_deref());
    let mut selected: Vec<&CodexHistoryStoredItem> = view
        .items
        .iter()
        .filter(|item| window.is_none_or(|window| item.window_id == *window))
        .filter(|item| view.agent_of(&item.window_id) == Some(agent))
        .filter(|item| filter.role.is_none_or(|role| item.item.role == Some(role)))
        .filter(|item| {
            tool_namespace
                .is_none_or(|namespace| item.item.tool_namespace.as_deref() == Some(namespace))
        })
        .filter(|item| tool_name.is_none_or(|name| item.item.tool_name.as_deref() == Some(name)))
        .filter(|item| matches(&item.item))
        .collect();
    selected.sort_by_key(|item| item.ordinal);
    if filter.recent_first {
        selected.reverse();
    }
    let item_count = selected.len();
    let content_bytes = budget.content_bytes();
    let mut used = 0u64;
    let mut summaries = Vec::new();
    for item in selected {
        let text = item.item.text.as_deref();
        let content = text.map(|text| truncate_prefix(text, content_chars, content_bytes));
        let cost = ITEM_RESULT_ENVELOPE_BYTES
            .saturating_add(item.item.item_id.len() as u64)
            .saturating_add(content.as_ref().map_or(0, |content| content.len() as u64));
        if !summaries.is_empty()
            && (summaries.len() as u64 >= limit || used.saturating_add(cost) > content_bytes as u64)
        {
            break;
        }
        used = used.saturating_add(cost);
        summaries.push(CodexHistoryItemSummary {
            item_id: item.item.item_id.clone(),
            window_id: item.window_id.expose_to_store().to_owned(),
            agent_name: agent.to_owned(),
            role: item.item.role,
            content_type: item.item.content_type.clone(),
            tool_namespace: item.item.tool_namespace.clone(),
            tool_name: item.item.tool_name.clone(),
            ordinal: item.ordinal,
            readable: text.is_some(),
            n_chars: text.map_or(0, |text| text.chars().count() as u64),
            truncated_content: content,
        });
    }
    CodexHistoryItemPage {
        window_id,
        item_count,
        returned: summaries.len(),
        truncated: summaries.len() < item_count,
        max_chars_per_item: content_chars as u64,
        items: summaries,
    }
}

fn empty_page(window_id: Option<String>, content_chars: usize) -> CodexHistoryItemPage {
    CodexHistoryItemPage {
        window_id,
        item_count: 0,
        returned: 0,
        truncated: false,
        max_chars_per_item: content_chars as u64,
        items: Vec::new(),
    }
}

/// 条目的稳定 ID
///
/// 上游声明了 `id` 时原样保存；官方部分对象（如 compaction）没有 ID，
/// 此时由窗口身份与对齐位置生成稳定值，列表输出与 read_item 完全一致
/// 生成值不做哈希，同一窗口同一位置始终得到同一 ID，不同位置即使正文相同也不相同
#[must_use]
pub fn codex_history_item_id(
    declared: Option<&str>,
    window_id: &CodexNativeWindowId,
    position: u64,
) -> String {
    match declared.map(str::trim).filter(|value| !value.is_empty()) {
        Some(declared) => declared.to_owned(),
        None => format!("cprs:{}:{position}", window_id.expose_to_store()),
    }
}

/// 归一化官方 agent 名：相对名相对当前 agent，绝对名以 `/` 开头
///
/// 空组件与 `.` 被忽略，`..` 回到上一级且不会越过根
///
/// # Errors
///
/// 当前 agent 或请求名为空、归一化后没有有效组件
pub fn resolve_agent_name(
    current: &str,
    requested: Option<&str>,
) -> Result<String, CodexHistoryQueryError> {
    let current = canonical_agent_path(current)
        .ok_or(CodexHistoryQueryError::InvalidArgument("agent_name"))?;
    let Some(requested) = requested.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(current);
    };
    let joined = if requested.starts_with('/') {
        requested.to_owned()
    } else {
        format!("{current}/{requested}")
    };
    canonical_agent_path(&joined).ok_or(CodexHistoryQueryError::InvalidArgument("agent_name"))
}

/// 窗口或条目未声明 agent 时归到官方根 agent
fn declared_agent_name(declared: Option<&str>) -> String {
    declared
        .and_then(canonical_agent_path)
        .unwrap_or_else(|| CODEX_ROOT_AGENT_NAME.to_owned())
}

fn canonical_agent_path(value: &str) -> Option<String> {
    let mut components: Vec<&str> = Vec::new();
    for component in value.split('/') {
        match component {
            "" | "." => {}
            // 第一个组件是 agent 树根，向上不会越过它
            ".." => {
                if components.len() > 1 {
                    components.pop();
                }
            }
            other => components.push(other),
        }
    }
    if components.is_empty() {
        return None;
    }
    Some(format!("/{}", components.join("/")))
}

fn requested_window(
    window_id: Option<&str>,
) -> Result<Option<CodexNativeWindowId>, CodexHistoryQueryError> {
    window_id
        .map(|window_id| {
            CodexNativeWindowId::try_new(window_id)
                .map_err(|_| CodexHistoryQueryError::InvalidArgument("window_id"))
        })
        .transpose()
}

fn filter_string(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn page_limit(limit: Option<u64>) -> Result<u64, CodexHistoryQueryError> {
    match limit {
        None => Ok(DEFAULT_HISTORY_PAGE_LIMIT),
        Some(0) => Err(CodexHistoryQueryError::InvalidArgument("limit")),
        Some(limit) => Ok(limit),
    }
}

/// 每条内容的字符上限由工具参数与输出预算共同决定
fn content_char_cap(
    requested: Option<u64>,
    budget: CodexHistoryQueryBudget,
) -> Result<usize, CodexHistoryQueryError> {
    match requested {
        Some(0) => Err(CodexHistoryQueryError::InvalidArgument(
            "max_chars_per_item",
        )),
        Some(chars) => Ok(usize::try_from(chars)
            .unwrap_or(usize::MAX)
            .min(budget.content_chars())),
        None => Ok(budget.content_chars()),
    }
}

/// 取字符范围并返回下次读取起点
fn char_range(
    text: &str,
    offset_chars: usize,
    limit_chars: usize,
    byte_cap: usize,
) -> (String, u64) {
    let chunk: String = text.chars().skip(offset_chars).take(limit_chars).collect();
    let chunk = truncate_prefix(&chunk, limit_chars, byte_cap);
    let next = offset_chars.saturating_add(chunk.chars().count());
    (chunk, next as u64)
}

/// 按字符上限与 UTF-8 字节上限截断，结果落在字符边界
///
/// 至少保留一个字符，保证按 offset 继续读取时可以推进
fn truncate_prefix(text: &str, char_cap: usize, byte_cap: usize) -> String {
    let mut end_bytes = 0usize;
    for (kept, character) in text.chars().enumerate() {
        if kept >= char_cap {
            break;
        }
        let next = end_bytes.saturating_add(character.len_utf8());
        if next > byte_cap && kept > 0 {
            break;
        }
        end_bytes = next;
    }
    text[..end_bytes].to_owned()
}
