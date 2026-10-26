//! Codex 原生会话状态、history 条目与 notes 操作日志的持久事实端口
//!
//! history 与 notes 属于 Client Key 与逻辑会话，PostgreSQL 是唯一权威
//! 本层只承载后续采集、查询、按序重放与切换发布需要的最小事实
//! 不解释 HTTP 接管、发送权代次、上游加密结果或跨宿主恢复
//! 存储实现必须按 Key 与逻辑会话隔离，并在同一事务内完成各自的顺序或 CAS 裁决

use std::fmt;

use futures::future::BoxFuture;

use super::{ProviderSessionAffinityKey, ProviderStoreError, ProviderStoreErrorKind};
use crate::{account::ProviderAccountId, policy::ClientApiKeyId};

/// 官方 history 逻辑窗口 ID，形状固定为 `<thread_id>:<window_number>`
///
/// 窗口 ID 与独立的 context window UUID 不是同一值，两者不能互相派生
/// 存储与查询只使用这里解析一次的窗口身份
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CodexNativeWindowId(String);

impl CodexNativeWindowId {
    /// 校验官方窗口 ID 形状
    ///
    /// 官方 auto compact 窗口从 0 开始编号，因此序号 0 是合法的首窗口
    ///
    /// # Errors
    ///
    /// 缺少冒号、线程部分为空或序号不是十进制无符号整数时返回无效数据
    pub fn try_new(value: impl Into<String>) -> Result<Self, ProviderStoreError> {
        let value = value.into();
        let well_formed = value.rsplit_once(':').is_some_and(|(thread_id, number)| {
            !thread_id.trim().is_empty()
                && !number.is_empty()
                && number.bytes().all(|byte| byte.is_ascii_digit())
                && number.parse::<u64>().is_ok()
        });
        if !well_formed {
            return Err(ProviderStoreError::new(
                ProviderStoreErrorKind::InvalidData,
                "validate Codex history window id",
            ));
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn expose_to_store(&self) -> &str {
        &self.0
    }

    /// 窗口所属线程；官方把窗口序号追加在最后一个冒号之后
    #[must_use]
    pub fn thread_id(&self) -> &str {
        self.0
            .rsplit_once(':')
            .map_or("", |(thread_id, _)| thread_id)
    }

    #[must_use]
    pub fn window_number(&self) -> u64 {
        self.0
            .rsplit_once(':')
            .and_then(|(_, number)| number.parse().ok())
            .unwrap_or_default()
    }
}

/// notes 副本所在的存储域代次
///
/// 切换时在新域重建，标识全局唯一且不复用，因此只接受规范 UUID 文本
/// 不能用可回滚的递增计数代替，否则恢复旧快照后会重建到已用过的域
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CodexNotesGeneration(String);

impl CodexNotesGeneration {
    /// 校验存储域代次文本
    ///
    /// # Errors
    ///
    /// 非规范小写连字符 UUID 文本返回无效数据
    pub fn try_new(value: impl Into<String>) -> Result<Self, ProviderStoreError> {
        let value = value.into();
        let canonical = uuid::Uuid::parse_str(&value)
            .is_ok_and(|parsed| parsed.hyphenated().to_string() == value);
        if !canonical {
            return Err(ProviderStoreError::new(
                ProviderStoreErrorKind::InvalidData,
                "validate Codex notes generation",
            ));
        }
        Ok(Self(value))
    }

    /// 生成新的域代次；每次重建都必须取新值，不得复用曾发布过的域
    #[must_use]
    pub fn generate() -> Self {
        Self(uuid::Uuid::now_v7().hyphenated().to_string())
    }

    #[must_use]
    pub fn expose_to_store(&self) -> &str {
        &self.0
    }
}

/// notes 副本的落点：账号与可选的自建域
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexNotesReplica {
    account: ProviderAccountId,
    /// None 表示该账号原有的 notes 域，尚未在受管会话中重建
    generation: Option<CodexNotesGeneration>,
}

impl CodexNotesReplica {
    /// 账号原有 notes 域；legacy 会话与首次认领后的默认落点
    #[must_use]
    pub const fn native(account: ProviderAccountId) -> Self {
        Self {
            account,
            generation: None,
        }
    }

    /// 切换后在独立域重建的副本
    #[must_use]
    pub const fn rebuilt(account: ProviderAccountId, generation: CodexNotesGeneration) -> Self {
        Self {
            account,
            generation: Some(generation),
        }
    }

    #[must_use]
    pub const fn account(&self) -> &ProviderAccountId {
        &self.account
    }

    #[must_use]
    pub fn generation(&self) -> Option<&CodexNotesGeneration> {
        self.generation.as_ref()
    }
}

/// 会话的持久来源，决定是否可宣称 history 完整
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodexSessionOrigin {
    /// 认领时尚未判定是否从首次新建开始采集
    ///
    /// 首个模型请求或辅助请求到达时仍是该值；不得据此断定旧状态为空
    Unclassified,
    /// 已确认从首次新建开始完整采集
    Managed,
    /// 进入网关前已有云端状态，history 与 notes 不保证完整
    Legacy,
}

/// notes 重建的冻结目标与已确认进度
///
/// 只有连续已判定前缀中的完整响应操作按 seq 重放；目标之外的任何操作都阻止发布
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexNotesReplay {
    /// 重建目标所在的自建域
    pub replica: CodexNotesReplica,
    /// 冻结目标包含的最后一个操作 seq
    pub frozen_through_seq: u64,
    /// 已确认在目标域重放到的 seq
    pub replayed_through_seq: u64,
}

impl CodexNotesReplay {
    /// 校验冻结目标；目标必须是自建域，且已重放位置不越过冻结目标
    ///
    /// # Errors
    ///
    /// 目标是账号原生域或已重放位置越界时返回无效数据
    pub fn try_new(
        replica: CodexNotesReplica,
        frozen_through_seq: u64,
        replayed_through_seq: u64,
    ) -> Result<Self, ProviderStoreError> {
        if replica.generation().is_none() || replayed_through_seq > frozen_through_seq {
            return Err(ProviderStoreError::new(
                ProviderStoreErrorKind::InvalidData,
                "validate Codex notes replay target",
            ));
        }
        Ok(Self {
            replica,
            frozen_through_seq,
            replayed_through_seq,
        })
    }
}

/// 会话的持久状态
///
/// 当前执行账号只是短期位置；history 与 notes 日志属于 Key 与逻辑会话
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexSessionState {
    pub origin: CodexSessionOrigin,
    pub owner: ProviderAccountId,
    /// 每次成功发布切换后递增；切换发布以读到的值做 CAS
    pub generation: u64,
    /// 当前已发布的 notes 副本落点
    pub notes: CodexNotesReplica,
    /// 已冻结的重建目标与进度；稳定会话为 None
    pub notes_replay: Option<CodexNotesReplay>,
}

/// 切换发布输入
///
/// 冻结目标之后的验证由存储在发布事务内复验，不在网络期间持有事务
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexNotesPublish {
    pub key_id: ClientApiKeyId,
    pub session_key: ProviderSessionAffinityKey,
    /// 读取会话状态时观测到的代次；不匹配时按最新状态失败
    pub expected_generation: u64,
    /// 切换目标账号
    pub owner: ProviderAccountId,
    /// 目标账号在准备阶段观测到的凭据版本
    pub credential_revision: u64,
    /// 已在目标域重建完成的副本落点
    pub replica: CodexNotesReplica,
    /// 冻结目标包含的最后一个操作 seq
    pub through_seq: u64,
}

/// 切换发布结果；只有 `Published` 表示新 owner 与新域同时生效
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodexNotesPublishOutcome {
    Published(CodexSessionState),
    /// 并发切换已先发布；返回最新状态供调用方重新准备
    GenerationChanged(CodexSessionState),
    /// 账号行锁内发现凭据版本已变化，不能用旧身份发布
    IdentityChanged,
    /// 账号已不存在，或不再是可承载原生会话的 OAuth 账号
    Unavailable,
    /// 日志中仍有未终结操作，或存在冻结目标之外的操作
    ReplayNotReady,
}

/// 一次 notes 写入操作的类型；只有真实 write/append 进入日志
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodexNotesOperationKind {
    WriteFile,
    AppendToFile,
}

/// notes 操作的真实观察状态
///
/// 收到完整 HTTP 响应只说明传输完成，不是机器确认的业务成功
/// 因此不存在以 HTTP 200 命名业务已应用的状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodexNotesOperationStatus {
    /// 意图与 seq 已持久，尝试尚未开始
    Unsent,
    /// 已持久尝试开始事实，尚无终结观察；重启后仍停在此时表示结果未知
    InFlight,
    /// 收到完整响应，原始响应已保存，加密业务结果未判明
    Responded,
    /// 明确失败：发送前拒绝，或上游返回非成功响应；不据此声称无副作用
    Failed,
    /// 已开始尝试但结果不明，含响应不完整与断流
    Unknown,
}

impl CodexNotesOperationStatus {
    /// 终态不可重开；未终结的操作占住自己的 seq
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Responded | Self::Failed)
    }

    /// 只有收到完整响应的操作按 seq 重放到新域
    #[must_use]
    pub const fn needs_replay(self) -> bool {
        matches!(self, Self::Responded)
    }
}

/// 一次 notes 写入的真实记录
#[derive(Clone, PartialEq, Eq)]
pub struct CodexNotesOperation {
    /// 会话内单调序号，同会话内唯一
    pub seq: u64,
    /// 该操作实际发往的副本落点
    pub replica: CodexNotesReplica,
    /// 官方请求体 context.current_agent_name 的原值
    pub agent_name: String,
    pub kind: CodexNotesOperationKind,
    pub status: CodexNotesOperationStatus,
    /// 原始请求 JSON 正文，含加密参数与 context，重放时原样使用
    pub request: String,
    /// 原始响应 JSON 正文；未收到响应时为 None
    pub response: Option<String>,
    /// 上游 HTTP 状态；没有响应时为 None
    pub response_status: Option<u16>,
}

impl fmt::Debug for CodexNotesOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CodexNotesOperation")
            .field("seq", &self.seq)
            .field("replica", &self.replica)
            .field("agent_name", &self.agent_name)
            .field("kind", &self.kind)
            .field("status", &self.status)
            .field("response_status", &self.response_status)
            .field("request", &"[REDACTED]")
            .field("response", &self.response.as_ref().map(|_| "[REDACTED]"))
            .finish()
    }
}

/// 追加 notes 写入意图；seq 由存储在会话内原子分配
///
/// 不按正文或 body hash 去重；内容相同的两次请求是两次真实变更
#[derive(Clone, PartialEq, Eq)]
pub struct NewCodexNotesOperation {
    pub key_id: ClientApiKeyId,
    pub session_key: ProviderSessionAffinityKey,
    pub replica: CodexNotesReplica,
    pub agent_name: String,
    pub kind: CodexNotesOperationKind,
    /// 官方请求 JSON 正文原值，不重新序列化
    pub request: String,
}

impl fmt::Debug for NewCodexNotesOperation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("NewCodexNotesOperation")
            .field("key_id", &self.key_id)
            .field("session_key", &self.session_key)
            .field("replica", &self.replica)
            .field("agent_name", &self.agent_name)
            .field("kind", &self.kind)
            .field("request", &"[REDACTED]")
            .finish()
    }
}

/// notes 操作的终结观察；原始响应必须与状态在同一事务内落库
#[derive(Clone, PartialEq, Eq)]
pub struct CodexNotesCompletion {
    pub key_id: ClientApiKeyId,
    pub session_key: ProviderSessionAffinityKey,
    pub seq: u64,
    /// 只接受 Responded、Failed 与 Unknown
    pub status: CodexNotesOperationStatus,
    pub response: Option<String>,
    pub response_status: Option<u16>,
}

impl fmt::Debug for CodexNotesCompletion {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CodexNotesCompletion")
            .field("key_id", &self.key_id)
            .field("session_key", &self.session_key)
            .field("seq", &self.seq)
            .field("status", &self.status)
            .field("response_status", &self.response_status)
            .field("response", &self.response.as_ref().map(|_| "[REDACTED]"))
            .finish()
    }
}

/// list_items 过滤使用的官方角色集合；未知角色不猜测
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CodexHistoryRole {
    User,
    Assistant,
    Tool,
    System,
    Developer,
}

impl CodexHistoryRole {
    /// 按官方 wire 值解析角色；未知取值返回 None 而不是猜测
    #[must_use]
    pub fn from_wire(value: &str) -> Option<Self> {
        match value {
            "user" => Some(Self::User),
            "assistant" => Some(Self::Assistant),
            "tool" => Some(Self::Tool),
            "system" => Some(Self::System),
            "developer" => Some(Self::Developer),
            _ => None,
        }
    }

    /// 官方 wire 值；同时用于历史条目投影与过滤参数
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Assistant => "assistant",
            Self::Tool => "tool",
            Self::System => "system",
            Self::Developer => "developer",
        }
    }
}

/// 一个 history 条目的原始对象与可读投影
#[derive(Clone, PartialEq, Eq)]
pub struct CodexHistoryItem {
    /// 官方不透明条目 ID，read_item 直接使用，不得二次哈希
    pub item_id: String,
    pub role: Option<CodexHistoryRole>,
    pub tool_namespace: Option<String>,
    pub tool_name: Option<String>,
    /// 原始内容类型，如 message、function_call、reasoning
    pub content_type: String,
    /// 原始条目 JSON，含密文与附件，不重新序列化
    pub raw: String,
    /// 明文可读投影；需要解密才能理解的对象为 None
    pub text: Option<String>,
    /// 完整语义身份，由协议投影时计算
    ///
    /// 只用于同一窗口同一位置的对象比较：忽略 id/status 与文本分段类型等承载差异，
    /// 密文与图片字段参与其中；不参与全局去重，也不替代原对象
    pub identity: String,
}

impl fmt::Debug for CodexHistoryItem {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CodexHistoryItem")
            .field("item_id", &self.item_id)
            .field("role", &self.role)
            .field("tool_namespace", &self.tool_namespace)
            .field("tool_name", &self.tool_name)
            .field("content_type", &self.content_type)
            .field("raw", &"[REDACTED]")
            .field("text", &self.text.as_ref().map(|_| "[REDACTED]"))
            .field("identity", &self.identity)
            .finish()
    }
}

/// history 条目所属的逻辑窗口
///
/// 线程身份来自窗口 ID 本身，不重复保存第二份
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexHistoryWindow {
    pub window_id: CodexNativeWindowId,
    /// 官方 metadata 声明的 agent；None 表示未声明，root 归由采集方解释
    pub agent_name: Option<String>,
    /// 官方独立的 context window UUID，与窗口 ID 不是同一值
    pub context_window_id: Option<String>,
}

/// 一次模型请求输入的采集记录
///
/// 输入意图在上游发送前落库；同一请求可分多批追加条目
#[derive(Clone, PartialEq, Eq)]
pub struct CodexHistoryRequest {
    pub key_id: ClientApiKeyId,
    pub session_key: ProviderSessionAffinityKey,
    pub window: CodexHistoryWindow,
    /// 客户端本次续接的父响应原值；增量输入据此对齐已保存的全量
    pub previous_response_id: Option<String>,
    pub items: Vec<CodexHistoryItem>,
}

impl fmt::Debug for CodexHistoryRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CodexHistoryRequest")
            .field("key_id", &self.key_id)
            .field("session_key", &self.session_key)
            .field("window", &self.window)
            .field("previous_response_id", &self.previous_response_id)
            .field("items", &self.items)
            .finish()
    }
}

/// 一次上游响应产出的采集记录
///
/// 同一 response 可重复追加批次；条目顺序由存储按追加顺序固定
/// 空条目批次仍会建立 response 锚，覆盖预热与零输出响应
#[derive(Clone, PartialEq, Eq)]
pub struct CodexHistoryResponse {
    pub key_id: ClientApiKeyId,
    pub session_key: ProviderSessionAffinityKey,
    /// 上游 response ID 原值，同时是后续续接的查找键
    pub response_id: String,
    pub window: CodexHistoryWindow,
    /// 产生该响应时客户端续接的父响应，用于重启后还原续链来源
    pub previous_response_id: Option<String>,
    /// 这次响应所看到请求的逻辑输入条目引用，按请求顺序
    ///
    /// 只在首次锚定时写入；后续批次沿用同一来源，不改写已保存的输入
    pub input: Vec<String>,
    pub items: Vec<CodexHistoryItem>,
}

impl fmt::Debug for CodexHistoryResponse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CodexHistoryResponse")
            .field("key_id", &self.key_id)
            .field("session_key", &self.session_key)
            .field("response_id", &self.response_id)
            .field("window", &self.window)
            .field("previous_response_id", &self.previous_response_id)
            .field("input", &self.input)
            .field("items", &self.items)
            .finish()
    }
}

/// 已保存响应的定位事实，用于续接输入对齐
///
/// `previous_response_id` 还原该响应在续链中的来源；`input` 与 `output` 是该响应
/// 准确的逻辑输入与产出引用，续接时据此还原当时前缀，而不是接当前窗口尾部
/// 未知父响应不能被当成没有历史，客户端必须改发完整输入
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexHistoryResponseAnchor {
    pub response_id: String,
    pub window: CodexHistoryWindow,
    pub previous_response_id: Option<String>,
    /// 产生该响应时请求的逻辑输入条目引用，按请求顺序
    pub input: Vec<String>,
    /// 该响应产出的条目引用，按产出顺序追加
    pub output: Vec<String>,
}

/// 发送前保存的一次请求逻辑输入快照
///
/// 输入在发送前就已落库，但上游在建立响应锚之前失败时（例如额度拒绝），
/// 这些条目还没有进入任何响应锚；下一次无父响应的全量重发据此逐位置比较语义身份，
/// 复用失败 attempt 已保存的原条目，而不是按当前条目数再归档一次
/// 快照只描述该窗口最近一次发送前请求的逻辑输入，不充当窗口全部档案
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexHistoryPendingInput {
    pub window: CodexHistoryWindow,
    /// 该请求发送时客户端续接的父响应原值
    pub previous_response_id: Option<String>,
    /// 该请求的逻辑输入条目引用，按请求顺序
    pub input: Vec<String>,
}

/// 已落库条目的稳定顺序
#[derive(Clone, PartialEq, Eq)]
pub struct CodexHistoryStoredItem {
    pub window_id: CodexNativeWindowId,
    /// 存储在会话内分配的单调顺序，排序只按该值
    pub ordinal: u64,
    pub item: CodexHistoryItem,
}

impl fmt::Debug for CodexHistoryStoredItem {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CodexHistoryStoredItem")
            .field("window_id", &self.window_id)
            .field("ordinal", &self.ordinal)
            .field("item", &self.item)
            .finish()
    }
}

/// 一条已落库记录声明的窗口元数据
///
/// agent 与 context window UUID 来自同一条记录，必须成对保留：
/// 同一内部窗口恢复后再次推进可能保存多个 UUID，分别取最大值会拼出不存在的组合
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexHistoryWindowMetadata {
    /// 官方 metadata 声明的 agent；None 表示未声明
    pub agent_name: Option<String>,
    /// 官方独立的 context window UUID，与窗口 ID 不是同一值
    pub context_window_id: Option<String>,
}

/// 已落库的逻辑窗口目录
///
/// 目录、顺序与条目数只由已落库条目定义；元数据集合另外合并条目、响应锚与
/// 发送前快照中同一窗口的已持久事实，只用于解析查询别名，不参与计数
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexHistoryStoredWindow {
    pub window_id: CodexNativeWindowId,
    pub item_count: u64,
    /// 去重的已持久窗口元数据；空集表示该窗口没有声明 agent 与 UUID
    pub metadata: Vec<CodexHistoryWindowMetadata>,
}

/// 会话状态与切换发布的持久事实
///
/// 该端口与 [`CodexSessionOwnershipPort`](super::CodexSessionOwnershipPort) 共用同一条会话记录
/// 不另建可写 owner；未装配时 session_pool 按现有合同如实失败
pub trait CodexSessionStatePort: Send + Sync {
    /// 读取会话状态；会话尚未被认领时返回 None
    fn load_session<'a>(
        &'a self,
        key_id: &'a ClientApiKeyId,
        session_key: &'a ProviderSessionAffinityKey,
    ) -> BoxFuture<'a, Result<Option<CodexSessionState>, ProviderStoreError>>;

    /// 首次判定来源；已判定过后返回既有状态，不覆盖来源、owner 或 notes 落点
    ///
    /// `origin` 只接受 [`CodexSessionOrigin::Managed`] 或 [`CodexSessionOrigin::Legacy`]
    fn resolve_session_origin<'a>(
        &'a self,
        key_id: &'a ClientApiKeyId,
        session_key: &'a ProviderSessionAffinityKey,
        origin: CodexSessionOrigin,
    ) -> BoxFuture<'a, Result<CodexSessionState, ProviderStoreError>>;

    /// 保存重建冻结目标与已确认进度；同一会话只保留一个进行中的重建目标
    fn save_notes_replay<'a>(
        &'a self,
        key_id: &'a ClientApiKeyId,
        session_key: &'a ProviderSessionAffinityKey,
        replay: &'a CodexNotesReplay,
    ) -> BoxFuture<'a, Result<(), ProviderStoreError>>;

    /// 以读到的代次做一次 CAS，发布新 owner 与新 notes 域
    ///
    /// 发布事务必须复验：目标账号凭据版本一致且仍是可用 OAuth 账号；
    /// `through_seq` 之前（含）的操作全部终态，且不存在更后的操作
    /// 网络与重放期间不得持有事务或行锁
    fn publish_notes_replay<'a>(
        &'a self,
        publish: &'a CodexNotesPublish,
    ) -> BoxFuture<'a, Result<CodexNotesPublishOutcome, ProviderStoreError>>;
}

/// 会话内容（history 条目与 notes 操作日志）的持久事实
///
/// 所有读写都以 Key 与逻辑会话为前缀；session_id 本身不是权限凭据
pub trait CodexSessionContentPort: Send + Sync {
    /// 落库一次请求输入条目；窗口与条目顺序由存储原子分配
    fn commit_history_request<'a>(
        &'a self,
        record: &'a CodexHistoryRequest,
    ) -> BoxFuture<'a, Result<(), ProviderStoreError>>;

    /// 落库一次响应产出的条目与本次逻辑输入引用
    ///
    /// 同一 response 可分多批追加输出；只有首次锚定写入输入引用
    fn commit_history_response<'a>(
        &'a self,
        record: &'a CodexHistoryResponse,
    ) -> BoxFuture<'a, Result<(), ProviderStoreError>>;

    /// 读取已保存响应的窗口定位与逻辑输入/输出引用
    ///
    /// 未保存时返回 None，不能推断其历史为空
    fn load_history_response<'a>(
        &'a self,
        key_id: &'a ClientApiKeyId,
        session_key: &'a ProviderSessionAffinityKey,
        response_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<CodexHistoryResponseAnchor>, ProviderStoreError>>;

    /// 读取该逻辑窗口最近一次锚定的响应
    ///
    /// 无父响应的完整回填以它的逻辑输入与产出引用为当前上下文前缀，
    /// 不能把窗口内全部历史档案当成当前会话上下文
    fn load_latest_history_response<'a>(
        &'a self,
        key_id: &'a ClientApiKeyId,
        session_key: &'a ProviderSessionAffinityKey,
        window_id: &'a CodexNativeWindowId,
    ) -> BoxFuture<'a, Result<Option<CodexHistoryResponseAnchor>, ProviderStoreError>>;

    /// 保存最近一次发送前请求的逻辑输入快照；同一窗口只保留最新一份
    ///
    /// 快照覆盖尚未进入任何响应锚的尾部输入：上游在锚建立之前失败时，
    /// 下一次全量重发据此复用已保存的原条目，不再按当前条目数重复归档
    fn save_history_pending_input<'a>(
        &'a self,
        key_id: &'a ClientApiKeyId,
        session_key: &'a ProviderSessionAffinityKey,
        pending: &'a CodexHistoryPendingInput,
    ) -> BoxFuture<'a, Result<(), ProviderStoreError>>;

    /// 读取最近一次发送前请求的逻辑输入快照
    ///
    /// 没有快照时返回 None，不能据此推断该窗口没有输入
    fn load_history_pending_input<'a>(
        &'a self,
        key_id: &'a ClientApiKeyId,
        session_key: &'a ProviderSessionAffinityKey,
        window_id: &'a CodexNativeWindowId,
    ) -> BoxFuture<'a, Result<Option<CodexHistoryPendingInput>, ProviderStoreError>>;

    /// 读取会话的窗口目录及条目数；过滤与排序预算由查询方实现
    ///
    /// 目录与条目数只由已落库条目定义；每个窗口附带条目、响应锚与发送前快照中
    /// 已持久保存的去重窗口元数据，供查询解析 context window UUID 别名
    /// 元数据集合不增加窗口或条目数，也不能为没有条目的窗口创建目录项
    fn load_history_windows<'a>(
        &'a self,
        key_id: &'a ClientApiKeyId,
        session_key: &'a ProviderSessionAffinityKey,
    ) -> BoxFuture<'a, Result<Vec<CodexHistoryStoredWindow>, ProviderStoreError>>;

    /// 读取会话全部条目，按存储顺序升序返回
    fn load_history_items<'a>(
        &'a self,
        key_id: &'a ClientApiKeyId,
        session_key: &'a ProviderSessionAffinityKey,
    ) -> BoxFuture<'a, Result<Vec<CodexHistoryStoredItem>, ProviderStoreError>>;

    /// 只读取一个窗口的条目，按存储顺序升序返回
    ///
    /// 采集对齐只需要当前窗口的前缀，不加载其它窗口档案
    fn load_history_window_items<'a>(
        &'a self,
        key_id: &'a ClientApiKeyId,
        session_key: &'a ProviderSessionAffinityKey,
        window_id: &'a CodexNativeWindowId,
    ) -> BoxFuture<'a, Result<Vec<CodexHistoryStoredItem>, ProviderStoreError>>;

    /// 追加一次 notes 写入意图并在会话内原子分配 seq
    ///
    /// 只有取得本次写入位置的操作才允许发送；返回状态为 `Unsent`
    fn append_notes_operation<'a>(
        &'a self,
        operation: &'a NewCodexNotesOperation,
    ) -> BoxFuture<'a, Result<CodexNotesOperation, ProviderStoreError>>;

    /// 在字节离开网关前持久化尝试开始事实
    ///
    /// 只接受仍为 `Unsent` 的操作；发送前明确失败的操作直接以 `Failed` 终结，不进入 `InFlight`
    fn start_notes_operation<'a>(
        &'a self,
        key_id: &'a ClientApiKeyId,
        session_key: &'a ProviderSessionAffinityKey,
        seq: u64,
    ) -> BoxFuture<'a, Result<CodexNotesOperation, ProviderStoreError>>;

    /// 保存终结观察与原始响应；已终结的操作不可重开
    ///
    /// 允许的转移为 `Unsent` → `Failed` 与 `InFlight` → `Responded`/`Failed`/`Unknown`
    /// 其他转移返回冲突，不能用新观察覆盖已固定的业务含义
    fn complete_notes_operation<'a>(
        &'a self,
        completion: &'a CodexNotesCompletion,
    ) -> BoxFuture<'a, Result<CodexNotesOperation, ProviderStoreError>>;

    /// 读取会话全部 notes 操作，按 seq 升序返回
    ///
    /// 顺序、状态与原始响应是重建的唯一依据；不按正文相似度合并
    fn load_notes_operations<'a>(
        &'a self,
        key_id: &'a ClientApiKeyId,
        session_key: &'a ProviderSessionAffinityKey,
    ) -> BoxFuture<'a, Result<Vec<CodexNotesOperation>, ProviderStoreError>>;
}
