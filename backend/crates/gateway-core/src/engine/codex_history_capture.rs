//! 原生 history 的请求与响应采集协调：来源判定、位置对齐与逻辑输入/输出引用
//!
//! Provider 只交付经过隐私规则的最终业务条目与响应事件事实；本模块决定会话是否采集、
//! 输入是否已作为前缀存在、父响应是否可续接，并把条目与精确引用写入持久事实
//! 采集只发生在已确认从首次新建完整采集的会话；其余来源保持既有上游行为
//! 不使用正文或 JSON 哈希去重：同一文本出现在不同位置仍是两条

use std::{
    collections::{HashMap, HashSet},
    fmt,
};

use thiserror::Error;

use super::codex_history::{CodexHistoryService, codex_history_item_id};
use crate::{
    policy::ClientApiKeyId,
    provider_ports::{
        CodexHistoryItem, CodexHistoryPendingInput, CodexHistoryRequest, CodexHistoryResponse,
        CodexHistoryResponseAnchor, CodexHistoryRole, CodexHistoryStoredItem, CodexHistoryWindow,
        CodexSessionOrigin, ProviderSessionAffinityKey, ProviderStoreError,
    },
};

/// 客户端声明的 history 初始化来源
///
/// 只保留判定完整采集需要的三类事实，不复制官方六个 wire 值；
/// 未声明既不能确认也不能否认从首次新建开始采集
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodexHistoryInitialization {
    /// 官方声明从首次新建开始
    New,
    /// 官方声明为清空、冷/热恢复、分叉或外部提供的历史
    Supplied,
    /// 客户端未声明来源
    Undeclared,
}

/// 一次模型请求的原生状态事实
///
/// 窗口与 agent 来自官方 turn metadata 投影；窗口缺失的请求不采集，
/// 因为不能猜测条目归属
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexHistoryRequestFacts {
    pub key_id: ClientApiKeyId,
    pub session_key: ProviderSessionAffinityKey,
    pub window: CodexHistoryWindow,
    /// 客户端本次续接的父响应原值
    pub previous_response_id: Option<String>,
    /// 官方请求类型，如 turn、prewarm、compaction；只记录事实，不单独决定来源
    pub request_kind: Option<String>,
    pub history_initialization: CodexHistoryInitialization,
}

/// 已由 Protocol 从最终业务正文或响应事件投影的条目
///
/// 条目还没有窗口内稳定 ID：位置由本模块按已持久数目分配，
/// 上游声明的 ID 原样保留，其余由窗口身份与位置生成
#[derive(Clone, PartialEq, Eq)]
pub struct CodexHistoryCaptureItem {
    pub declared_item_id: Option<String>,
    pub role: Option<CodexHistoryRole>,
    pub tool_namespace: Option<String>,
    pub tool_name: Option<String>,
    pub content_type: String,
    /// 原始条目 JSON，含密文与附件字段
    pub raw: String,
    /// 明文可读投影；需要解密才能理解的对象为 None
    pub text: Option<String>,
    /// 完整语义身份；同位置比较时使用，密文与图片字段已参与其中
    pub identity: String,
}

impl fmt::Debug for CodexHistoryCaptureItem {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CodexHistoryCaptureItem")
            .field("declared_item_id", &self.declared_item_id)
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

impl CodexHistoryCaptureItem {
    /// 按窗口存储位置生成持久条目
    fn to_history_item(&self, window: &CodexHistoryWindow, position: u64) -> CodexHistoryItem {
        CodexHistoryItem {
            item_id: self.declared_identity_at(window, position),
            role: self.role,
            tool_namespace: self.tool_namespace.clone(),
            tool_name: self.tool_name.clone(),
            content_type: self.content_type.clone(),
            raw: self.raw.clone(),
            text: self.text.clone(),
            identity: self.identity.clone(),
        }
    }

    /// 该条目在同一窗口同一位置上的稳定 ID
    fn declared_identity_at(&self, window: &CodexHistoryWindow, position: u64) -> String {
        codex_history_item_id(
            self.declared_item_id.as_deref(),
            &window.window_id,
            position,
        )
    }

    /// 与已存条目在同一位置上是否表示同一语义对象
    ///
    /// 上游声明了稳定 ID 时按 ID 对齐；没有 ID 时比较完整的语义身份，
    /// 因此回填省略 ID/status 的输出项仍能对齐，而同位置改变密文、图片或正文不会被当成同一条
    fn aligns_with(&self, stored: &CodexHistoryItem) -> bool {
        match self.declared_item_id.as_deref() {
            Some(declared) => stored.item_id == declared,
            None => !stored.identity.is_empty() && stored.identity == self.identity,
        }
    }
}

/// 一次请求采集的结果
///
/// `input` 是本次请求准确的逻辑输入引用，发送前随条目一起落库，
/// 响应锚定时由同一请求句柄写进对应响应
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CodexHistoryCapturedRequest {
    /// 按客户端请求顺序排列的逻辑输入条目引用
    pub input: Vec<String>,
    /// 本次新写入的条目数
    pub appended: usize,
}

/// 采集失败；父响应不可续接不能降级成“没有历史”
#[derive(Debug, Error)]
pub enum CodexHistoryCaptureError {
    /// 持久事实读写失败
    #[error(transparent)]
    Store(#[from] ProviderStoreError),
    /// 客户端续接了本地没有保存的响应；不能猜丢失前缀
    #[error("Codex history parent response is not stored for this session")]
    ParentResponseUnknown,
    /// 父响应属于另一个逻辑窗口；输入不能跨窗口归档
    #[error("Codex history parent response belongs to another window")]
    ParentWindowChanged,
}

impl CodexHistoryService {
    /// 会话是否由本地 history 回答；只有 managed 会话为真
    ///
    /// 只读已判定事实，不用它解释首次来源；未判定会话仍走上游 history
    ///
    /// # Errors
    ///
    /// 读取会话状态失败
    pub async fn uses_local_history(
        &self,
        key_id: &ClientApiKeyId,
        session_key: &ProviderSessionAffinityKey,
    ) -> Result<bool, ProviderStoreError> {
        Ok(self
            .load_origin(key_id, session_key)
            .await?
            .is_some_and(|origin| origin == CodexSessionOrigin::Managed))
    }

    /// 在发送前判定本会话是否由本地采集
    ///
    /// 未判定会话在这里完成首次来源判定并缓存结论；空输入预热同样经过同一入口，
    /// 因此首次请求就能从 Unclassified 变成 Managed，而不是永远停在只读判定
    ///
    /// # Errors
    ///
    /// 读取会话状态失败
    pub async fn begin_request(
        &self,
        facts: &CodexHistoryRequestFacts,
    ) -> Result<bool, CodexHistoryCaptureError> {
        Ok(self.settled_managed(facts).await?)
    }

    /// 校验客户端续接的父响应属于本会话与同一窗口
    ///
    /// 未知父响应不能被当成“没有历史”；调用方在发送前用它决定是否让客户端改发全量
    ///
    /// # Errors
    ///
    /// 读取持久事实失败、父响应未知或属于另一个窗口
    pub async fn validate_parent(
        &self,
        facts: &CodexHistoryRequestFacts,
    ) -> Result<(), CodexHistoryCaptureError> {
        let Some(parent) = facts.previous_response_id.as_deref() else {
            return Ok(());
        };
        self.load_parent(facts, parent).await?;
        Ok(())
    }

    /// 采集一次模型请求的输入条目并返回该请求的逻辑输入引用
    ///
    /// 有父响应时按该响应保存的逻辑输入与产出引用还原当时前缀
    /// 无父响应时按当前窗口最近一次响应的同一快照对齐，并按发送前逻辑输入快照
    /// 覆盖尚未进入响应锚的尾部，不把窗口内全部档案当成当前上下文
    /// 上游已声明且已存过的条目只补引用，不重复入库
    ///
    /// # Errors
    ///
    /// 读取或写入持久事实失败、父响应未知或属于另一个窗口
    pub async fn capture_request(
        &self,
        facts: &CodexHistoryRequestFacts,
        items: &[CodexHistoryCaptureItem],
    ) -> Result<CodexHistoryCapturedRequest, CodexHistoryCaptureError> {
        if !self.settled_managed(facts).await? {
            return Ok(CodexHistoryCapturedRequest::default());
        }
        // 空输入仍要返回本次请求的逻辑上下文：预热请求的上下文为空，
        // 但带父响应的空增量请求上下文正是父响应的输入与产出，
        // 提前返回空引用会让后续续接丢失整段前缀
        let window = &facts.window;
        let existing = self
            .content
            .load_history_window_items(&facts.key_id, &facts.session_key, &window.window_id)
            .await?;
        let (mut input, skip) = match facts.previous_response_id.as_deref() {
            Some(parent) => {
                let anchor = self.load_parent(facts, parent).await?;
                // 父响应的逻辑输入与产出都是本次上下文；只有完整重发该前缀才跳过
                let prefix = anchor
                    .input
                    .into_iter()
                    .chain(anchor.output)
                    .collect::<Vec<_>>();
                let replayed = aligned_prefix(items, &prefix, &existing) == prefix.len();
                let skip = if replayed { prefix.len() } else { 0 };
                (prefix, skip)
            }
            None => {
                // 无父响应时当前上下文是本窗口最近一次响应的逻辑输入与产出；
                // 但上游在建立响应锚之前失败时（如额度拒绝），发送前已保存的尾部输入
                // 还没进入任何锚，只能从发送前快照恢复。两个候选都按位置比较完整语义身份，
                // 取真正对齐更长的一方：有锚时锚包含产出，无锚时快照覆盖失败 attempt 的尾部
                let anchor_prefix = match self
                    .content
                    .load_latest_history_response(
                        &facts.key_id,
                        &facts.session_key,
                        &window.window_id,
                    )
                    .await?
                {
                    Some(anchor) => anchor
                        .input
                        .into_iter()
                        .chain(anchor.output)
                        .collect::<Vec<_>>(),
                    None => Vec::new(),
                };
                let pending_prefix = self
                    .content
                    .load_history_pending_input(
                        &facts.key_id,
                        &facts.session_key,
                        &window.window_id,
                    )
                    .await?
                    .map(|pending| pending.input)
                    .unwrap_or_default();
                let anchors = aligned_prefix(items, &anchor_prefix, &existing);
                let pendings = aligned_prefix(items, &pending_prefix, &existing);
                // 平局时取锚：它同时包含当时产出，快照只是它的输入子集
                if pendings > anchors {
                    (pending_prefix[..pendings].to_vec(), pendings)
                } else {
                    (anchor_prefix[..anchors].to_vec(), anchors)
                }
            }
        };

        let mut known = existing
            .iter()
            .map(|stored| stored.item.item_id.clone())
            .collect::<HashSet<String>>();
        let mut position = u64::try_from(existing.len()).unwrap_or(u64::MAX);
        let mut committed = Vec::new();
        for item in &items[skip..] {
            if let Some(declared) = item
                .declared_item_id
                .as_deref()
                .filter(|declared| known.contains(*declared))
            {
                // 已入库的同 ID 条目只补引用；不按正文全局去重
                input.push(declared.to_owned());
                continue;
            }
            let item_id = item.declared_identity_at(window, position);
            known.insert(item_id.clone());
            input.push(item_id);
            committed.push(item.to_history_item(window, position));
            position = position.saturating_add(1);
        }

        let appended = committed.len();
        if appended != 0 {
            self.content
                .commit_history_request(&CodexHistoryRequest {
                    key_id: facts.key_id.clone(),
                    session_key: facts.session_key.clone(),
                    window: window.clone(),
                    previous_response_id: facts.previous_response_id.clone(),
                    items: committed,
                })
                .await?;
        }
        // 发送前保存本次请求的逻辑输入快照：上游在响应锚建立之前失败时，
        // 下一次全量重发据它复用这些原条目；空输入没有可对齐的上下文，不覆盖已有快照
        if !input.is_empty() {
            self.content
                .save_history_pending_input(
                    &facts.key_id,
                    &facts.session_key,
                    &CodexHistoryPendingInput {
                        window: window.clone(),
                        previous_response_id: facts.previous_response_id.clone(),
                        input: input.clone(),
                    },
                )
                .await?;
        }
        Ok(CodexHistoryCapturedRequest { input, appended })
    }

    /// 采集一批响应产出条目；空批次只建立 response 锚
    ///
    /// 只应在同一请求的 [`Self::capture_request`] 判定采集后调用：预热与零输出响应
    /// 同样需要锚，因此空批次不能被跳过
    /// `input` 来自同一请求发送前保存的逻辑输入引用，只在首次锚定时写入
    ///
    /// # Errors
    ///
    /// 读取或写入持久事实失败
    pub async fn capture_response(
        &self,
        facts: &CodexHistoryRequestFacts,
        response_id: &str,
        input: &[String],
        items: &[CodexHistoryCaptureItem],
    ) -> Result<(), CodexHistoryCaptureError> {
        if !self.settled_managed(facts).await? {
            return Ok(());
        }
        let window = &facts.window;
        let base = if items.is_empty() {
            0
        } else {
            self.window_item_count(&facts.key_id, &facts.session_key, window)
                .await?
        };
        let committed = items
            .iter()
            .enumerate()
            .map(|(offset, item)| {
                let position = base.saturating_add(u64::try_from(offset).unwrap_or(u64::MAX));
                item.to_history_item(window, position)
            })
            .collect::<Vec<_>>();
        self.content
            .commit_history_response(&CodexHistoryResponse {
                key_id: facts.key_id.clone(),
                session_key: facts.session_key.clone(),
                response_id: response_id.to_owned(),
                window: window.clone(),
                previous_response_id: facts.previous_response_id.clone(),
                input: input.to_vec(),
                items: committed,
            })
            .await?;
        Ok(())
    }

    /// 读取父响应锚并确认它属于本会话与同一窗口
    async fn load_parent(
        &self,
        facts: &CodexHistoryRequestFacts,
        parent: &str,
    ) -> Result<CodexHistoryResponseAnchor, CodexHistoryCaptureError> {
        let anchor = self
            .content
            .load_history_response(&facts.key_id, &facts.session_key, parent)
            .await?;
        let Some(anchor) = anchor else {
            return Err(CodexHistoryCaptureError::ParentResponseUnknown);
        };
        if anchor.window.window_id != facts.window.window_id {
            return Err(CodexHistoryCaptureError::ParentWindowChanged);
        }
        Ok(anchor)
    }

    /// 判定来源并在未判定时写入首次结论
    ///
    /// 关闭新会话托管时未判定会话直接固定为 legacy：即使以后开启开关，
    /// 已漏记的会话也不会被冒充为从首次新建完整采集
    async fn settled_managed(
        &self,
        facts: &CodexHistoryRequestFacts,
    ) -> Result<bool, ProviderStoreError> {
        match self.load_origin(&facts.key_id, &facts.session_key).await? {
            Some(CodexSessionOrigin::Managed) => Ok(true),
            Some(CodexSessionOrigin::Unclassified) => {
                let origin = if self.managed_admission {
                    classify_origin(facts)
                } else {
                    Some(CodexSessionOrigin::Legacy)
                };
                let Some(origin) = origin else {
                    return Ok(false);
                };
                let state = self
                    .state
                    .resolve_session_origin(&facts.key_id, &facts.session_key, origin)
                    .await?;
                Ok(state.origin == CodexSessionOrigin::Managed)
            }
            Some(CodexSessionOrigin::Legacy) | None => Ok(false),
        }
    }

    /// 会话尚未认领时返回 None；不能据此断定旧状态为空
    async fn load_origin(
        &self,
        key_id: &ClientApiKeyId,
        session_key: &ProviderSessionAffinityKey,
    ) -> Result<Option<CodexSessionOrigin>, ProviderStoreError> {
        Ok(self
            .state
            .load_session(key_id, session_key)
            .await?
            .map(|state| state.origin))
    }

    /// 窗口内已持久条目数；条目位置据此分配
    async fn window_item_count(
        &self,
        key_id: &ClientApiKeyId,
        session_key: &ProviderSessionAffinityKey,
        window: &CodexHistoryWindow,
    ) -> Result<u64, ProviderStoreError> {
        let windows = self
            .content
            .load_history_windows(key_id, session_key)
            .await?;
        Ok(windows
            .iter()
            .find(|stored| stored.window_id == window.window_id)
            .map_or(0, |stored| stored.item_count))
    }
}

/// 请求输入与已保存前缀的公共语义前缀长度
///
/// 只有从第一条起逐位对齐的部分算已存在；增量请求不会被单个同文条目误吞
fn aligned_prefix(
    items: &[CodexHistoryCaptureItem],
    prefix: &[String],
    existing: &[CodexHistoryStoredItem],
) -> usize {
    let located = existing
        .iter()
        .map(|stored| (stored.item.item_id.as_str(), stored))
        .collect::<HashMap<_, _>>();
    items
        .iter()
        .zip(prefix.iter())
        .take_while(|(item, item_id)| {
            located
                .get(item_id.as_str())
                .is_some_and(|stored| item.aligns_with(&stored.item))
        })
        .count()
}

/// 首次来源判定；只有能确认从首次新建完整采集时才成为 managed
///
/// 官方声明非新建、已带父响应或窗口序号非 0 都说明本地没有完整前缀；
/// 缺少声明时保持未判定，不猜任何一种结论
/// 预热请求同样可以建立 managed：它声明的是首窗口新建，且携带同一份完整输入；
/// 正式生成续接预热响应时只追加增量
fn classify_origin(facts: &CodexHistoryRequestFacts) -> Option<CodexSessionOrigin> {
    let fresh_window = facts.window.window_id.window_number() == 0;
    let root_request = facts.previous_response_id.is_none();
    match facts.history_initialization {
        CodexHistoryInitialization::Supplied => Some(CodexSessionOrigin::Legacy),
        CodexHistoryInitialization::New => {
            (fresh_window && root_request).then_some(CodexSessionOrigin::Managed)
        }
        CodexHistoryInitialization::Undeclared => {
            (!fresh_window || !root_request).then_some(CodexSessionOrigin::Legacy)
        }
    }
}
