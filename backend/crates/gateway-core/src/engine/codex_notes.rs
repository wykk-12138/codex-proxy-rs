//! 原生 notes 的会话内顺序、操作日志与副本重建协调
//!
//! 本模块决定一个会话是否记录写入、每次真实写入的 seq，以及切换时按 seq 在新域重建并发布
//! 只使用持久事实端口；不解释 HTTP、凭据、加密正文或上游业务结果
//! 互斥只覆盖同一 Key 与逻辑会话的 notes 变更与重建，不阻塞模型流与其它会话

use std::{collections::HashMap, sync::Arc};

use futures::lock::{Mutex, OwnedMutexGuard};
use thiserror::Error;

use crate::{
    account::ProviderAccountId,
    policy::ClientApiKeyId,
    provider_ports::{
        CodexNotesCompletion, CodexNotesGeneration, CodexNotesOperation, CodexNotesOperationKind,
        CodexNotesPublish, CodexNotesPublishOutcome, CodexNotesReplay, CodexNotesReplica,
        CodexSessionContentPort, CodexSessionOrigin, CodexSessionStatePort, NewCodexNotesOperation,
        ProviderSessionAffinityKey, ProviderStoreError,
    },
};

/// 一次 notes 请求所属会话的来源与已发布落点
///
/// 出站域只由这里决定；未认领会话没有该快照，也不能凭客户端会话 ID 猜测落点
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexNotesDomain {
    pub origin: CodexSessionOrigin,
    pub replica: CodexNotesReplica,
}

impl CodexNotesDomain {
    /// legacy 会话沿既有原生域行为，不新增日志
    ///
    /// 未判定会话可能先收到辅助写入，仍要先行记录，避免后续来源判定漏日志
    #[must_use]
    pub const fn records_operations(&self) -> bool {
        matches!(
            self.origin,
            CodexSessionOrigin::Managed | CodexSessionOrigin::Unclassified
        )
    }

    /// 出站 `context.session_id`
    ///
    /// 原生域沿用客户端会话 ID；重建域用发布过的域代次，避免写回曾用账号的原生域
    #[must_use]
    pub fn outbound_session_id<'a>(&'a self, client_session_id: &'a str) -> &'a str {
        self.replica
            .generation()
            .map_or(client_session_id, CodexNotesGeneration::expose_to_store)
    }
}

/// notes 协调失败
///
/// 未终结操作、越序重放与未完成冻结目标都直接报错，不自动恢复也不跳过
#[derive(Debug, Error)]
pub enum CodexNotesError {
    /// 持久事实读写失败
    #[error(transparent)]
    Store(#[from] ProviderStoreError),
    /// 会话尚未被认领；不能猜测归属或记录操作
    #[error("Codex session is not claimed for notes operations")]
    SessionAbsent,
    /// legacy 会话没有本地完整性，保持账号原生域
    #[error("Codex legacy session keeps its native notes domain")]
    LegacySession,
    /// 日志中存在未终结操作；冻结目标不能跳过它
    #[error("Codex notes operation seq {seq} has no terminal observation")]
    OperationUnsettled { seq: u64 },
    /// 重放步骤必须按冻结顺序推进
    #[error("Codex notes replay step is not the next frozen operation")]
    ReplayOutOfOrder,
    /// 冻结目标尚未全部重放，不能发布新域
    #[error("Codex notes replay has not reached the frozen target")]
    ReplayNotComplete,
}

/// 一次重建的冻结目标与按 seq 排列的待重放操作
///
/// 步骤只包含收到完整响应的操作；`Failed` 不进入重放，但已终结并计入冻结位置
/// 冻结位置末尾的 `Failed` 没有需要发送的内容，重放完最后一步即视为到达冻结位置
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexNotesRebuild {
    /// 重建目标账号
    pub account: ProviderAccountId,
    /// 本次重建取用的新域代次；不复用曾发布过的域
    pub generation: CodexNotesGeneration,
    /// 读取会话状态时观测到的代次；发布时据此 CAS
    pub expected_generation: u64,
    /// 冻结目标包含的最后一个操作 seq；空日志为 0
    pub through_seq: u64,
    /// 已确认在目标域重放到的 seq
    pub replayed_through_seq: u64,
    pub steps: Vec<CodexNotesOperation>,
}

impl CodexNotesRebuild {
    /// 是否已按冻结目标重放完毕
    #[must_use]
    pub const fn is_replayed(&self) -> bool {
        self.replayed_through_seq == self.through_seq
    }

    /// 目标副本落点
    #[must_use]
    pub fn replica(&self) -> CodexNotesReplica {
        CodexNotesReplica::rebuilt(self.account.clone(), self.generation.clone())
    }
}

/// 同一会话 notes 顺序与重建的协调入口
///
/// 组合根只装配一个实例，并把它同时交给 notes 辅助与切换入口；
/// 每请求另建锁会让两个写入各自取得不同互斥而失去顺序
pub struct CodexNotesService {
    state: Arc<dyn CodexSessionStatePort>,
    content: Arc<dyn CodexSessionContentPort>,
    /// 是否允许新会话进入托管；关闭时未判定会话按 legacy 固定
    managed_admission: bool,
    sessions: Mutex<HashMap<CodexNotesSessionKey, Arc<Mutex<()>>>>,
}

impl CodexNotesService {
    /// `managed_admission` 只影响未判定会话：关闭时固定为 legacy，不记录新操作；
    /// 已 managed 的会话继续使用已发布域与日志，保留重启后的顺序与切号能力
    #[must_use]
    pub fn new(
        state: Arc<dyn CodexSessionStatePort>,
        content: Arc<dyn CodexSessionContentPort>,
        managed_admission: bool,
    ) -> Self {
        Self {
            state,
            content,
            managed_admission,
            sessions: Mutex::new(HashMap::new()),
        }
    }

    /// 取得同一 Key 与逻辑会话的 notes 互斥
    ///
    /// 写类端点与重建在整个发送与落库生命周期内持有该互斥；
    /// 读类端点只读已发布快照，不参与互斥
    pub async fn lock_session(
        &self,
        key_id: &ClientApiKeyId,
        session_key: &ProviderSessionAffinityKey,
    ) -> OwnedMutexGuard<()> {
        let session = {
            let mut sessions = self.sessions.lock().await;
            Arc::clone(
                sessions
                    .entry(CodexNotesSessionKey::new(key_id, session_key))
                    .or_default(),
            )
        };
        session.lock_owned().await
    }

    /// 读取会话的来源与已发布落点；未认领返回 None
    ///
    /// 关闭新会话托管时未判定会话在这里固定为 legacy，因此首次模型或辅助请求
    /// 就不会记录新日志；以后开启开关也不会把已漏记的会话当成完整托管
    ///
    /// # Errors
    ///
    /// 读取或固定会话状态失败
    pub async fn domain(
        &self,
        key_id: &ClientApiKeyId,
        session_key: &ProviderSessionAffinityKey,
    ) -> Result<Option<CodexNotesDomain>, CodexNotesError> {
        let Some(state) = self.state.load_session(key_id, session_key).await? else {
            return Ok(None);
        };
        let state = if self.managed_admission || state.origin != CodexSessionOrigin::Unclassified {
            state
        } else {
            self.state
                .resolve_session_origin(key_id, session_key, CodexSessionOrigin::Legacy)
                .await?
        };
        Ok(Some(CodexNotesDomain {
            origin: state.origin,
            replica: state.notes,
        }))
    }

    /// 记录一次真实写入意图并取得会话内 seq
    ///
    /// 拿到 seq 才允许发送；内容相同的两次请求是两次真实变更，不按正文去重
    ///
    /// # Errors
    ///
    /// 写入日志失败
    pub async fn append_operation(
        &self,
        key_id: &ClientApiKeyId,
        session_key: &ProviderSessionAffinityKey,
        replica: CodexNotesReplica,
        kind: CodexNotesOperationKind,
        agent_name: String,
        request: String,
    ) -> Result<CodexNotesOperation, CodexNotesError> {
        Ok(self
            .content
            .append_notes_operation(&NewCodexNotesOperation {
                key_id: key_id.clone(),
                session_key: session_key.clone(),
                replica,
                agent_name,
                kind,
                request,
            })
            .await?)
    }

    /// 在字节离开网关前持久化尝试开始事实
    ///
    /// # Errors
    ///
    /// 操作不是 `Unsent` 或写入失败
    pub async fn start_operation(
        &self,
        key_id: &ClientApiKeyId,
        session_key: &ProviderSessionAffinityKey,
        seq: u64,
    ) -> Result<CodexNotesOperation, CodexNotesError> {
        Ok(self
            .content
            .start_notes_operation(key_id, session_key, seq)
            .await?)
    }

    /// 保存终结观察与原始响应
    ///
    /// # Errors
    ///
    /// 状态转移不合法或写入失败
    pub async fn complete_operation(
        &self,
        completion: &CodexNotesCompletion,
    ) -> Result<CodexNotesOperation, CodexNotesError> {
        Ok(self.content.complete_notes_operation(completion).await?)
    }

    /// 读取会话全部操作，按 seq 升序
    ///
    /// # Errors
    ///
    /// 读取日志失败
    pub async fn operations(
        &self,
        key_id: &ClientApiKeyId,
        session_key: &ProviderSessionAffinityKey,
    ) -> Result<Vec<CodexNotesOperation>, CodexNotesError> {
        Ok(self
            .content
            .load_notes_operations(key_id, session_key)
            .await?)
    }

    /// 冻结当前日志尾部并为目标账号生成新的重建域
    ///
    /// 调用方必须先持有会话互斥；这里保存冻结目标与零进度，重放期间不持事务
    ///
    /// # Errors
    ///
    /// 会话未认领或已判定 legacy、日志存在未终结操作、保存冻结目标失败
    pub async fn begin_rebuild(
        &self,
        key_id: &ClientApiKeyId,
        session_key: &ProviderSessionAffinityKey,
        target: &ProviderAccountId,
    ) -> Result<CodexNotesRebuild, CodexNotesError> {
        let state = self
            .state
            .load_session(key_id, session_key)
            .await?
            .ok_or(CodexNotesError::SessionAbsent)?;
        if state.origin == CodexSessionOrigin::Legacy {
            return Err(CodexNotesError::LegacySession);
        }
        let operations = self.operations(key_id, session_key).await?;
        // 未终结操作占住自己的 seq，冻结目标不能跳过它
        if let Some(unsettled) = operations
            .iter()
            .find(|operation| !operation.status.is_terminal())
        {
            return Err(CodexNotesError::OperationUnsettled { seq: unsettled.seq });
        }
        let through_seq = operations.last().map_or(0, |operation| operation.seq);
        let steps = operations
            .into_iter()
            .filter(|operation| operation.status.needs_replay())
            .collect::<Vec<_>>();
        // 每次重建都取全新域代次；半途失败的目标域随之成为遗留，不污染新域
        // 没有需要重放的操作时冻结位置已经是完成状态；只有 Failed 的日志不重放任何内容
        let replayed_through_seq = if steps.is_empty() { through_seq } else { 0 };
        let rebuild = CodexNotesRebuild {
            account: target.clone(),
            generation: CodexNotesGeneration::generate(),
            expected_generation: state.generation,
            through_seq,
            replayed_through_seq,
            steps,
        };
        self.save_replay(key_id, session_key, &rebuild, replayed_through_seq)
            .await?;
        Ok(rebuild)
    }

    /// 记录一步重放已完成并推进冻结进度
    ///
    /// 调用方必须持有会话互斥；只有目标域真实收到完整成功响应才推进
    ///
    /// # Errors
    ///
    /// 步骤不是冻结顺序中的下一步或保存进度失败
    pub async fn advance_rebuild(
        &self,
        key_id: &ClientApiKeyId,
        session_key: &ProviderSessionAffinityKey,
        rebuild: &mut CodexNotesRebuild,
        seq: u64,
    ) -> Result<(), CodexNotesError> {
        let next = rebuild
            .steps
            .iter()
            .map(|step| step.seq)
            .find(|step| *step > rebuild.replayed_through_seq);
        if next != Some(seq) {
            return Err(CodexNotesError::ReplayOutOfOrder);
        }
        // 最后一步之后不再有待发送内容；冻结位置末尾的 Failed 随该步一并到达
        let replayed = if rebuild.steps.iter().any(|step| step.seq > seq) {
            seq
        } else {
            rebuild.through_seq
        };
        self.save_replay(key_id, session_key, rebuild, replayed)
            .await?;
        rebuild.replayed_through_seq = replayed;
        Ok(())
    }

    /// 以原代次 CAS 发布已完整重放的新 owner 与新域
    ///
    /// 发布事务会复验保存的冻结目标与进度，未同步完成的新域不能被发布
    ///
    /// # Errors
    ///
    /// 冻结目标尚未全部重放或发布事务失败
    pub async fn publish_rebuild(
        &self,
        key_id: &ClientApiKeyId,
        session_key: &ProviderSessionAffinityKey,
        rebuild: &CodexNotesRebuild,
        credential_revision: u64,
    ) -> Result<CodexNotesPublishOutcome, CodexNotesError> {
        if !rebuild.is_replayed() {
            return Err(CodexNotesError::ReplayNotComplete);
        }
        Ok(self
            .state
            .publish_notes_replay(&CodexNotesPublish {
                key_id: key_id.clone(),
                session_key: session_key.clone(),
                expected_generation: rebuild.expected_generation,
                owner: rebuild.account.clone(),
                credential_revision,
                replica: rebuild.replica(),
                through_seq: rebuild.through_seq,
            })
            .await?)
    }

    async fn save_replay(
        &self,
        key_id: &ClientApiKeyId,
        session_key: &ProviderSessionAffinityKey,
        rebuild: &CodexNotesRebuild,
        replayed_through_seq: u64,
    ) -> Result<(), CodexNotesError> {
        let replay = CodexNotesReplay::try_new(
            rebuild.replica(),
            rebuild.through_seq,
            replayed_through_seq,
        )?;
        self.state
            .save_notes_replay(key_id, session_key, &replay)
            .await?;
        Ok(())
    }
}

/// 互斥键只由 Key 与逻辑会话构成；会话 ID 本身不是权限凭据
#[derive(Clone, PartialEq, Eq, Hash)]
struct CodexNotesSessionKey {
    key_id: ClientApiKeyId,
    session_key: ProviderSessionAffinityKey,
}

impl CodexNotesSessionKey {
    fn new(key_id: &ClientApiKeyId, session_key: &ProviderSessionAffinityKey) -> Self {
        Self {
            key_id: key_id.clone(),
            session_key: session_key.clone(),
        }
    }
}
