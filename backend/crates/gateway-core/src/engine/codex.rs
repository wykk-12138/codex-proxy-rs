//! Codex 原生辅助请求的有限 Provider 能力与已授权上下文

use std::sync::Arc;

use bytes::Bytes;
use futures::future::BoxFuture;

use crate::{
    account::{ProviderAccountId, scope::FrozenAccountScope},
    error::ProviderError,
    lifecycle::{CancellationToken, Deadline},
    policy::ClientApiKeyId,
};

use super::middleware::{MiddlewareHeader, MiddlewareResponse};

/// 固定操作集合；官方 URL、方法与 OAuth 均由 Provider 解释
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodexAuxiliaryOperation {
    HistoryListWindows,
    HistoryListItems,
    HistoryReadItem,
    HistorySearchContents,
    NotesThreadHint,
    NotesListFilesByPrefix,
    NotesReadFile,
    NotesSearchContents,
    NotesAppendToFile,
    NotesWriteFile,
    Usage,
    Profile,
    UserSettings,
    WorkspaceMessages,
    ConfigBundle,
}

impl CodexAuxiliaryOperation {
    /// 会话级辅助操作跟随逻辑会话 owner；账号级请求仍描述入口账号
    #[must_use]
    pub const fn follows_codex_session(self) -> bool {
        match self {
            Self::HistoryListWindows
            | Self::HistoryListItems
            | Self::HistoryReadItem
            | Self::HistorySearchContents
            | Self::NotesThreadHint
            | Self::NotesListFilesByPrefix
            | Self::NotesReadFile
            | Self::NotesSearchContents
            | Self::NotesAppendToFile
            | Self::NotesWriteFile => true,
            Self::Usage
            | Self::Profile
            | Self::UserSettings
            | Self::WorkspaceMessages
            | Self::ConfigBundle => false,
        }
    }
}

/// 每次执行重新认证后由 Core 创建，API 不能自行构造账号授权
#[derive(Debug, Clone)]
pub struct CodexAuthorizedContext {
    pub(crate) client_api_key_id: ClientApiKeyId,
    /// fixed 模式的入口账号；session_pool 由 Provider 按会话归属解析执行账号
    pub(crate) entry_account_id: ProviderAccountId,
    pub(crate) session_pool: bool,
    pub(crate) account_scope: Arc<FrozenAccountScope>,
    /// session_pool 首次辅助请求可能需要认领账号；此处冻结与模型请求同形的选号事实
    pub(crate) selection: Option<Arc<super::AttemptContext>>,
    pub(crate) cancellation: CancellationToken,
    pub(crate) deadline: Deadline,
}

impl CodexAuthorizedContext {
    #[must_use]
    pub const fn client_api_key_id(&self) -> &ClientApiKeyId {
        &self.client_api_key_id
    }

    #[must_use]
    pub const fn entry_account_id(&self) -> &ProviderAccountId {
        &self.entry_account_id
    }

    /// session_pool 下执行账号来自持久会话归属，不能退回入口账号
    #[must_use]
    pub const fn session_pool(&self) -> bool {
        self.session_pool
    }

    /// 辅助请求首次认领时复用的账号选择上下文
    #[must_use]
    pub fn selection(&self) -> Option<&super::AttemptContext> {
        self.selection.as_deref()
    }

    #[must_use]
    pub fn account_scope(&self) -> &FrozenAccountScope {
        &self.account_scope
    }

    #[must_use]
    pub const fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    #[must_use]
    pub const fn deadline(&self) -> Deadline {
        self.deadline
    }
}

/// 模型 attempt 已授权的会话范围
///
/// 只携带会话级 notes 重建需要的授权事实，刻意不含入口账号：
/// 模型请求没有固定入口身份，目标账号不能被伪称 entry identity
/// 由 [`AttemptContext::session_authorization`](super::AttemptContext::session_authorization) 投影
#[derive(Debug, Clone)]
pub struct CodexSessionAuthorization {
    pub(crate) client_api_key_id: ClientApiKeyId,
    pub(crate) account_scope: Arc<FrozenAccountScope>,
    pub(crate) cancellation: CancellationToken,
    pub(crate) deadline: Deadline,
}

impl CodexSessionAuthorization {
    #[must_use]
    pub const fn client_api_key_id(&self) -> &ClientApiKeyId {
        &self.client_api_key_id
    }

    #[must_use]
    pub fn account_scope(&self) -> &FrozenAccountScope {
        &self.account_scope
    }

    #[must_use]
    pub const fn cancellation(&self) -> &CancellationToken {
        &self.cancellation
    }

    #[must_use]
    pub const fn deadline(&self) -> Deadline {
        self.deadline
    }
}

/// 仅携带业务 bytes；API 过滤凭据、Cookie、账号身份与传输头
#[derive(Debug)]
pub struct CodexAuxiliaryRequest {
    pub operation: CodexAuxiliaryOperation,
    pub query: Option<String>,
    pub headers: Vec<MiddlewareHeader>,
    pub body: Bytes,
}

/// Provider 从当前凭据解析的真实身份事实，不包含 OAuth secret
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexIdentity {
    pub user_id: String,
    pub account_id: String,
    pub email: Option<String>,
    pub plan_type: Option<String>,
    pub is_fedramp: bool,
}

/// 仅接受 Core 已授权的固定账号；不支持任意 URL、换号或业务重放
/// 返回正文沿用惰性 body 合同，关闭或丢弃必须释放上游资源
pub trait CodexGateway: Send + Sync {
    fn identity<'a>(
        &'a self,
        context: &'a CodexAuthorizedContext,
    ) -> BoxFuture<'a, Result<CodexIdentity, ProviderError>>;

    fn auxiliary<'a>(
        &'a self,
        context: &'a CodexAuthorizedContext,
        request: CodexAuxiliaryRequest,
    ) -> BoxFuture<'a, Result<MiddlewareResponse, ProviderError>>;
}
