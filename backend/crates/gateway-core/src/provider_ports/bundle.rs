//! 按能力提供 Provider 所需的存储端口

use super::{
    CodexSessionContentPort, CodexSessionOwnershipPort, CodexSessionStatePort,
    OAuthPendingFlowPort, ProviderArtifactProfileCachePort, ProviderCatalogCachePort,
    ProviderCooldownPort, ProviderCredentialStatePort, ProviderLeasePort,
    ProviderRuntimePolicyPort, ProviderSessionAffinityPort, ProviderSessionExclusionPort,
};
use crate::account::{AccountFeedbackStats, ProviderAccountStore};
use std::{fmt, sync::Arc};

/// Provider 只能按能力取用端口，无法取得 Redis client 或 repository 集合
#[derive(Clone)]
pub struct ProviderStorePorts {
    accounts: Arc<dyn ProviderAccountStore>,
    leases: Arc<dyn ProviderLeasePort>,
    session_affinity: Arc<dyn ProviderSessionAffinityPort>,
    session_exclusions: Arc<dyn ProviderSessionExclusionPort>,
    account_feedback: Arc<AccountFeedbackStats>,
    catalog_cache: Arc<dyn ProviderCatalogCachePort>,
    artifact_profiles: Arc<dyn ProviderArtifactProfileCachePort>,
    credential_state: Arc<dyn ProviderCredentialStatePort>,
    cooldowns: Arc<dyn ProviderCooldownPort>,
    runtime_policy: Arc<dyn ProviderRuntimePolicyPort>,
    oauth_pending: Arc<dyn OAuthPendingFlowPort>,
    /// session_pool 的会话归属；未装配时该模式如实失败，fixed 不受影响
    codex_sessions: Option<Arc<dyn CodexSessionOwnershipPort>>,
    /// 原生会话状态（来源、代次、notes 落点与重建进度）
    codex_session_state: Option<Arc<dyn CodexSessionStatePort>>,
    /// history 条目与 notes 操作日志的内容存取
    codex_session_content: Option<Arc<dyn CodexSessionContentPort>>,
    diagnostics: Arc<dyn crate::diagnostics::OperationalDiagnostics>,
}

impl ProviderStorePorts {
    #[must_use]
    // 每个参数代表独立能力端口；合并为单一配置对象会隐藏 Provider 能力边界
    #[expect(clippy::too_many_arguments)]
    pub fn new(
        accounts: Arc<dyn ProviderAccountStore>,
        leases: Arc<dyn ProviderLeasePort>,
        session_affinity: Arc<dyn ProviderSessionAffinityPort>,
        session_exclusions: Arc<dyn ProviderSessionExclusionPort>,
        catalog_cache: Arc<dyn ProviderCatalogCachePort>,
        artifact_profiles: Arc<dyn ProviderArtifactProfileCachePort>,
        credential_state: Arc<dyn ProviderCredentialStatePort>,
        cooldowns: Arc<dyn ProviderCooldownPort>,
        runtime_policy: Arc<dyn ProviderRuntimePolicyPort>,
        oauth_pending: Arc<dyn OAuthPendingFlowPort>,
        diagnostics: Arc<dyn crate::diagnostics::OperationalDiagnostics>,
    ) -> Self {
        Self {
            accounts,
            leases,
            session_affinity,
            session_exclusions,
            account_feedback: Arc::new(AccountFeedbackStats::default()),
            catalog_cache,
            artifact_profiles,
            credential_state,
            cooldowns,
            runtime_policy,
            oauth_pending,
            codex_sessions: None,
            codex_session_state: None,
            codex_session_content: None,
            diagnostics,
        }
    }

    /// 装配会话归属端口；由组装根在持久存储就绪后注入
    #[must_use]
    pub fn with_codex_sessions(mut self, sessions: Arc<dyn CodexSessionOwnershipPort>) -> Self {
        self.codex_sessions = Some(sessions);
        self
    }

    #[must_use]
    pub fn codex_sessions(&self) -> Option<Arc<dyn CodexSessionOwnershipPort>> {
        self.codex_sessions.clone()
    }

    /// 装配原生会话状态端口；与 owner 端口共用同一条会话记录
    #[must_use]
    pub fn with_codex_session_state(mut self, state: Arc<dyn CodexSessionStatePort>) -> Self {
        self.codex_session_state = Some(state);
        self
    }

    #[must_use]
    pub fn codex_session_state(&self) -> Option<Arc<dyn CodexSessionStatePort>> {
        self.codex_session_state.clone()
    }

    /// 装配 history 与 notes 操作日志的内容端口
    #[must_use]
    pub fn with_codex_session_content(mut self, content: Arc<dyn CodexSessionContentPort>) -> Self {
        self.codex_session_content = Some(content);
        self
    }

    #[must_use]
    pub fn codex_session_content(&self) -> Option<Arc<dyn CodexSessionContentPort>> {
        self.codex_session_content.clone()
    }

    #[must_use]
    pub fn diagnostics(&self) -> Arc<dyn crate::diagnostics::OperationalDiagnostics> {
        self.diagnostics.clone()
    }

    #[must_use]
    pub fn accounts(&self) -> Arc<dyn ProviderAccountStore> {
        Arc::clone(&self.accounts)
    }

    #[must_use]
    pub fn leases(&self) -> Arc<dyn ProviderLeasePort> {
        Arc::clone(&self.leases)
    }

    #[must_use]
    pub fn session_affinity(&self) -> Arc<dyn ProviderSessionAffinityPort> {
        Arc::clone(&self.session_affinity)
    }

    #[must_use]
    pub fn session_exclusions(&self) -> Arc<dyn ProviderSessionExclusionPort> {
        Arc::clone(&self.session_exclusions)
    }

    #[must_use]
    pub fn account_feedback(&self) -> Arc<AccountFeedbackStats> {
        Arc::clone(&self.account_feedback)
    }

    #[must_use]
    pub fn catalog_cache(&self) -> Arc<dyn ProviderCatalogCachePort> {
        Arc::clone(&self.catalog_cache)
    }

    #[must_use]
    pub fn artifact_profiles(&self) -> Arc<dyn ProviderArtifactProfileCachePort> {
        Arc::clone(&self.artifact_profiles)
    }

    #[must_use]
    pub fn credential_state(&self) -> Arc<dyn ProviderCredentialStatePort> {
        Arc::clone(&self.credential_state)
    }

    #[must_use]
    pub fn cooldowns(&self) -> Arc<dyn ProviderCooldownPort> {
        Arc::clone(&self.cooldowns)
    }

    #[must_use]
    pub fn runtime_policy(&self) -> Arc<dyn ProviderRuntimePolicyPort> {
        Arc::clone(&self.runtime_policy)
    }

    #[must_use]
    pub fn oauth_pending(&self) -> Arc<dyn OAuthPendingFlowPort> {
        Arc::clone(&self.oauth_pending)
    }
}

impl fmt::Debug for ProviderStorePorts {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ProviderStorePorts([CAPABILITIES])")
    }
}
