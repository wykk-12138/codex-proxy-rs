//! Codex 会话账号归属的持久事实端口；PostgreSQL 是唯一权威，Redis 不复制该事实

use futures::future::BoxFuture;

use super::{ProviderSessionAffinityKey, ProviderStoreError};
use crate::{account::ProviderAccountId, policy::ClientApiKeyId};

/// 首次认领输入；账号行锁内复核选号时观测到的凭据版本
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodexSessionClaim {
    pub key_id: ClientApiKeyId,
    pub session_key: ProviderSessionAffinityKey,
    pub account_id: ProviderAccountId,
    /// 选号时取得的账号凭据版本；身份替换会推进该版本
    pub credential_revision: u64,
}

/// 首次认领结果；只有 `Owned` 表示本次选中的账号就是会话 owner
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodexSessionClaimOutcome {
    Owned,
    /// 并发首请求已由其他账号认领；调用方释放租约后按该 owner 重新选号
    ConflictingOwner(ProviderAccountId),
    /// 账号行锁内发现凭据版本已变化，不能用旧身份快照发送
    IdentityChanged,
    /// 账号已不存在，或不再是可承载原生会话的 OpenAI OAuth 账号
    Unavailable,
}

/// 逻辑会话到授权池账号的持久归属
///
/// 归属建立后不迁移，也不设 TTL；读取失败必须如实失败，不能退回入口账号或任意选号
pub trait CodexSessionOwnershipPort: Send + Sync {
    fn load<'a>(
        &'a self,
        key_id: &'a ClientApiKeyId,
        session_key: &'a ProviderSessionAffinityKey,
    ) -> BoxFuture<'a, Result<Option<ProviderAccountId>, ProviderStoreError>>;

    /// 首次认领原子完成；已有归属只返回真实 owner，绝不覆盖
    fn claim<'a>(
        &'a self,
        claim: &'a CodexSessionClaim,
    ) -> BoxFuture<'a, Result<CodexSessionClaimOutcome, ProviderStoreError>>;
}
