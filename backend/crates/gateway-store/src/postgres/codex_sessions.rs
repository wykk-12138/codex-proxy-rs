//! Codex 会话归属的 PostgreSQL owner；同一逻辑会话只认领一个授权池账号

use futures::future::BoxFuture;
use gateway_core::{
    account::ProviderAccountId,
    error::ErrorSource,
    policy::ClientApiKeyId,
    provider_ports::{
        CodexSessionClaim, CodexSessionClaimOutcome, CodexSessionOwnershipPort,
        ProviderSessionAffinityKey, ProviderStoreError, ProviderStoreErrorKind,
    },
};
use sqlx::PgPool;

/// 归属事实只存在这里；Redis 仍只承担准入、租约与可丢失的软亲和
#[derive(Clone)]
pub struct PgCodexSessionOwnership {
    pool: PgPool,
}

impl PgCodexSessionOwnership {
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    async fn claim_inner(
        &self,
        claim: &CodexSessionClaim,
    ) -> Result<CodexSessionClaimOutcome, ProviderStoreError> {
        let mut transaction = self
            .pool
            .begin()
            .await
            .map_err(|source| unavailable("begin Codex session claim", source))?;
        // 与凭据身份替换共用账号行锁：锁内复核选号时的凭据版本，替换后不能用旧身份发送
        let account: Option<(i64, String, String, bool)> = sqlx::query_as(
            "select credential_revision, provider_kind, authentication_kind, enabled
             from provider_accounts where id = $1 for update",
        )
        .bind(claim.account_id.as_str())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|source| unavailable("lock Codex session account", source))?;
        let Some((revision, provider, authentication, enabled)) = account else {
            return Ok(CodexSessionClaimOutcome::Unavailable);
        };
        if provider != "openai" || authentication != "oauth" || !enabled {
            return Ok(CodexSessionClaimOutcome::Unavailable);
        }
        if u64::try_from(revision).ok() != Some(claim.credential_revision) {
            return Ok(CodexSessionClaimOutcome::IdentityChanged);
        }
        let existing: Option<String> = sqlx::query_scalar(
            "select account_id from client_api_key_codex_sessions
             where key_id = $1 and session_key = $2",
        )
        .bind(claim.key_id.as_str())
        .bind(claim.session_key.expose_to_store())
        .fetch_optional(&mut *transaction)
        .await
        .map_err(|source| unavailable("read Codex session owner", source))?;
        let outcome = match existing {
            Some(owner) if owner == claim.account_id.as_str() => CodexSessionClaimOutcome::Owned,
            Some(owner) => CodexSessionClaimOutcome::ConflictingOwner(decode_owner(owner)?),
            None => {
                // 并发首请求只有一个插入成功；竞争方读取胜出的 owner，绝不覆盖
                // 新会话尚未判定采集来源；已有行保持迁移后的 legacy 默认
                let inserted = sqlx::query(
                    "insert into client_api_key_codex_sessions
                       (key_id, session_key, account_id, origin)
                     values ($1, $2, $3, 'unclassified')
                     on conflict (key_id, session_key) do nothing",
                )
                .bind(claim.key_id.as_str())
                .bind(claim.session_key.expose_to_store())
                .bind(claim.account_id.as_str())
                .execute(&mut *transaction)
                .await
                .map_err(|source| unavailable("claim Codex session owner", source))?;
                if inserted.rows_affected() == 1 {
                    CodexSessionClaimOutcome::Owned
                } else {
                    let owner: String = sqlx::query_scalar(
                        "select account_id from client_api_key_codex_sessions
                         where key_id = $1 and session_key = $2",
                    )
                    .bind(claim.key_id.as_str())
                    .bind(claim.session_key.expose_to_store())
                    .fetch_one(&mut *transaction)
                    .await
                    .map_err(|source| unavailable("read claimed Codex session owner", source))?;
                    if owner == claim.account_id.as_str() {
                        CodexSessionClaimOutcome::Owned
                    } else {
                        CodexSessionClaimOutcome::ConflictingOwner(decode_owner(owner)?)
                    }
                }
            }
        };
        transaction
            .commit()
            .await
            .map_err(|source| unavailable("commit Codex session claim", source))?;
        Ok(outcome)
    }
}

impl CodexSessionOwnershipPort for PgCodexSessionOwnership {
    fn load<'a>(
        &'a self,
        key_id: &'a ClientApiKeyId,
        session_key: &'a ProviderSessionAffinityKey,
    ) -> BoxFuture<'a, Result<Option<ProviderAccountId>, ProviderStoreError>> {
        Box::pin(async move {
            let owner: Option<String> = sqlx::query_scalar(
                "select account_id from client_api_key_codex_sessions
                 where key_id = $1 and session_key = $2",
            )
            .bind(key_id.as_str())
            .bind(session_key.expose_to_store())
            .fetch_optional(&self.pool)
            .await
            .map_err(|source| unavailable("load Codex session owner", source))?;
            owner.map(decode_owner).transpose()
        })
    }

    fn claim<'a>(
        &'a self,
        claim: &'a CodexSessionClaim,
    ) -> BoxFuture<'a, Result<CodexSessionClaimOutcome, ProviderStoreError>> {
        Box::pin(self.claim_inner(claim))
    }
}

fn decode_owner(owner: String) -> Result<ProviderAccountId, ProviderStoreError> {
    ProviderAccountId::new(owner).map_err(|source| {
        ProviderStoreError::caused_by(
            ProviderStoreErrorKind::InvalidData,
            "decode Codex session owner",
            ErrorSource::new(source),
        )
    })
}

fn unavailable(operation: &'static str, source: sqlx::Error) -> ProviderStoreError {
    ProviderStoreError::caused_by(
        ProviderStoreErrorKind::Unavailable,
        operation,
        ErrorSource::new(source),
    )
}
