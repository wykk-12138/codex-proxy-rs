//! 逻辑会话账号绑定与线程传输隔离键的单向派生

use gateway_core::operation::RawJsonPayload;
use gateway_core::policy::ClientApiKeyId;
use gateway_core::provider_ports::{ProviderSessionAffinityKey, ProviderSessionAlias};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::request_identity::{account_session_with_headers, follows_session_with_headers};
use crate::transport::protocol::responses::CodexResponsesRequest;
use crate::transport::request::derive_conversation_anchor;

const AFFINITY_KEY_HASH_LENGTH: usize = 12;

/// 一次请求派生出的账号亲和键及其结构化日志上下文
pub(crate) struct CodexSessionAffinity {
    key: ProviderSessionAffinityKey,
    turn_alias: Option<(ProviderSessionAffinityKey, ProviderSessionAlias)>,
    key_hash: String,
    anchor_source: &'static str,
    anchor: String,
    session_id: Option<String>,
    follow_only: bool,
}

impl CodexSessionAffinity {
    pub(crate) fn from_turn_alias(
        turn: ProviderSessionAffinityKey,
        alias: ProviderSessionAlias,
    ) -> Self {
        // 旧轮次可能登记在线程绑定下，统一还原为会话身份并保留原记录用于续期
        let root = alias.root_session_key.as_ref();
        let key = root.unwrap_or(&alias.session_key).clone();
        Self {
            key_hash: short_key_hash(&key),
            key,
            anchor_source: "turn-session",
            anchor: String::new(),
            session_id: None,
            follow_only: root.is_some() || alias.follow_only,
            turn_alias: Some((turn, alias)),
        }
    }

    pub(crate) fn turn_alias(&self) -> Option<&ProviderSessionAffinityKey> {
        self.turn_alias.as_ref().map(|(key, _)| key)
    }

    pub(crate) fn alias_record(&self) -> ProviderSessionAlias {
        // 当前请求可加严跟随约束，续期仍保留轮次最初登记的身份，不能改写关联
        self.turn_alias
            .as_ref()
            .map(|(_, alias)| alias.clone())
            .unwrap_or_else(|| ProviderSessionAlias {
                session_key: self.key.clone(),
                follow_only: self.follow_only,
                root_session_key: None,
            })
    }

    pub(crate) const fn follow_only(&self) -> bool {
        self.follow_only
    }

    pub(crate) fn with_follow_only(mut self, follow_only: bool) -> Self {
        self.follow_only |= follow_only;
        self
    }

    #[must_use]
    pub(crate) const fn key(&self) -> &ProviderSessionAffinityKey {
        &self.key
    }

    #[must_use]
    pub(crate) fn key_hash(&self) -> &str {
        &self.key_hash
    }

    /// 返回可持久化的客户端作用域不透明会话关联值
    #[must_use]
    pub(crate) fn persistence_hash(&self) -> &str {
        self.key.expose_to_store()
    }

    #[must_use]
    pub(crate) const fn anchor_source(&self) -> &'static str {
        self.anchor_source
    }

    #[must_use]
    pub(crate) fn anchor(&self) -> &str {
        &self.anchor
    }

    #[must_use]
    pub(crate) fn session_id(&self) -> Option<&str> {
        self.session_id.as_deref()
    }

    #[must_use]
    pub(crate) const fn session_id_present(&self) -> bool {
        self.session_id.is_some()
    }

    #[must_use]
    pub(crate) fn into_key(self) -> ProviderSessionAffinityKey {
        self.key
    }
}

/// 将原始 response ID 投影为客户端作用域的不可逆关联值
#[must_use]
pub(crate) fn derive_previous_response_id_hash(
    previous_response_id: &str,
    client_api_key_id: &ClientApiKeyId,
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"codex-previous-response-observation-v1\0");
    hasher.update(client_api_key_id.as_str().as_bytes());
    hasher.update(b"\0");
    hasher.update(previous_response_id.as_bytes());
    hex::encode(hasher.finalize())
}

pub(crate) fn derive_codex_session_affinity(
    request: &CodexResponsesRequest,
    client_api_key_id: &ClientApiKeyId,
) -> Option<CodexSessionAffinity> {
    let session_id = non_empty(request.client_account_session_id.as_deref())?;
    session_affinity(
        "root-session",
        session_id.to_owned(),
        Some(session_id.to_owned()),
        client_api_key_id,
    )
    .map(|affinity| affinity.with_follow_only(request.client_account_follow_only))
}

/// session_pool 的会话键与软亲和共用同一不可逆派生
///
/// 模型请求与辅助请求必须命中同一键；follow_only 由持久归属单独表达，不叠加软亲和语义
pub(crate) fn derive_pool_session_affinity(
    session_id: &str,
    client_api_key_id: &ClientApiKeyId,
) -> Option<CodexSessionAffinity> {
    let session_id = non_empty(Some(session_id))?;
    session_affinity(
        "root-session",
        session_id.to_owned(),
        Some(session_id.to_owned()),
        client_api_key_id,
    )
}

/// 原生 JSON 端点与 Responses 共用绑定命名空间，Search id 是逻辑会话身份
pub(crate) fn derive_codex_endpoint_session_affinity(
    payload: &RawJsonPayload,
    client_api_key_id: &ClientApiKeyId,
    body_session_field: &str,
) -> Option<CodexSessionAffinity> {
    derive_endpoint_affinity_with_headers(payload, client_api_key_id, body_session_field, &[])
}

pub(crate) fn derive_endpoint_affinity_with_headers(
    payload: &RawJsonPayload,
    client_api_key_id: &ClientApiKeyId,
    body_session_field: &str,
    headers: &[gateway_core::engine::middleware::MiddlewareHeader],
) -> Option<CodexSessionAffinity> {
    let mut body = serde_json::from_slice::<Map<String, Value>>(payload.body()).unwrap_or_default();
    if let Some(session) = body.get(body_session_field).cloned() {
        body.entry("session_id").or_insert(session);
    }
    let session_id = account_session_with_headers(&body, payload.context(), headers)?;
    session_affinity(
        "root-session",
        session_id.clone(),
        Some(session_id),
        client_api_key_id,
    )
    .map(|affinity| {
        affinity.with_follow_only(follows_session_with_headers(
            &body,
            payload.context(),
            headers,
        ))
    })
}

pub(crate) fn derive_live_session_affinity(
    request: &gateway_core::operation::ProviderHttpRequest,
    headers: &[gateway_core::engine::middleware::MiddlewareHeader],
    client_api_key_id: &ClientApiKeyId,
) -> Option<CodexSessionAffinity> {
    let mut effective = headers.to_vec();
    effective.extend(
        request
            .headers()
            .iter()
            .filter(|h| {
                !headers
                    .iter()
                    .any(|override_header| override_header.name().eq_ignore_ascii_case(h.name()))
            })
            .map(|h| {
                gateway_core::engine::middleware::MiddlewareHeader::new(h.name(), h.value().clone())
            }),
    );
    // x-session-id 是实时通话身份，不代表根对话；只消费官方发送的 session-id
    let session_id = account_session_with_headers(&Map::new(), &Map::new(), &effective)?;
    session_affinity(
        "root-session",
        session_id.clone(),
        Some(session_id),
        client_api_key_id,
    )
    .map(|affinity| {
        affinity.with_follow_only(follows_session_with_headers(
            &Map::new(),
            &Map::new(),
            &effective,
        ))
    })
}

/// HTTP 降级等传输状态仍按线程隔离，不能跟随整个对话的账号绑定扩大范围
pub(crate) fn derive_codex_transport_key(
    request: &CodexResponsesRequest,
    client_api_key_id: &ClientApiKeyId,
) -> Option<ProviderSessionAffinityKey> {
    let (source, anchor) = non_empty(request.client_thread_id.as_deref())
        .map(|thread| ("thread-transport", thread.to_owned()))
        .or_else(|| derive_conversation_anchor(request))?;
    session_affinity(source, anchor, None, client_api_key_id).map(CodexSessionAffinity::into_key)
}

fn session_affinity(
    anchor_source: &'static str,
    anchor: String,
    session_id: Option<String>,
    client_api_key_id: &ClientApiKeyId,
) -> Option<CodexSessionAffinity> {
    let session_key = opaque_affinity_key(anchor_source, &anchor)?;
    let key = opaque_affinity_key(
        "client-session",
        &format!(
            "{}\0{}",
            client_api_key_id.as_str(),
            session_key.expose_to_store()
        ),
    )?;
    let key_hash = short_key_hash(&key);
    Some(CodexSessionAffinity {
        key,
        turn_alias: None,
        key_hash,
        anchor_source,
        anchor,
        session_id,
        follow_only: false,
    })
}

fn short_key_hash(key: &ProviderSessionAffinityKey) -> String {
    // 亲和键本身已经是 SHA-256；日志沿用 WebSocket 诊断的 12 位短哈希长度
    key.expose_to_store()
        .chars()
        .take(AFFINITY_KEY_HASH_LENGTH)
        .collect()
}

fn opaque_affinity_key(domain: &str, value: &str) -> Option<ProviderSessionAffinityKey> {
    let mut hasher = Sha256::new();
    hasher.update(b"codex-session-affinity-v1\0");
    hasher.update(domain.as_bytes());
    hasher.update(b"\0");
    hasher.update(value.as_bytes());
    ProviderSessionAffinityKey::try_new(hex::encode(hasher.finalize())).ok()
}

/// 恢复旧版 `cyber_policy` 的会话隔离键
///
/// 它只接受显式 session/conversation 或客户端明确给出的 prompt cache key，避免将
/// 请求内容哈希误当成长会话；`previous_response_id` 续写不参与该策略
pub(crate) fn derive_codex_cyber_policy_session_key(
    request: &CodexResponsesRequest,
    client_api_key_id: &ClientApiKeyId,
) -> Option<ProviderSessionAffinityKey> {
    if request.previous_response_id().is_some() {
        return None;
    }
    let session_id = non_empty(request.client_session_id.as_deref())
        .or_else(|| non_empty(request.client_conversation_id.as_deref()))
        .or_else(|| {
            request
                .explicit_prompt_cache_key
                .then(|| request.prompt_cache_key())
                .flatten()
                .and_then(|value| non_empty(Some(value)))
        })?;
    let mut hasher = Sha256::new();
    hasher.update(b"cyber-policy-session\0");
    hasher.update(client_api_key_id.as_str().as_bytes());
    hasher.update(b"\0");
    hasher.update(session_id.as_bytes());
    ProviderSessionAffinityKey::try_new(hex::encode(hasher.finalize())).ok()
}

fn non_empty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

/// 官方 Images 的轮次 ID 与 Responses turn_id 对应，关联必须隔离客户端 API Key
pub(crate) fn derive_turn_alias(
    turn_id: &str,
    client: &ClientApiKeyId,
) -> Option<ProviderSessionAffinityKey> {
    let turn_id = non_empty(Some(turn_id))?;
    opaque_affinity_key("client-turn", &format!("{}\0{turn_id}", client.as_str()))
}
