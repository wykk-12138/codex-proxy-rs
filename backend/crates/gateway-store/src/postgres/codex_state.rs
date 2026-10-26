//! Codex 原生会话状态、history 与 notes 操作日志的 PostgreSQL 权威实现
//!
//! 会话归属沿用既有 owner 表并在同一行保存来源、代次与 notes 副本落点
//! history 与 notes 内容按 Key 与逻辑会话隔离，顺序由会话游标原子分配
//! 不在日志或错误中输出请求正文、响应正文与凭据

use futures::future::BoxFuture;
use gateway_core::{
    account::ProviderAccountId,
    error::ErrorSource,
    policy::ClientApiKeyId,
    provider_ports::{
        CodexHistoryItem, CodexHistoryPendingInput, CodexHistoryRequest, CodexHistoryResponse,
        CodexHistoryResponseAnchor, CodexHistoryRole, CodexHistoryStoredItem,
        CodexHistoryStoredWindow, CodexHistoryWindow, CodexHistoryWindowMetadata,
        CodexNativeWindowId, CodexNotesCompletion, CodexNotesGeneration, CodexNotesOperation,
        CodexNotesOperationKind, CodexNotesOperationStatus, CodexNotesPublish,
        CodexNotesPublishOutcome, CodexNotesReplay, CodexNotesReplica, CodexSessionContentPort,
        CodexSessionOrigin, CodexSessionState, CodexSessionStatePort, NewCodexNotesOperation,
        ProviderSessionAffinityKey, ProviderStoreError, ProviderStoreErrorKind,
    },
};
use sqlx::{PgPool, Postgres, Transaction, types::Json};

/// 会话状态与内容共用的 PostgreSQL 适配器
/// 响应锚的行形状：窗口定位加上客户端声明的父响应原值
type ResponseAnchorRow = (String, Option<String>, Option<String>, Option<String>);

/// 发送前请求快照的行形状：窗口定位、父响应原值与逻辑输入引用
type PendingInputRow = (
    String,
    Option<String>,
    Option<String>,
    Option<String>,
    Json<Vec<String>>,
);

#[derive(Clone)]
pub struct PgCodexSessionState {
    pool: PgPool,
}

impl PgCodexSessionState {
    #[must_use]
    pub const fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// 读取一次响应的窗口定位与逻辑输入/产出引用
    async fn response_anchor(
        &self,
        key_id: &ClientApiKeyId,
        session_key: &ProviderSessionAffinityKey,
        response_id: &str,
    ) -> Result<Option<CodexHistoryResponseAnchor>, ProviderStoreError> {
        let row: Option<ResponseAnchorRow> = sqlx::query_as(
            "select window_id, agent_name, context_window_id, previous_response_id
             from client_api_key_codex_history_responses
             where key_id = $1 and session_key = $2 and response_id = $3",
        )
        .bind(key_id.as_str())
        .bind(session_key.expose_to_store())
        .bind(response_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|source| unavailable("load Codex history response", source))?;
        let Some((window_id, agent_name, context_window_id, previous_response_id)) = row else {
            return Ok(None);
        };
        let references: Vec<(String, String)> = sqlx::query_as(
            "select kind, item_id from client_api_key_codex_history_response_items
             where key_id = $1 and session_key = $2 and response_id = $3
             order by kind, position",
        )
        .bind(key_id.as_str())
        .bind(session_key.expose_to_store())
        .bind(response_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|source| unavailable("load Codex history response items", source))?;
        let mut input = Vec::new();
        let mut output = Vec::new();
        for (kind, item_id) in references {
            match kind.as_str() {
                "input" => input.push(item_id),
                "output" => output.push(item_id),
                _ => return Err(invalid("load Codex history response")),
            }
        }
        Ok(Some(CodexHistoryResponseAnchor {
            response_id: response_id.to_owned(),
            window: CodexHistoryWindow {
                window_id: CodexNativeWindowId::try_new(window_id)?,
                agent_name,
                context_window_id,
            },
            previous_response_id,
            input,
            output,
        }))
    }
}

/// owner 表中一条会话记录的当前事实
#[derive(sqlx::FromRow)]
struct SessionRow {
    account_id: String,
    origin: String,
    generation: i64,
    notes_account_id: Option<String>,
    notes_generation: Option<String>,
    replay_target_account_id: Option<String>,
    replay_target_generation: Option<String>,
    replay_frozen_through_seq: Option<i64>,
    replay_replayed_through_seq: Option<i64>,
}

impl SessionRow {
    fn into_state(self) -> Result<CodexSessionState, ProviderStoreError> {
        const OPERATION: &str = "decode Codex session state";
        let owner = decode_account(self.account_id)?;
        let notes_account = match self.notes_account_id {
            Some(account) => decode_account(account)?,
            None => owner.clone(),
        };
        let notes = match self.notes_generation {
            Some(generation) => CodexNotesReplica::rebuilt(
                notes_account,
                CodexNotesGeneration::try_new(generation)?,
            ),
            None => CodexNotesReplica::native(notes_account),
        };
        let notes_replay = match (self.replay_target_account_id, self.replay_target_generation) {
            (None, None) => None,
            (Some(account), Some(generation)) => {
                let frozen = self
                    .replay_frozen_through_seq
                    .ok_or_else(|| invalid(OPERATION))?;
                let replayed = self
                    .replay_replayed_through_seq
                    .ok_or_else(|| invalid(OPERATION))?;
                Some(CodexNotesReplay::try_new(
                    CodexNotesReplica::rebuilt(
                        decode_account(account)?,
                        CodexNotesGeneration::try_new(generation)?,
                    ),
                    non_negative(frozen, OPERATION)?,
                    non_negative(replayed, OPERATION)?,
                )?)
            }
            _ => return Err(invalid(OPERATION)),
        };
        Ok(CodexSessionState {
            origin: decode_origin(&self.origin)?,
            owner,
            generation: non_negative(self.generation, OPERATION)?,
            notes,
            notes_replay,
        })
    }
}

/// 一条已落库 notes 操作的原值
#[derive(sqlx::FromRow)]
struct NotesOperationRow {
    seq: i64,
    account_id: String,
    generation: Option<String>,
    agent_name: String,
    operation: String,
    status: String,
    request_body: String,
    response_body: Option<String>,
    response_status: Option<i32>,
}

impl NotesOperationRow {
    fn into_operation(self) -> Result<CodexNotesOperation, ProviderStoreError> {
        const OPERATION: &str = "decode Codex notes operation";
        let account = decode_account(self.account_id)?;
        let replica = match self.generation {
            Some(generation) => {
                CodexNotesReplica::rebuilt(account, CodexNotesGeneration::try_new(generation)?)
            }
            None => CodexNotesReplica::native(account),
        };
        let response_status = self
            .response_status
            .map(|status| u16::try_from(status).map_err(|_| invalid(OPERATION)))
            .transpose()?;
        Ok(CodexNotesOperation {
            seq: non_negative(self.seq, OPERATION)?,
            replica,
            agent_name: self.agent_name,
            kind: decode_operation_kind(&self.operation)?,
            status: decode_operation_status(&self.status)?,
            request: self.request_body,
            response: self.response_body,
            response_status,
        })
    }
}

/// 一条已落库 history 条目的原对象与投影
#[derive(sqlx::FromRow)]
struct HistoryItemRow {
    window_id: String,
    ordinal: i64,
    item_id: String,
    role: Option<String>,
    tool_namespace: Option<String>,
    tool_name: Option<String>,
    content_type: String,
    raw_object: String,
    text_projection: Option<String>,
    item_identity: String,
}

impl HistoryItemRow {
    fn into_stored_item(self) -> Result<CodexHistoryStoredItem, ProviderStoreError> {
        const OPERATION: &str = "decode Codex history item";
        Ok(CodexHistoryStoredItem {
            window_id: CodexNativeWindowId::try_new(self.window_id)?,
            ordinal: non_negative(self.ordinal, OPERATION)?,
            item: CodexHistoryItem {
                item_id: self.item_id,
                role: decode_role(self.role)?,
                tool_namespace: self.tool_namespace,
                tool_name: self.tool_name,
                content_type: self.content_type,
                raw: self.raw_object,
                text: self.text_projection,
                identity: self.item_identity,
            },
        })
    }
}

impl CodexSessionStatePort for PgCodexSessionState {
    fn load_session<'a>(
        &'a self,
        key_id: &'a ClientApiKeyId,
        session_key: &'a ProviderSessionAffinityKey,
    ) -> BoxFuture<'a, Result<Option<CodexSessionState>, ProviderStoreError>> {
        Box::pin(async move {
            let row: Option<SessionRow> = sqlx::query_as(
                "select account_id, origin, generation, notes_account_id, notes_generation,
                        replay_target_account_id, replay_target_generation,
                        replay_frozen_through_seq, replay_replayed_through_seq
                 from client_api_key_codex_sessions
                 where key_id = $1 and session_key = $2",
            )
            .bind(key_id.as_str())
            .bind(session_key.expose_to_store())
            .fetch_optional(&self.pool)
            .await
            .map_err(|source| unavailable("load Codex session state", source))?;
            row.map(SessionRow::into_state).transpose()
        })
    }

    fn resolve_session_origin<'a>(
        &'a self,
        key_id: &'a ClientApiKeyId,
        session_key: &'a ProviderSessionAffinityKey,
        origin: CodexSessionOrigin,
    ) -> BoxFuture<'a, Result<CodexSessionState, ProviderStoreError>> {
        Box::pin(async move {
            let resolved = match origin {
                CodexSessionOrigin::Managed => "managed",
                CodexSessionOrigin::Legacy => "legacy",
                CodexSessionOrigin::Unclassified => {
                    return Err(invalid("resolve Codex session origin"));
                }
            };
            let mut transaction = self
                .pool
                .begin()
                .await
                .map_err(|source| unavailable("begin Codex session origin", source))?;
            // 来源只能从未判定转为已判定；已判定行保持首次事实
            let updated: Option<SessionRow> = sqlx::query_as(
                "update client_api_key_codex_sessions set origin = $3
                 where key_id = $1 and session_key = $2 and origin = 'unclassified'
                 returning account_id, origin, generation, notes_account_id, notes_generation,
                           replay_target_account_id, replay_target_generation,
                           replay_frozen_through_seq, replay_replayed_through_seq",
            )
            .bind(key_id.as_str())
            .bind(session_key.expose_to_store())
            .bind(resolved)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| unavailable("resolve Codex session origin", source))?;
            let row = match updated {
                Some(row) => row,
                None => sqlx::query_as(
                    "select account_id, origin, generation, notes_account_id, notes_generation,
                            replay_target_account_id, replay_target_generation,
                            replay_frozen_through_seq, replay_replayed_through_seq
                     from client_api_key_codex_sessions
                     where key_id = $1 and session_key = $2",
                )
                .bind(key_id.as_str())
                .bind(session_key.expose_to_store())
                .fetch_optional(&mut *transaction)
                .await
                .map_err(|source| unavailable("load resolved Codex session origin", source))?
                .ok_or_else(|| invalid("resolve Codex session origin"))?,
            };
            let state = row.into_state()?;
            transaction
                .commit()
                .await
                .map_err(|source| unavailable("commit Codex session origin", source))?;
            Ok(state)
        })
    }

    fn save_notes_replay<'a>(
        &'a self,
        key_id: &'a ClientApiKeyId,
        session_key: &'a ProviderSessionAffinityKey,
        replay: &'a CodexNotesReplay,
    ) -> BoxFuture<'a, Result<(), ProviderStoreError>> {
        Box::pin(async move {
            let Some(generation) = replay.replica.generation() else {
                return Err(invalid("save Codex notes replay"));
            };
            let frozen = i64::try_from(replay.frozen_through_seq)
                .map_err(|_| invalid("save Codex notes replay"))?;
            let replayed = i64::try_from(replay.replayed_through_seq)
                .map_err(|_| invalid("save Codex notes replay"))?;
            let updated = sqlx::query(
                "update client_api_key_codex_sessions set
                   replay_target_account_id = $3,
                   replay_target_generation = $4,
                   replay_frozen_through_seq = $5,
                   replay_replayed_through_seq = $6
                 where key_id = $1 and session_key = $2",
            )
            .bind(key_id.as_str())
            .bind(session_key.expose_to_store())
            .bind(replay.replica.account().as_str())
            .bind(generation.expose_to_store())
            .bind(frozen)
            .bind(replayed)
            .execute(&self.pool)
            .await
            .map_err(|source| unavailable("save Codex notes replay", source))?;
            if updated.rows_affected() == 0 {
                return Err(invalid("save Codex notes replay"));
            }
            Ok(())
        })
    }

    fn publish_notes_replay<'a>(
        &'a self,
        publish: &'a CodexNotesPublish,
    ) -> BoxFuture<'a, Result<CodexNotesPublishOutcome, ProviderStoreError>> {
        Box::pin(async move {
            if publish.replica.generation().is_none() || publish.replica.account() != &publish.owner
            {
                return Err(invalid("publish Codex notes replay"));
            }
            let mut transaction = self
                .pool
                .begin()
                .await
                .map_err(|source| unavailable("begin Codex notes publish", source))?;
            // 与 claim 及凭据身份替换共用账号行锁，锁内复核准备阶段看到的账号身份
            // 两处按同一顺序加锁才不会与认领互等
            let account: Option<(i64, String, String, bool)> = sqlx::query_as(
                "select credential_revision, provider_kind, authentication_kind, enabled
                 from provider_accounts where id = $1 for update",
            )
            .bind(publish.owner.as_str())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| unavailable("lock Codex notes publish account", source))?;
            let Some((revision, provider, authentication, enabled)) = account else {
                return Ok(CodexNotesPublishOutcome::Unavailable);
            };
            if provider != "openai" || authentication != "oauth" || !enabled {
                return Ok(CodexNotesPublishOutcome::Unavailable);
            }
            if u64::try_from(revision).ok() != Some(publish.credential_revision) {
                return Ok(CodexNotesPublishOutcome::IdentityChanged);
            }
            // 会话行是切换发布的互斥点；并发切换只能有一个按读到的代次成功
            let row: Option<SessionRow> = sqlx::query_as(
                "select account_id, origin, generation, notes_account_id, notes_generation,
                        replay_target_account_id, replay_target_generation,
                        replay_frozen_through_seq, replay_replayed_through_seq
                 from client_api_key_codex_sessions
                 where key_id = $1 and session_key = $2 for update",
            )
            .bind(publish.key_id.as_str())
            .bind(publish.session_key.expose_to_store())
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| unavailable("lock Codex notes publish", source))?;
            let Some(row) = row else {
                return Err(invalid("publish Codex notes replay"));
            };
            if non_negative(row.generation, "publish Codex notes replay")?
                != publish.expected_generation
            {
                let state = row.into_state()?;
                return Ok(CodexNotesPublishOutcome::GenerationChanged(state));
            }
            // 只发布真正同步完成的目标域：保存的账号、代次、冻结位置与已重放位置
            // 必须与本次发布一致，不能仅凭日志终态就发布一个没重建过的域
            let Some(frozen) = publish_target(&row, &publish.replica, publish.through_seq)? else {
                return Ok(CodexNotesPublishOutcome::ReplayNotReady);
            };
            // 冻结目标之前必须全部终结，之后不能再有操作进入旧域
            let unresolved: i64 = sqlx::query_scalar(
                "select count(*) from client_api_key_codex_notes_operations
                 where key_id = $1 and session_key = $2 and seq <= $3
                   and status not in ('responded', 'failed')",
            )
            .bind(publish.key_id.as_str())
            .bind(publish.session_key.expose_to_store())
            .bind(frozen)
            .fetch_one(&mut *transaction)
            .await
            .map_err(|source| unavailable("count unresolved Codex notes operations", source))?;
            let beyond: i64 = sqlx::query_scalar(
                "select count(*) from client_api_key_codex_notes_operations
                 where key_id = $1 and session_key = $2 and seq > $3",
            )
            .bind(publish.key_id.as_str())
            .bind(publish.session_key.expose_to_store())
            .bind(frozen)
            .fetch_one(&mut *transaction)
            .await
            .map_err(|source| unavailable("count Codex notes operations beyond target", source))?;
            if unresolved != 0 || beyond != 0 {
                return Ok(CodexNotesPublishOutcome::ReplayNotReady);
            }
            let Some(generation) = publish.replica.generation() else {
                return Err(invalid("publish Codex notes replay"));
            };
            let row: SessionRow = sqlx::query_as(
                "update client_api_key_codex_sessions set
                   account_id = $3,
                   generation = generation + 1,
                   notes_account_id = $4,
                   notes_generation = $5,
                   replay_target_account_id = null,
                   replay_target_generation = null,
                   replay_frozen_through_seq = null,
                   replay_replayed_through_seq = null
                 where key_id = $1 and session_key = $2
                 returning account_id, origin, generation, notes_account_id, notes_generation,
                           replay_target_account_id, replay_target_generation,
                           replay_frozen_through_seq, replay_replayed_through_seq",
            )
            .bind(publish.key_id.as_str())
            .bind(publish.session_key.expose_to_store())
            .bind(publish.owner.as_str())
            .bind(publish.replica.account().as_str())
            .bind(generation.expose_to_store())
            .fetch_one(&mut *transaction)
            .await
            .map_err(|source| unavailable("publish Codex notes replay", source))?;
            let state = row.into_state()?;
            transaction
                .commit()
                .await
                .map_err(|source| unavailable("commit Codex notes publish", source))?;
            Ok(CodexNotesPublishOutcome::Published(state))
        })
    }
}

impl CodexSessionContentPort for PgCodexSessionState {
    fn commit_history_request<'a>(
        &'a self,
        record: &'a CodexHistoryRequest,
    ) -> BoxFuture<'a, Result<(), ProviderStoreError>> {
        Box::pin(async move {
            if record.items.is_empty() {
                return Ok(());
            }
            let mut transaction = self
                .pool
                .begin()
                .await
                .map_err(|source| unavailable("begin Codex history request", source))?;
            let first = allocate_history_ordinals(
                &mut transaction,
                &record.key_id,
                &record.session_key,
                record.items.len(),
            )
            .await?;
            insert_history_items(
                &mut transaction,
                &record.key_id,
                &record.session_key,
                &record.window,
                first,
                &record.items,
            )
            .await?;
            transaction
                .commit()
                .await
                .map_err(|source| unavailable("commit Codex history request", source))
        })
    }

    fn commit_history_response<'a>(
        &'a self,
        record: &'a CodexHistoryResponse,
    ) -> BoxFuture<'a, Result<(), ProviderStoreError>> {
        Box::pin(async move {
            let mut transaction = self
                .pool
                .begin()
                .await
                .map_err(|source| unavailable("begin Codex history response", source))?;
            if !record.items.is_empty() {
                let first = allocate_history_ordinals(
                    &mut transaction,
                    &record.key_id,
                    &record.session_key,
                    record.items.len(),
                )
                .await?;
                insert_history_items(
                    &mut transaction,
                    &record.key_id,
                    &record.session_key,
                    &record.window,
                    first,
                    &record.items,
                )
                .await?;
            }
            let existing: Option<String> = sqlx::query_scalar(
                "select window_id from client_api_key_codex_history_responses
                 where key_id = $1 and session_key = $2 and response_id = $3",
            )
            .bind(record.key_id.as_str())
            .bind(record.session_key.expose_to_store())
            .bind(&record.response_id)
            .fetch_optional(&mut *transaction)
            .await
            .map_err(|source| unavailable("read Codex history response", source))?;
            match existing {
                Some(window_id) if window_id != record.window.window_id.expose_to_store() => {
                    // 同一 response 只能属于一个逻辑窗口；后到的冲突批次整体回滚
                    return Err(conflict("commit Codex history response"));
                }
                Some(_) => {}
                None => {
                    sqlx::query(
                        "insert into client_api_key_codex_history_responses
                           (key_id, session_key, response_id, window_id, agent_name,
                            context_window_id, previous_response_id)
                         values ($1, $2, $3, $4, $5, $6, $7)",
                    )
                    .bind(record.key_id.as_str())
                    .bind(record.session_key.expose_to_store())
                    .bind(&record.response_id)
                    .bind(record.window.window_id.expose_to_store())
                    .bind(record.window.agent_name.as_deref())
                    .bind(record.window.context_window_id.as_deref())
                    .bind(record.previous_response_id.as_deref())
                    .execute(&mut *transaction)
                    .await
                    .map_err(|source| unavailable("save Codex history response", source))?;
                    // 逻辑输入引用只在首次锚定时写入，后续批次不改写当时输入
                    let input = record.input.iter().map(String::as_str).collect::<Vec<_>>();
                    append_response_items(&mut transaction, record, "input", &input).await?;
                }
            }
            let output = record
                .items
                .iter()
                .map(|item| item.item_id.as_str())
                .collect::<Vec<_>>();
            append_response_items(&mut transaction, record, "output", &output).await?;
            transaction
                .commit()
                .await
                .map_err(|source| unavailable("commit Codex history response", source))
        })
    }

    fn load_history_response<'a>(
        &'a self,
        key_id: &'a ClientApiKeyId,
        session_key: &'a ProviderSessionAffinityKey,
        response_id: &'a str,
    ) -> BoxFuture<'a, Result<Option<CodexHistoryResponseAnchor>, ProviderStoreError>> {
        Box::pin(async move { self.response_anchor(key_id, session_key, response_id).await })
    }

    fn load_latest_history_response<'a>(
        &'a self,
        key_id: &'a ClientApiKeyId,
        session_key: &'a ProviderSessionAffinityKey,
        window_id: &'a CodexNativeWindowId,
    ) -> BoxFuture<'a, Result<Option<CodexHistoryResponseAnchor>, ProviderStoreError>> {
        Box::pin(async move {
            // 锚定时刻最近的响应即当前逻辑上下文；同一时刻以响应 ID 稳定排序
            let response_id: Option<String> = sqlx::query_scalar(
                "select response_id from client_api_key_codex_history_responses
                 where key_id = $1 and session_key = $2 and window_id = $3
                 order by created_at desc, response_id desc limit 1",
            )
            .bind(key_id.as_str())
            .bind(session_key.expose_to_store())
            .bind(window_id.expose_to_store())
            .fetch_optional(&self.pool)
            .await
            .map_err(|source| unavailable("load latest Codex history response", source))?;
            let Some(response_id) = response_id else {
                return Ok(None);
            };
            self.response_anchor(key_id, session_key, &response_id)
                .await
        })
    }

    fn save_history_pending_input<'a>(
        &'a self,
        key_id: &'a ClientApiKeyId,
        session_key: &'a ProviderSessionAffinityKey,
        pending: &'a CodexHistoryPendingInput,
    ) -> BoxFuture<'a, Result<(), ProviderStoreError>> {
        Box::pin(async move {
            // 同一窗口只保留最近一次发送前请求的快照：失败 attempt 的尾部输入
            // 还没有响应锚，下一次全量重发只能靠它找回原条目
            sqlx::query(
                "insert into client_api_key_codex_history_pending_inputs
                   (key_id, session_key, window_id, agent_name, context_window_id,
                    previous_response_id, item_ids, updated_at)
                 values ($1, $2, $3, $4, $5, $6, $7, now())
                 on conflict (key_id, session_key, window_id) do update set
                   agent_name = excluded.agent_name,
                   context_window_id = excluded.context_window_id,
                   previous_response_id = excluded.previous_response_id,
                   item_ids = excluded.item_ids,
                   updated_at = now()",
            )
            .bind(key_id.as_str())
            .bind(session_key.expose_to_store())
            .bind(pending.window.window_id.expose_to_store())
            .bind(pending.window.agent_name.as_deref())
            .bind(pending.window.context_window_id.as_deref())
            .bind(pending.previous_response_id.as_deref())
            .bind(Json(pending.input.clone()))
            .execute(&self.pool)
            .await
            .map_err(|source| unavailable("save Codex history pending input", source))?;
            Ok(())
        })
    }

    fn load_history_pending_input<'a>(
        &'a self,
        key_id: &'a ClientApiKeyId,
        session_key: &'a ProviderSessionAffinityKey,
        window_id: &'a CodexNativeWindowId,
    ) -> BoxFuture<'a, Result<Option<CodexHistoryPendingInput>, ProviderStoreError>> {
        Box::pin(async move {
            let row: Option<PendingInputRow> = sqlx::query_as(
                "select window_id, agent_name, context_window_id, previous_response_id, item_ids
                 from client_api_key_codex_history_pending_inputs
                 where key_id = $1 and session_key = $2 and window_id = $3",
            )
            .bind(key_id.as_str())
            .bind(session_key.expose_to_store())
            .bind(window_id.expose_to_store())
            .fetch_optional(&self.pool)
            .await
            .map_err(|source| unavailable("load Codex history pending input", source))?;
            let Some((window_id, agent_name, context_window_id, previous_response_id, input)) = row
            else {
                return Ok(None);
            };
            Ok(Some(CodexHistoryPendingInput {
                window: CodexHistoryWindow {
                    window_id: CodexNativeWindowId::try_new(window_id)?,
                    agent_name,
                    context_window_id,
                },
                previous_response_id,
                input: input.0,
            }))
        })
    }

    fn load_history_windows<'a>(
        &'a self,
        key_id: &'a ClientApiKeyId,
        session_key: &'a ProviderSessionAffinityKey,
    ) -> BoxFuture<'a, Result<Vec<CodexHistoryStoredWindow>, ProviderStoreError>> {
        Box::pin(async move {
            // 目录与条目数只来自已落库条目；响应锚与发送前快照只补充同一窗口的
            // 已持久元数据，因此不能创建目录项也不能影响计数
            // union 按 (window_id, agent_name, context_window_id) 去重，同一窗口的
            // 多个 UUID 分别保留，不取最大值拼出不存在的组合
            let rows: Vec<(String, i64, Option<String>, Option<String>)> = sqlx::query_as(
                "with item_windows as (
                   select window_id, count(*) as item_count, min(ordinal) as first_ordinal
                   from client_api_key_codex_history_items
                   where key_id = $1 and session_key = $2
                   group by window_id
                 ), window_metadata as (
                   select window_id, agent_name, context_window_id
                   from client_api_key_codex_history_items
                   where key_id = $1 and session_key = $2
                   union
                   select window_id, agent_name, context_window_id
                   from client_api_key_codex_history_responses
                   where key_id = $1 and session_key = $2
                   union
                   select window_id, agent_name, context_window_id
                   from client_api_key_codex_history_pending_inputs
                   where key_id = $1 and session_key = $2
                 )
                 select item_windows.window_id, item_windows.item_count,
                        window_metadata.agent_name, window_metadata.context_window_id
                 from item_windows
                 join window_metadata on window_metadata.window_id = item_windows.window_id
                 order by item_windows.first_ordinal, window_metadata.agent_name nulls first,
                          window_metadata.context_window_id nulls first",
            )
            .bind(key_id.as_str())
            .bind(session_key.expose_to_store())
            .fetch_all(&self.pool)
            .await
            .map_err(|source| unavailable("load Codex history windows", source))?;
            let mut windows: Vec<CodexHistoryStoredWindow> = Vec::new();
            for (window_id, item_count, agent_name, context_window_id) in rows {
                let window_id = CodexNativeWindowId::try_new(window_id)?;
                let metadata = CodexHistoryWindowMetadata {
                    agent_name,
                    context_window_id,
                };
                // 条目序号在会话内唯一，同一窗口的行按首条序号连续排列
                match windows.last_mut() {
                    Some(window) if window.window_id == window_id => {
                        window.metadata.push(metadata);
                    }
                    _ => windows.push(CodexHistoryStoredWindow {
                        item_count: non_negative(item_count, "load Codex history windows")?,
                        window_id,
                        metadata: vec![metadata],
                    }),
                }
            }
            Ok(windows)
        })
    }

    fn load_history_items<'a>(
        &'a self,
        key_id: &'a ClientApiKeyId,
        session_key: &'a ProviderSessionAffinityKey,
    ) -> BoxFuture<'a, Result<Vec<CodexHistoryStoredItem>, ProviderStoreError>> {
        Box::pin(async move {
            let rows: Vec<HistoryItemRow> = sqlx::query_as(
                "select window_id, ordinal, item_id, role, tool_namespace, tool_name,
                        content_type, raw_object, text_projection, item_identity
                 from client_api_key_codex_history_items
                 where key_id = $1 and session_key = $2
                 order by ordinal",
            )
            .bind(key_id.as_str())
            .bind(session_key.expose_to_store())
            .fetch_all(&self.pool)
            .await
            .map_err(|source| unavailable("load Codex history items", source))?;
            rows.into_iter()
                .map(HistoryItemRow::into_stored_item)
                .collect()
        })
    }

    fn load_history_window_items<'a>(
        &'a self,
        key_id: &'a ClientApiKeyId,
        session_key: &'a ProviderSessionAffinityKey,
        window_id: &'a CodexNativeWindowId,
    ) -> BoxFuture<'a, Result<Vec<CodexHistoryStoredItem>, ProviderStoreError>> {
        Box::pin(async move {
            let rows: Vec<HistoryItemRow> = sqlx::query_as(
                "select window_id, ordinal, item_id, role, tool_namespace, tool_name,
                        content_type, raw_object, text_projection, item_identity
                 from client_api_key_codex_history_items
                 where key_id = $1 and session_key = $2 and window_id = $3
                 order by ordinal",
            )
            .bind(key_id.as_str())
            .bind(session_key.expose_to_store())
            .bind(window_id.expose_to_store())
            .fetch_all(&self.pool)
            .await
            .map_err(|source| unavailable("load Codex history window items", source))?;
            rows.into_iter()
                .map(HistoryItemRow::into_stored_item)
                .collect()
        })
    }

    fn append_notes_operation<'a>(
        &'a self,
        operation: &'a NewCodexNotesOperation,
    ) -> BoxFuture<'a, Result<CodexNotesOperation, ProviderStoreError>> {
        Box::pin(async move {
            let mut transaction = self
                .pool
                .begin()
                .await
                .map_err(|source| unavailable("begin Codex notes operation", source))?;
            let seq =
                allocate_notes_seq(&mut transaction, &operation.key_id, &operation.session_key)
                    .await?;
            sqlx::query(
                "insert into client_api_key_codex_notes_operations
                   (key_id, session_key, seq, account_id, generation, agent_name, operation,
                    status, request_body)
                 values ($1, $2, $3, $4, $5, $6, $7, 'unsent', $8)",
            )
            .bind(operation.key_id.as_str())
            .bind(operation.session_key.expose_to_store())
            .bind(seq)
            .bind(operation.replica.account().as_str())
            .bind(
                operation
                    .replica
                    .generation()
                    .map(CodexNotesGeneration::expose_to_store),
            )
            .bind(&operation.agent_name)
            .bind(operation_kind_wire(operation.kind))
            .bind(&operation.request)
            .execute(&mut *transaction)
            .await
            .map_err(|source| unavailable("append Codex notes operation", source))?;
            transaction
                .commit()
                .await
                .map_err(|source| unavailable("commit Codex notes operation", source))?;
            Ok(CodexNotesOperation {
                seq: non_negative(seq, "append Codex notes operation")?,
                replica: operation.replica.clone(),
                agent_name: operation.agent_name.clone(),
                kind: operation.kind,
                status: CodexNotesOperationStatus::Unsent,
                request: operation.request.clone(),
                response: None,
                response_status: None,
            })
        })
    }

    fn start_notes_operation<'a>(
        &'a self,
        key_id: &'a ClientApiKeyId,
        session_key: &'a ProviderSessionAffinityKey,
        seq: u64,
    ) -> BoxFuture<'a, Result<CodexNotesOperation, ProviderStoreError>> {
        Box::pin(async move {
            let seq = i64::try_from(seq).map_err(|_| invalid("start Codex notes operation"))?;
            let row: Option<NotesOperationRow> = sqlx::query_as(
                "update client_api_key_codex_notes_operations set status = 'in_flight'
                 where key_id = $1 and session_key = $2 and seq = $3 and status = 'unsent'
                 returning seq, account_id, generation, agent_name, operation, status,
                           request_body, response_body, response_status",
            )
            .bind(key_id.as_str())
            .bind(session_key.expose_to_store())
            .bind(seq)
            .fetch_optional(&self.pool)
            .await
            .map_err(|source| unavailable("start Codex notes operation", source))?;
            if let Some(row) = row {
                return row.into_operation();
            }
            match load_notes_operation(&self.pool, key_id, session_key, seq).await? {
                Some(_) => Err(conflict("start Codex notes operation")),
                None => Err(invalid("start Codex notes operation")),
            }
        })
    }

    fn complete_notes_operation<'a>(
        &'a self,
        completion: &'a CodexNotesCompletion,
    ) -> BoxFuture<'a, Result<CodexNotesOperation, ProviderStoreError>> {
        Box::pin(async move {
            let status = operation_status_wire(completion.status)
                .ok_or_else(|| invalid("complete Codex notes operation"))?;
            let seq = i64::try_from(completion.seq)
                .map_err(|_| invalid("complete Codex notes operation"))?;
            if completion.status == CodexNotesOperationStatus::Responded
                && completion.response.is_none()
            {
                return Err(invalid("complete Codex notes operation"));
            }
            let row: Option<NotesOperationRow> = sqlx::query_as(
                "update client_api_key_codex_notes_operations set
                   status = $4,
                   response_body = $5,
                   response_status = $6,
                   completed_at = now()
                 where key_id = $1 and session_key = $2 and seq = $3
                   and ((status = 'unsent' and $4 = 'failed')
                     or (status = 'in_flight' and $4 in ('responded', 'failed', 'unknown')))
                 returning seq, account_id, generation, agent_name, operation, status,
                           request_body, response_body, response_status",
            )
            .bind(completion.key_id.as_str())
            .bind(completion.session_key.expose_to_store())
            .bind(seq)
            .bind(status)
            .bind(completion.response.as_deref())
            .bind(completion.response_status.map(i32::from))
            .fetch_optional(&self.pool)
            .await
            .map_err(|source| unavailable("complete Codex notes operation", source))?;
            if let Some(row) = row {
                return row.into_operation();
            }
            match load_notes_operation(&self.pool, &completion.key_id, &completion.session_key, seq)
                .await?
            {
                Some(_) => Err(conflict("complete Codex notes operation")),
                None => Err(invalid("complete Codex notes operation")),
            }
        })
    }

    fn load_notes_operations<'a>(
        &'a self,
        key_id: &'a ClientApiKeyId,
        session_key: &'a ProviderSessionAffinityKey,
    ) -> BoxFuture<'a, Result<Vec<CodexNotesOperation>, ProviderStoreError>> {
        Box::pin(async move {
            let rows: Vec<NotesOperationRow> = sqlx::query_as(
                "select seq, account_id, generation, agent_name, operation, status,
                        request_body, response_body, response_status
                 from client_api_key_codex_notes_operations
                 where key_id = $1 and session_key = $2 order by seq",
            )
            .bind(key_id.as_str())
            .bind(session_key.expose_to_store())
            .fetch_all(&self.pool)
            .await
            .map_err(|source| unavailable("load Codex notes operations", source))?;
            rows.into_iter()
                .map(NotesOperationRow::into_operation)
                .collect()
        })
    }
}

async fn allocate_history_ordinals(
    transaction: &mut Transaction<'_, Postgres>,
    key_id: &ClientApiKeyId,
    session_key: &ProviderSessionAffinityKey,
    count: usize,
) -> Result<i64, ProviderStoreError> {
    let count = i64::try_from(count).map_err(|_| invalid("allocate Codex history ordinals"))?;
    // 单条 upsert 同时取行锁并推进游标；并发批次各得连续且不重叠的区间
    let next: i64 = sqlx::query_scalar(
        "insert into client_api_key_codex_session_counters
           (key_id, session_key, next_history_ordinal)
         values ($1, $2, $3)
         on conflict (key_id, session_key) do update
           set next_history_ordinal =
             client_api_key_codex_session_counters.next_history_ordinal + $3
         returning next_history_ordinal",
    )
    .bind(key_id.as_str())
    .bind(session_key.expose_to_store())
    .bind(count)
    .fetch_one(&mut **transaction)
    .await
    .map_err(|source| unavailable("allocate Codex history ordinals", source))?;
    next.checked_sub(count)
        .ok_or_else(|| invalid("allocate Codex history ordinals"))
}

async fn allocate_notes_seq(
    transaction: &mut Transaction<'_, Postgres>,
    key_id: &ClientApiKeyId,
    session_key: &ProviderSessionAffinityKey,
) -> Result<i64, ProviderStoreError> {
    // 会话内 seq 从 1 开始；0 只表示空日志/尚未重放，首条真实写入必须能被 advance 选中
    // 游标与 history 共用同一行，history 先建行时 next_notes_seq 仍是 0，不能据此分配 seq 0
    let next: i64 = sqlx::query_scalar(
        "insert into client_api_key_codex_session_counters
           (key_id, session_key, next_notes_seq)
         values ($1, $2, 2)
         on conflict (key_id, session_key) do update
           set next_notes_seq =
             greatest(client_api_key_codex_session_counters.next_notes_seq, 1) + 1
         returning next_notes_seq",
    )
    .bind(key_id.as_str())
    .bind(session_key.expose_to_store())
    .fetch_one(&mut **transaction)
    .await
    .map_err(|source| unavailable("allocate Codex notes seq", source))?;
    // 游标保存下一个可用值，本次分配的是它之前的位置
    next.checked_sub(1)
        .filter(|seq| *seq >= 1)
        .ok_or_else(|| invalid("allocate Codex notes seq"))
}

async fn insert_history_items(
    transaction: &mut Transaction<'_, Postgres>,
    key_id: &ClientApiKeyId,
    session_key: &ProviderSessionAffinityKey,
    window: &CodexHistoryWindow,
    first_ordinal: i64,
    items: &[CodexHistoryItem],
) -> Result<(), ProviderStoreError> {
    for (offset, item) in items.iter().enumerate() {
        let offset = i64::try_from(offset).map_err(|_| invalid("save Codex history item"))?;
        let ordinal = first_ordinal
            .checked_add(offset)
            .ok_or_else(|| invalid("save Codex history item"))?;
        sqlx::query(
            "insert into client_api_key_codex_history_items
               (key_id, session_key, ordinal, window_id, agent_name, context_window_id,
                item_id, role, tool_namespace, tool_name, content_type, raw_object,
                text_projection, item_identity)
             values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
             on conflict (key_id, session_key, window_id, item_id) do nothing",
        )
        .bind(key_id.as_str())
        .bind(session_key.expose_to_store())
        .bind(ordinal)
        .bind(window.window_id.expose_to_store())
        .bind(window.agent_name.as_deref())
        .bind(window.context_window_id.as_deref())
        .bind(&item.item_id)
        .bind(item.role.map(role_wire))
        .bind(item.tool_namespace.as_deref())
        .bind(item.tool_name.as_deref())
        .bind(&item.content_type)
        .bind(&item.raw)
        .bind(item.text.as_deref())
        .bind(&item.identity)
        .execute(&mut **transaction)
        .await
        .map_err(|source| unavailable("save Codex history item", source))?;
    }
    Ok(())
}

/// 按响应保存一批条目引用；位置从该 kind 已有最大位置继续
async fn append_response_items(
    transaction: &mut Transaction<'_, Postgres>,
    record: &CodexHistoryResponse,
    kind: &'static str,
    item_ids: &[&str],
) -> Result<(), ProviderStoreError> {
    if item_ids.is_empty() {
        return Ok(());
    }
    let last: Option<i64> = sqlx::query_scalar(
        "select max(position) from client_api_key_codex_history_response_items
         where key_id = $1 and session_key = $2 and response_id = $3 and kind = $4",
    )
    .bind(record.key_id.as_str())
    .bind(record.session_key.expose_to_store())
    .bind(&record.response_id)
    .bind(kind)
    .fetch_one(&mut **transaction)
    .await
    .map_err(|source| unavailable("read Codex history response items", source))?;
    let start = match last {
        Some(position) => position
            .checked_add(1)
            .ok_or_else(|| invalid("save Codex history response items"))?,
        None => 0,
    };
    for (offset, item_id) in item_ids.iter().enumerate() {
        let offset =
            i64::try_from(offset).map_err(|_| invalid("save Codex history response items"))?;
        let position = start
            .checked_add(offset)
            .ok_or_else(|| invalid("save Codex history response items"))?;
        sqlx::query(
            "insert into client_api_key_codex_history_response_items
               (key_id, session_key, response_id, kind, position, item_id)
             values ($1, $2, $3, $4, $5, $6)",
        )
        .bind(record.key_id.as_str())
        .bind(record.session_key.expose_to_store())
        .bind(&record.response_id)
        .bind(kind)
        .bind(position)
        .bind(*item_id)
        .execute(&mut **transaction)
        .await
        .map_err(|source| unavailable("save Codex history response items", source))?;
    }
    Ok(())
}

/// 校验保存的重建目标与本次发布一致且已完整重放
///
/// 返回冻结位置；没有保存目标、账号或代次不符、进度未到冻结位置时返回 None
/// 空日志仍要求已保存 through=0 的目标，不能用登录终态代替同步事实
fn publish_target(
    row: &SessionRow,
    replica: &CodexNotesReplica,
    through_seq: u64,
) -> Result<Option<i64>, ProviderStoreError> {
    const OPERATION: &str = "validate Codex notes publish target";
    let (Some(account), Some(generation)) = (
        row.replay_target_account_id.as_deref(),
        row.replay_target_generation.as_deref(),
    ) else {
        return Ok(None);
    };
    let Some(replica_generation) = replica.generation() else {
        return Ok(None);
    };
    if account != replica.account().as_str() || generation != replica_generation.expose_to_store() {
        return Ok(None);
    }
    let (Some(frozen), Some(replayed)) = (
        row.replay_frozen_through_seq,
        row.replay_replayed_through_seq,
    ) else {
        return Ok(None);
    };
    if non_negative(frozen, OPERATION)? != through_seq
        || non_negative(replayed, OPERATION)? != through_seq
    {
        return Ok(None);
    }
    Ok(Some(frozen))
}

async fn load_notes_operation(
    pool: &PgPool,
    key_id: &ClientApiKeyId,
    session_key: &ProviderSessionAffinityKey,
    seq: i64,
) -> Result<Option<CodexNotesOperation>, ProviderStoreError> {
    let row: Option<NotesOperationRow> = sqlx::query_as(
        "select seq, account_id, generation, agent_name, operation, status,
                request_body, response_body, response_status
         from client_api_key_codex_notes_operations
         where key_id = $1 and session_key = $2 and seq = $3",
    )
    .bind(key_id.as_str())
    .bind(session_key.expose_to_store())
    .bind(seq)
    .fetch_optional(pool)
    .await
    .map_err(|source| unavailable("load Codex notes operation", source))?;
    row.map(NotesOperationRow::into_operation).transpose()
}

fn decode_account(value: String) -> Result<ProviderAccountId, ProviderStoreError> {
    ProviderAccountId::new(value).map_err(|source| {
        ProviderStoreError::caused_by(
            ProviderStoreErrorKind::InvalidData,
            "decode Codex session account",
            ErrorSource::new(source),
        )
    })
}

fn decode_origin(value: &str) -> Result<CodexSessionOrigin, ProviderStoreError> {
    match value {
        "unclassified" => Ok(CodexSessionOrigin::Unclassified),
        "managed" => Ok(CodexSessionOrigin::Managed),
        "legacy" => Ok(CodexSessionOrigin::Legacy),
        _ => Err(invalid("decode Codex session origin")),
    }
}

fn decode_role(value: Option<String>) -> Result<Option<CodexHistoryRole>, ProviderStoreError> {
    value
        .map(|value| match value.as_str() {
            "user" => Ok(CodexHistoryRole::User),
            "assistant" => Ok(CodexHistoryRole::Assistant),
            "tool" => Ok(CodexHistoryRole::Tool),
            "system" => Ok(CodexHistoryRole::System),
            "developer" => Ok(CodexHistoryRole::Developer),
            _ => Err(invalid("decode Codex history role")),
        })
        .transpose()
}

const fn role_wire(role: CodexHistoryRole) -> &'static str {
    match role {
        CodexHistoryRole::User => "user",
        CodexHistoryRole::Assistant => "assistant",
        CodexHistoryRole::Tool => "tool",
        CodexHistoryRole::System => "system",
        CodexHistoryRole::Developer => "developer",
    }
}

fn decode_operation_kind(value: &str) -> Result<CodexNotesOperationKind, ProviderStoreError> {
    match value {
        "write_file" => Ok(CodexNotesOperationKind::WriteFile),
        "append_to_file" => Ok(CodexNotesOperationKind::AppendToFile),
        _ => Err(invalid("decode Codex notes operation kind")),
    }
}

const fn operation_kind_wire(kind: CodexNotesOperationKind) -> &'static str {
    match kind {
        CodexNotesOperationKind::WriteFile => "write_file",
        CodexNotesOperationKind::AppendToFile => "append_to_file",
    }
}

fn decode_operation_status(value: &str) -> Result<CodexNotesOperationStatus, ProviderStoreError> {
    match value {
        "unsent" => Ok(CodexNotesOperationStatus::Unsent),
        "in_flight" => Ok(CodexNotesOperationStatus::InFlight),
        "responded" => Ok(CodexNotesOperationStatus::Responded),
        "failed" => Ok(CodexNotesOperationStatus::Failed),
        "unknown" => Ok(CodexNotesOperationStatus::Unknown),
        _ => Err(invalid("decode Codex notes operation status")),
    }
}

/// 只有真实终结观察可写入；未终结状态由 append 与 start 负责
const fn operation_status_wire(status: CodexNotesOperationStatus) -> Option<&'static str> {
    match status {
        CodexNotesOperationStatus::Responded => Some("responded"),
        CodexNotesOperationStatus::Failed => Some("failed"),
        CodexNotesOperationStatus::Unknown => Some("unknown"),
        CodexNotesOperationStatus::Unsent | CodexNotesOperationStatus::InFlight => None,
    }
}

fn non_negative(value: i64, operation: &'static str) -> Result<u64, ProviderStoreError> {
    u64::try_from(value).map_err(|_| invalid(operation))
}

fn invalid(operation: &'static str) -> ProviderStoreError {
    ProviderStoreError::new(ProviderStoreErrorKind::InvalidData, operation)
}

fn conflict(operation: &'static str) -> ProviderStoreError {
    ProviderStoreError::new(ProviderStoreErrorKind::Conflict, operation)
}

fn unavailable(operation: &'static str, source: sqlx::Error) -> ProviderStoreError {
    ProviderStoreError::caused_by(
        ProviderStoreErrorKind::Unavailable,
        operation,
        ErrorSource::new(source),
    )
}
