-- 发送前请求的逻辑输入快照；0036 已冻结，这里只新增表

-- 输入在发送前落库，但上游可能在建立响应锚之前失败（例如额度拒绝），
-- 这些条目还没有进入任何响应锚；下一次无父响应的全量重发据此逐位置对齐，
-- 复用已保存的原条目，而不是按当前条目数再归档一次
-- 每个窗口只保留最近一次发送前请求的一份快照，不充当窗口全部档案
create table client_api_key_codex_history_pending_inputs (
    key_id text not null references client_api_keys(id) on delete cascade,
    session_key text not null,
    window_id text not null,
    agent_name text,
    context_window_id text,
    previous_response_id text,
    item_ids jsonb not null,
    updated_at timestamptz not null default now(),
    primary key (key_id, session_key, window_id)
);
