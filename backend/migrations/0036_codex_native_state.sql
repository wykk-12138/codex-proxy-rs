-- 原生会话状态、history 条目与 notes 操作日志；会话归属继续由既有 owner 表持有

-- 旧会话进入网关前已有云端状态，既有行按 legacy 处理；新认领由存储写入 unclassified
alter table client_api_key_codex_sessions
    add column origin text not null default 'legacy'
        check (origin in ('unclassified', 'managed', 'legacy')),
    add column generation bigint not null default 0,
    -- notes 副本落点；账号为空表示沿用 owner 账号，代次为空表示该账号原生域
    add column notes_account_id text,
    add column notes_generation text,
    add column replay_target_account_id text,
    add column replay_target_generation text,
    add column replay_frozen_through_seq bigint,
    add column replay_replayed_through_seq bigint,
    add constraint client_api_key_codex_sessions_notes_check check (
        (notes_generation is null or notes_account_id is not null)
        and (replay_target_account_id is null) = (replay_target_generation is null)
        and (replay_target_account_id is null) = (replay_frozen_through_seq is null)
        and (replay_target_account_id is null) = (replay_replayed_through_seq is null)
        and (replay_replayed_through_seq is null
             or replay_replayed_through_seq <= replay_frozen_through_seq)
    );

-- 会话内顺序分配的唯一游标；history 条目与 notes 操作共用一条会话记录
create table client_api_key_codex_session_counters (
    key_id text not null references client_api_keys(id) on delete cascade,
    session_key text not null,
    next_history_ordinal bigint not null default 0,
    next_notes_seq bigint not null default 0,
    primary key (key_id, session_key)
);

-- 原始模型 item 与可读投影分开保存；raw_object 保留官方原文，不重新序列化
create table client_api_key_codex_history_items (
    key_id text not null references client_api_keys(id) on delete cascade,
    session_key text not null,
    ordinal bigint not null check (ordinal >= 0),
    window_id text not null,
    agent_name text,
    context_window_id text,
    item_id text not null,
    role text check (role in ('user', 'assistant', 'tool', 'system', 'developer')),
    tool_namespace text,
    tool_name text,
    content_type text not null,
    raw_object text not null,
    text_projection text,
    created_at timestamptz not null default now(),
    primary key (key_id, session_key, ordinal)
);

create index client_api_key_codex_history_items_window_idx
    on client_api_key_codex_history_items(key_id, session_key, window_id, ordinal);

-- 已知上游响应与逻辑窗口的定位；同一 response 的后续批次复用首条窗口
create table client_api_key_codex_history_responses (
    key_id text not null references client_api_keys(id) on delete cascade,
    session_key text not null,
    response_id text not null,
    window_id text not null,
    agent_name text,
    context_window_id text,
    created_at timestamptz not null default now(),
    primary key (key_id, session_key, response_id)
);

-- 每次真实 notes 写入一行；seq 由会话游标分配，原始请求与响应按原字节保存
create table client_api_key_codex_notes_operations (
    key_id text not null references client_api_keys(id) on delete cascade,
    session_key text not null,
    seq bigint not null check (seq >= 0),
    account_id text not null,
    generation text,
    agent_name text not null,
    operation text not null check (operation in ('write_file', 'append_to_file')),
    status text not null check (status in ('unsent', 'in_flight', 'responded', 'failed', 'unknown')),
    request_body text not null,
    response_body text,
    response_status integer,
    created_at timestamptz not null default now(),
    completed_at timestamptz,
    primary key (key_id, session_key, seq),
    check (status <> 'responded' or response_body is not null)
);
