-- 原生访问模式与会话归属；既有绑定保持固定入口账号语义
alter table client_api_key_codex_bindings
    add column mode text not null default 'fixed'
        check (mode in ('fixed', 'session_pool'));

-- 同一 Key 的逻辑会话只认领一个授权池账号，创建后不迁移
create table client_api_key_codex_sessions (
    key_id text not null references client_api_keys(id) on delete cascade,
    session_key text not null,
    account_id text not null references provider_accounts(id) on delete restrict,
    created_at timestamptz not null default now(),
    primary key (key_id, session_key)
);

create index client_api_key_codex_sessions_account_idx
    on client_api_key_codex_sessions(account_id);
