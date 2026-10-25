-- 每个上游响应准确的逻辑输入与产出条目引用；0036/0037 已冻结，这里只新增关系

-- 引用只保存条目 ID 与顺序，不重复保存条目 payload
-- input 在响应锚定时写入一次，output 随 response.output_item.done 批次追加
create table client_api_key_codex_history_response_items (
    key_id text not null,
    session_key text not null,
    response_id text not null,
    kind text not null check (kind in ('input', 'output')),
    position bigint not null check (position >= 0),
    item_id text not null,
    primary key (key_id, session_key, response_id, kind, position),
    foreign key (key_id, session_key, response_id)
        references client_api_key_codex_history_responses(key_id, session_key, response_id)
        on delete cascade
);

-- 续接时按引用定位当时前缀，不再依赖当前窗口尾部
create index client_api_key_codex_history_response_items_item_idx
    on client_api_key_codex_history_response_items(key_id, session_key, item_id);
