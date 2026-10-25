-- 原生 history 采集的续链锚与窗口内条目唯一键；0036 已冻结，这里只追加列与索引

-- 响应的父响应原值与窗口定位一起保存，重启后仍能还原续链来源
alter table client_api_key_codex_history_responses
    add column previous_response_id text;

-- 同一窗口内的稳定条目 ID 只能对应一条条目；重复提交是幂等 no-op，不新增 ordinal
create unique index client_api_key_codex_history_items_identity_idx
    on client_api_key_codex_history_items(key_id, session_key, window_id, item_id);
