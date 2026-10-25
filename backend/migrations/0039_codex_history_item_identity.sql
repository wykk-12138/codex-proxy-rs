-- history 条目的完整语义身份；0036 已冻结，这里只追加列

-- 同一窗口同一位置比较时使用：忽略 id/status 与文本分段类型等承载差异，
-- 密文与图片字段参与其中；不参与全局去重，也不替代 raw_object
alter table client_api_key_codex_history_items
    add column item_identity text not null default '';
