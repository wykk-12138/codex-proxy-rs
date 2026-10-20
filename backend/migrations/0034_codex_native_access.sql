-- 原生入口绑定与整站可信 origin；历史 Key 不建立绑定，默认关闭
create table client_api_key_codex_bindings (
    key_id text primary key references client_api_keys(id) on delete cascade,
    account_id text not null references provider_accounts(id) on delete restrict,
    enabled boolean not null
);

create index client_api_key_codex_bindings_account_idx
    on client_api_key_codex_bindings(account_id);

alter table runtime_settings add column codex_public_origin text;
