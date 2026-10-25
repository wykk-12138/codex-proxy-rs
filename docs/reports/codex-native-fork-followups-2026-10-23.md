# CPRS 原生 Codex 二开后续修复与优化建议

> 依据：[二开审核报告](codex-native-fork-review-2026-10-23.md)及其修复复审
> 对象：`codex-native` 分支 `36c87b82` 加未提交的修复工作区；日期 2026-10-23
> 只列有具体依据的项；按项目规则，不为假想场景增加防御分支或兼容兜底，需要权衡的项单独说明

## 一、建议现在做

成本低、依据明确，可以一起提交

### 1. 删除无依据的 `mode` 兼容兜底

- **后端**：`gateway-store/src/postgres/client_keys.rs` 的 `decode_codex_binding` 在 `mode` 为 `None` 时按 `Fixed` 处理，注释称是"旧 shape"
  - `mode` 列为 `not null default 'fixed'`；没有绑定时 `account_id` 同为空，函数提前返回
  - 不存在"有账号无 mode"的行，该分支是死代码
  - 改法：`None` 按数据错误处理
- **前端**：`api/modules/keys.ts` 把 `mode?` 声明为可选，`useApiKeyEditor.ts`、`index.vue` 里有 `?? 'fixed'`，`ApiKeyFormModal.vue` 的 `updateMode` 把未知值当作 fixed
  - 后端 `ClientKeyView` 总是返回 `mode`
  - 改法：`mode` 改为必填，去掉这些兜底
- 依据：AGENTS.md 要求处理本次引入的无依据兼容分支

### 2. `docs/api.md` 写明会话池只使用 OpenAI OAuth 账号

- 当前只有 `docs/architecture.md` 与 `deploy/README.md` 这样写，API 合同的会话池段缺这一句

### 3. 补 coordinator 多轮重试的 pool 标志测试

- 扩展 `gateway-core/tests/engine/execution/codex.rs` 现有的 `AttemptWiringProvider`：第一轮返回可重试错误，断言第二个 attempt 仍带 `codex_session_pool`，且 `required_account` 为空
- 是复审剩余缺口里成本最低的一项，防止以后改 coordinator 时丢掉 `coordinator.rs` 中的单行透传

## 二、下次合并上游时必须一起做

### 4. 原生辅助请求的出站标识隔离

- 对应上游 `7a60bc6d`（按账号隔离出站标识）与 `28fa655c`（input turn 作用域）
- 合并后模型请求发往上游的 `session_id`、`thread_id` 等改为按账号 installation 哈希的值，而 `providers/openai/src/transport/native.rs` 的 history/notes 请求仍原样转发正文 `context.session_id` 与 turn metadata 请求头
- 改法：复用上游 `transport/correlation.rs` 的映射，用执行账号的 installation 对辅助请求做同样处理；fixed 与 session_pool 都需要
- **合并前需要决定**：已有会话的模型轮次与 history/notes 在上游按原值存储，合并后换成映射值，官方存储中这些会话可能无法再按原会话关联。可以接受这个断点，也可以评估是否对原生入口豁免隔离
- 合并后需验收既有会话的历史标识连续性

## 三、可做的简化与一致性改进

不阻塞，适合下次修改这部分代码时顺带处理

### 5. 合并两套认领结果处理

- `providers/openai/src/credential/selector.rs` 的 claim 分支与 `providers/openai/src/provider/native.rs` 的辅助认领循环，各自处理 `Owned`、`ConflictingOwner`、`IdentityChanged`、`Unavailable`
- 可以在 selector 提一个小函数，把认领结果统一映射为"继续 / 跟随 owner / 重试快照 / 失败"，两处只负责各自的租约与执行
- 收益：以后修改认领语义只改一处；代价：要动刚验收过的代码

### 6. 统一辅助请求的账号租约

- 现状：辅助请求只在首次认领时持有账号租约，并持有到正文结束；跟随已有 owner 和 fixed 模式都不持有
- 需要先决定辅助请求是否计入账号并发
  - 计入：三条路径都持有租约
  - 不计入：认领成功后立即释放，统一走 `unleased_auxiliary_target`，`NativeBody` 不再带租约字段
- 现在两种语义各沾一点

## 四、性能优化

有实测压力再做

### 7. PAT 账号辅助请求的 whoami 往返

- 现状：`native.rs` 的 `native_identity` 对 PAT 账号每次辅助请求都调用一次上游 whoami，`token_client.rs` 无缓存
- 不能直接改读数据库：PAT 的 `is_fedramp` 没有持久化，而它决定出站的 `x-openai-fedramp` 头
- 可行做法：PAT 导入、替换时已经调用过 whoami，把 `chatgpt_account_is_fedramp` 写入加密的凭据文档（无需迁移），`native_identity` 对 PAT 与 JWT 统一读已存储的事实
- 难点：已有 PAT 账号没有该字段，需要重新授权一次，或保留一次性 whoami 回填（属于兼容分支）
- 建议先观察 history/notes 的实际调用频率再决定

### 8. 会话归属的进程内缓存

- 现状：每次会话池请求都对 PostgreSQL 做一次主键查询
- 归属建立后不变，只随 Key 删除消失，而 Key 删除后认证先失败，缓存失效简单
- 但属于"必要缓存"，需要写明并发与失效约束；没有观察到数据库压力时不建议做

## 五、运维与文档

按需处理

### 9. 删除账号被会话归属拦截时的提示

- `gateway-store/src/postgres/provider_accounts/identity.rs` 的 `map_account_delete_error` 返回的冲突 id 固定为 `"Codex-bound account"`
- 管理端能否显示"被哪些 Key 的会话引用、需删除对应 Key"未核实；若只显示泛化冲突，建议改为可操作的文案
- 动手前先确认前端的实际错误展示

### 10. 插件文档补两句边界说明

- 原生请求的 endpoint 是 `/backend-api/codex/...`，按 `/v1/...` 字符串匹配的插件不会命中
- 插件发起的嵌套模型调用不跟随会话 owner
- 两条在复审中已认可为合理边界，只差文档

## 六、手工验收

代码测试补不了的证据

### 11. 官方客户端端到端验收

- 用测试 Key 开启会话池，跑一个会触发 history/notes 和长对话压缩的会话
- 在请求日志中核对模型请求与辅助请求的 `chatgpt-account-id` 始终相同
- 这是目前唯一能证明"history/notes 与推理同 owner"的证据
- 同时补齐前端界面的截图：锁定态、空候选、加载失败

## 不建议做

- **清理中间件改写会话后留下的归属**：选号先于中间件是既有设计，留下的是客户端真实会话的归属，没有危害
- **对正文多个 turn metadata 字段互判冲突**（`turnMetadata`、`turn_metadata` 等）：官方客户端只发 `client_metadata` 一处，属于假想场景
- **给归属表加 TTL 或迁移机制**：与"归属永久、不迁移"的明确需求冲突

## 建议顺序

1. 第 1、2、3 项一起做，都是小改动，可以一个提交
2. 第 4 项在下次合并上游时作为阻断项
3. 其余等有实际需求再做
