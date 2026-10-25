# CPRS 原生 Codex 二开审核报告

> 审核对象：`codex-native` 分支 `36c87b82`；范围 `git diff v3.29.7..HEAD`
> 审核日期：2026-10-23；只读审核，未修改源码、未运行测试
> AI 未报告的问题不等于维护者已批准

## 基线

- `v3.29.7` 标签对象 `aeb01518`，指向 `78bc57c5`
- 范围内 17 个提交（16 个二开提交 + 1 个合并提交），131 个文件，+8918/−307 行，与审核提示一致
- 审核时工作区不干净，以下两项不计入已交付代码
  - `deploy/README.md` 有未提交改动：版本号改为 `0.163.0-alpha.11`，新增桌面版 Work 模式说明
  - `context.md` 未纳入版本管理；其中"生产运行 `3.29.7-codex-native.2`"与审核提示第 8 节"生产是官方镜像"矛盾，审核未据此判断代码对错

## 一、结论

**需求基本成立，实现大部分到位，但有一处与合同不符，必须修复**：会话池选号没有限定 OAuth 账号，见二、第 1 条

- **原生接入、白名单、404/405、旧前缀 404**：已实现
  - `gateway-api/src/openai/codex.rs` 用精确路由注册白名单路径，并显式让命名空间和旧前缀返回 404，不落入管理端 SPA
  - 错误方法由 axum 返回 405，`HEAD` 由 `reject_head` 单独拒绝
  - 普通 `/v1` 由认证目的 `Models` 隔离，不受影响
- **两种模式与锁定**：已实现
  - Store 的 `write_codex_binding` 在事务内拒绝修改 `accountId` 或 `mode`；更新时省略 `mode` 表示保留原模式
  - 前端提交只带 `enabled` 与原 `accountId`，后端同样拒绝修改，属于真锁定
- **会话键合同**：判定集中在 `gateway-protocol::codex_session_identity`，Provider 只做请求头适配，没有重复解释规则
  - 优先级、兜底、缺失或冲突返回 `InvalidRequest` 且禁止重试，与 `docs/api.md` 一致
  - 有一个细节偏差，见三、第 1 条
- **生命周期与持久化**：基本正确
  - 归属表主键 `(key_id, session_key)`；删 Key 级联删除归属，被引用账号受 `restrict` 保护
  - 认领与凭据身份替换共用 `provider_accounts` 行锁，锁内复核 `credential_revision`
  - 并发首绑靠唯一键加 `on conflict do nothing` 再读回，只有一个胜出者
  - Redis 只作提示，归属以 PostgreSQL 为准
- **方案复杂度**：在需求约束下属于最小合理；复用既有 Responses、SSE、WS、Search adapter 与执行、准入链路，新增一个端口、一张表和有限辅助操作，没有第二套调度

## 二、必须修复

### 1. 会话池的模型请求可能选中 OpenAI API Key 账号，认领失败后整次请求直接失败

- **位置**
  - `backend/crates/providers/openai/src/credential/selector.rs:381-385`：`select_with_cyber_policy` 固定 `requires_oauth: false`
  - `backend/crates/gateway-core/src/engine/execution/contract.rs:119`：会话池分支只把范围限定到 `openai` Provider，没有排除 API Key 类账号
  - `backend/crates/gateway-store/src/postgres/codex_sessions.rs:48`：认领遇到非 `oauth` 账号返回 `Unavailable`
  - `selector.rs:1082`：收到 `Unavailable` 直接返回 `NoEligibleCredential`，不排除该账号重新选号
- **触发条件**：会话池 Key 的有效分组含 OpenAI API Key 账号（或分组留空即全部账号），新会话首个模型请求被调度到该账号；sticky 或 smart 策略下可能连续选中
- **规范依据**
  - `docs/architecture.md`："session_pool 保留 Key 有效范围内的 OpenAI OAuth 授权池"
  - 辅助请求首绑使用 `requires_oauth: true`（`provider/native.rs:158`），模型与辅助请求规则不一致
- **影响**：混合池中新会话随机失败，客户端看到无可用账号而非真实原因；若历史或笔记请求先到则会选中 OAuth 账号，结果随到达顺序变化
- **最小修复方向**：在一处限定会话池只取 OAuth 账号——Core 在 `restrict_codex_access` 中收窄范围，或 Provider 在 `context.codex_session_pool()` 为真时传 `requires_oauth: true`，二选一，不要两层都做；补一条混合池负例测试

## 三、可选建议

1. **turn metadata 的正文与请求上下文未分别计入冲突判定**
   - `gateway-protocol/src/openai/codex.rs:38-47` 使用 `or_else`，正文有值时不再看上下文
   - 已核实：HTTP 在中间件后复验时单独加入请求头值，冲突最终仍返回 400；WS 上下文值取自帧正文，不会冲突，因此不会发往错误账号
   - 但认领发生在选号阶段，早于中间件后的冲突复验，最终被 400 拒绝的请求可能已写入一条归属
   - 建议协议层按文档把两个来源分别加入去重
2. **辅助请求首绑遇到 `IdentityChanged` 直接失败，不重新选号**
   - `provider/native.rs:180-186` 返回 `Unavailable`；模型路径 `selector.rs:1072` 会重试快照，`docs/architecture.md` 也写"版本已推进则丢弃该快照重新选号"
   - 触发：历史或笔记请求先于模型请求到达，且选号与认领之间账号凭据 revision 被推进（如 RT 刷新；推进 revision 一点未逐行验证）
   - 建议复用 selector 的认领处理或修正文档；`selector.rs` 与 `native.rs` 各有一套认领结果分支且处理不一致，可合并
3. **PAT 上游账号每次辅助请求多一次 whoami 往返**
   - `provider/native.rs:268-297` 的 `at-` 分支，`token_client.rs` 无缓存
   - 历史、笔记、usage、身份投影每次都先调一次上游 whoami；JWT 账号读已存储身份投影，两路径不一致
   - 绑定后身份已锁定，可改读已存储的 `upstream_user_id`、`upstream_account_id`
4. **归属表不设 TTL，被引用账号永久不可删除**
   - 属需求明确要求，但运维后果需说明：上游被封账号只要被会话引用过，除非删除整个 Key 否则无法删除，表随会话数无限增长
   - 建议在文档写明，或另行评估受审计的释放手段
5. **会话池下插件嵌套执行不跟随会话 owner**
   - `engine/execution/service.rs:1044`、`:1207` 固定 `codex_session_pool: false`
   - 原生会话池请求内插件发起的嵌套或绑定调用可能落到池内其他账号；若属有意设计，建议文档注明
6. **中间件与插件看到的 endpoint 变化**
   - 原生请求的 `RequestInput.endpoint` 为 `/backend-api/codex/responses` 等，按 endpoint 字符串分支的插件不会命中原生流量
   - 建议在 API 或插件文档注明
7. **文案与注释**
   - `gateway-api/src/openai/router.rs:41` 注释仍写"挂在根级 /auth 与 /backend-api"，`/auth` 现为显式 404
   - `deploy/README.md:402`"分组留空表示全部账号，池内也只有该分组内的账号"前后语义冲突
8. **职责边界（仅观察）**
   - Store 的 `write_codex_binding` 与 `codex_sessions.rs` 含 OAuth、身份、范围判定，并硬编码 `"openai"`、`"oauth"`；为在行锁内保证原子性，`architecture.md` 有记录，可以接受
   - API 层只做解析与认证目的，Core 负责授权、范围与准入，未越界

## 四、上游冲突面（`origin/main` 3 个未发版提交）

- **`7a60bc6d`（按账号隔离出站标识）**
  - 文本冲突：上游删除 `provider/execution.rs` Search 段的 `turn_metadata` 提取与 `RawJsonEndpointRequest.turn_metadata` 字段，二开在同处把亲和计算包成 if/else；保留二开分支结构、删除 `turn_metadata` 即可，会话池段无需让出结构
  - 会话池复验仍成立：隔离只作用于出站副本，复验使用入站或中间件后的原值
  - **语义冲突（合并时必须处理）**：合并后模型请求发往上游的 `session_id`、`thread_id` 与 Search `id` 会被替换为按账号 installation 哈希的值，而 `transport/native.rs` 的历史和笔记请求原样转发正文 `context.session_id` 与 turn metadata 请求头，上游看到的会话标识将不一致，fixed 与 session_pool 均受影响；需让原生辅助请求用执行账号的 installation 做同样隔离，或明确决定豁免
- **`28fa655c`（input turn 作用域）**：同属出站标识隔离，处理同上
- **`a5b767e6`（插件 pin）**：无冲突

## 五、验证缺口

- **测试有效性总体良好**：抽查用例断言的是可观察行为（发往上游的 `chatgpt-account-id`、认领次数、发送前失败、准入释放），未见只证明旧字段消失或复述实现的用例
- **缺少的负例与分支覆盖**
  - 无混合池（含 API Key 账号）用例，对应二、第 1 条
  - `gateway-core/tests/engine/execution/codex.rs` 没有会话池用例：`codex_session_pool` 标志透传、`required_account` 为空、重试时 coordinator 透传均未覆盖
  - `gateway-api/tests` 没有会话池数据面用例：HTTP 缺失或冲突返回 `400 invalid_request`、WS `response.create` 阶段失败、`fixed` 与 `/v1` 在 API 层不受影响，均只在 Provider 层断言 `ProviderErrorKind`
  - Store 认领测试为顺序执行，未覆盖并发插入时 `rows_affected() == 0` 再读回分支（`codex_sessions.rs:79` 之后）
  - 辅助首绑的 `IdentityChanged`、`Unavailable` 分支无用例
- **审核提示已列缺口仍存在**：历史/笔记与推理同 owner 的逐请求证据；后台压缩时同 owner 并发复验；apiKey 认证下历史/笔记链路可达性
  - 对照官方源码 `ext/history-notes/src/backend.rs` 与 `core/src/client.rs`：历史/笔记只在正文 `context.session_id` 带会话标识，不带 `session-id` 头；根代理 Responses 的 `session-id` 头可能是 prompt cache key，与"逻辑来源优先、兜底不冲突"设计吻合
  - turn metadata 的 `session_id` 是否等于 `session_store.level_id()` 未逐行证实
- **前端**：无截图，锁定态、空候选、加载失败的视觉与交互未验证；`useCodexAccountCandidates` 加载失败时静默返回空列表，用户只看到"暂无 OpenAI OAuth 账号"
- **本次实际验证**
  - `0034`、`0035` 的 sha256 与 `.frozen-sha256` 一致；迁移目录 diff 只有新增这两个文件与追加两行 checksum
  - 未运行测试，门禁结论沿用审核提示第 7 节 2026-10-22 记录

## 六、实际覆盖范围

**读过全文**

- `gateway-api/src/openai/codex.rs`
- `gateway-core/src/engine/codex.rs`
- `gateway-core/src/engine/execution/codex.rs`
- `gateway-core/src/provider_ports/session_owner.rs`
- `providers/openai/src/provider/native.rs`
- `providers/openai/src/transport/native.rs`
- `gateway-store/src/postgres/codex_sessions.rs`
- `gateway-store/src/postgres/provider_accounts/identity.rs`
- `gateway-protocol/src/openai/codex.rs`
- 迁移 `0034`、`0035`

**读过 diff**

- gateway-api 其余源码
- gateway-core 其余源码：contract、service、coordinator、policy、settings、snapshot、model
- providers/openai：`provider/{mod,execution,failure}.rs`、`request_identity.rs`、`credential/{affinity,selector,types,admin,oauth,mod}.rs`、`admin.rs`、`lib.rs`
- gateway-store：`client_keys.rs`、`snapshot.rs`、`bundle.rs`、`provider_accounts/{repository,core_adapter,admin_adapter}.rs`
- 前端 keys 与 settings 全部改动
- `docs/api.md`、`docs/architecture.md`、`deploy/README.md`
- 上游 `7a60bc6d` 的 `execution.rs` 与 `correlation.rs`

**测试**：列出 6 个关键测试文件全部用例名；阅读协议层会话判定用例全文、`session_binding.rs` 前两个池用例、Store 认领用例主体

**未覆盖**

- `providers/xai/src/admin.rs`
- `gateway-plugin` 的 callback 与 sdk
- `gateway-admin` 的 model 与 use_case
- `runtime_settings` 存储层
- `docs/reports/` 研究归档
- 其余约 30 个测试文件正文
- WS 完整状态机与 coordinator 完整重试路径
- `.runtime/codex-native/` 参考材料
