# CPRS 原生 history/notes 连续性架构

> 状态：已实现并通过个人单实例核心链路验收；尚未部署，不代表全仓测试全绿
> 目标：[跨账号 history/notes 连续性](codex-native-state-continuity.md)
> 范围：个人自用、单实例 CPRS；不发布、不做多宿主与灾备
> 实测客户端：Codex `0.164.0-alpha.11.1`；官方参考源码：`6905ecede7816e91373e9eabda3f6b0368fc747b`，未证明与安装的 alpha 二进制精确对应

本文说明当前实现的架构与用法。运行证据及未覆盖项见[验收报告](reports/codex-native-state-continuity-acceptance.md)，
个人场景验收不代替发布或部署审批

## 1. 目标与范围

同一客户端逻辑会话在 A 推理额度耗尽后改用授权池中的 B，仍能读取和维护原 notes，并检索 A、B 各阶段的 history；
换窗口、压缩和网关重启后仍然成立。

落在源码上的两条主线：

1. **history 由 CPRS 持久保存并服务查询**：托管会话的四个官方 history 工具由本地 PostgreSQL 事实回答，
   history 检索的查询词由网关出站适配成明文声明，客户端不需要定制
2. **notes 保留上游实现，CPRS 只保存原始加密操作并按序重放**：write/append 的原始请求体、每个会话内 seq 与
   观察到的响应落库；切号时在新域按 seq 重放，发布后读写才指向新副本

PostgreSQL 是会话归属、history、notes 操作与副本进度的唯一持久权威。Redis 仍只负责既有准入、租约、冷却与
可丢失提示。不新建文件服务、搜索集群、事件总线、插件，也不改官方客户端

### 1.1 覆盖边界

- 原生 `/backend-api/codex/*` 的 `session_pool` 会话；`fixed` 与普通 `/v1` 完全不进入本地状态存储
- 入口身份查询与账号级只读（`whoami`、`accounts/check`、`usage`、`profiles/me`、`settings/user`、
  `workspace-messages`、`config/bundle`）仍使用入口账号，不参与会话状态与切号
- 不保证接入前已经存在于官方云端的旧状态自动搬家；这类会话按 legacy 处理
- 不保证跨 Client Key 无授权共享 history；session_id 本身不是权限凭据
- 不做多宿主接管、备份/PITR 与远端副本的一致性恢复，也不做通用发送权框架（见 §9）

### 1.2 不提供的机制

个人单实例范围下不提供以下机制：

- 多宿主接管、每步发送权代次、在途登记排空与受阻出口：单实例由同一会话互斥 + PostgreSQL 事务 CAS 承担
- 旧快照/PITR 恢复远端 notes 副本的一致性编排：只记录“恢复旧库后远端副本可能超前”这一事实
- 通用重试/补偿/自动垃圾清理：失败如实返回，未终结操作阻止后续自动迁移
- 本地 notes 实现与正文解密：不解密、不伪造密文、不把 read_file 输出当 write 参数

## 2. 依据

关键实测（直接 API，非官方客户端验收；实验过程记录不随仓库提交）：

- A 真实生成的 notes write/append 加密参数，按相同顺序重放到 B 后 B 能读到并继续业务计算；
  迁移前 B 读不到该笔记（负对照）
- A 已消费成功的 notes.read_file 输出密文可被 B 消费，说明 notes 结果由上游按域执行、不需要网关解密
- 可见文本 history 可由本地列表/读取支撑多窗口恢复，阴性查询返回 UNAVAILABLE

官方事实（固定版本源码）：history 的 `search_contents.query` 带 `encrypted` 声明；notes 的 write/append 与两个
search 的查询参数带加密声明；`encrypted_output` 或 `encrypted_function_args` 时按密文传递，普通 JSON 结果有
官方 fallback。协议形状见 `gateway-protocol` 的 history 解析与投影

## 3. 拓扑与职责

```text
官方客户端（同一 Key / 逻辑 session，未改动）
    │ 原生认证、HTTP/SSE/WS、官方工具调用
    ▼
gateway-api 原生入口 → gateway-core 授权与会话状态 → providers/openai
        ├─ history 四工具：本地 PostgreSQL 事实 → 官方结果形状 → 客户端
        ├─ notes 六端点：保存原始加密操作 → 已发布副本账号 → 官方上游端点
        └─ 模型请求：既有 Selector / attempt / continuation → 上游 A 或 B
                 │ 切号时：notes 按 seq 重放到新域并发布，之后才发模型请求
                 └────────→ history 采集（输入在发送前，输出随流）
```

| 模块 | 实际职责 |
| --- | --- |
| `gateway-protocol/src/openai/history.rs` | 官方 history/notes 参数解析、明文投影、条目语义身份、仅对托管会话删除 history 检索的 `encrypted` 声明 |
| `gateway-core/src/engine/codex_history.rs` | 四个 history 工具的本地查询、分页、范围读取与预算 |
| `gateway-core/src/engine/codex_history_capture.rs` | 来源判定、输入快照与输出归档、父响应校验、无 ID 条目的前缀对齐 |
| `gateway-core/src/engine/codex_notes.rs` | 会话内 notes 互斥、seq 分配、冻结目标与按序重放、发布结果判定 |
| `gateway-core/src/provider_ports/codex_state.rs` | 上述状态、条目、操作与副本进度的类型化端口 |
| `gateway-store/src/postgres/codex_state.rs` | 上述事实的 PostgreSQL 实现：seq 分配、条目与引用、发布 CAS |
| `providers/openai/src/native_history.rs`、`native_notes.rs` | 采集与查询在 Provider 侧的接线、官方结果编码、notes 请求域映射与凭据选择 |
| `providers/openai/src/provider/{mod,native,execution}.rs` | 模型请求内的换号时机、notes 重建与发布、发布前不建流 |
| `providers/openai/src/credential/selector.rs` | 持久 owner、额度耗尽时解除永久 pin、候选租约与迁移标记 |

`gateway-api` 只保留入口认证与转发，不直接读 Store，也没有第二个 owner

## 4. 启用开关

唯一运行设置是 `openai.codex_state_storage_enabled`（YAML，默认 `false`），经现有 `openai` 配置反序列化链路读取，
不逐请求读环境变量，也不进管理端数据库设置

两个端口齐备时，组合根总是装配同一份 history/notes 服务并把开关作为托管准入传入。开关只决定
“未判定会话能否成为托管”：

| 会话 | 开关关闭（默认） | 开关开启 |
| --- | --- | --- |
| 新会话（官方 new、首窗口、完整首请求） | 首次模型或首次 notes 请求时固定为 legacy：不采集、不记录 notes、不参与切号 | 判定为 managed，开始本地采集与 notes 日志 |
| 已 managed | 继续本地 history、已发布 notes 域与日志，仍可换号 | 同左 |
| legacy | 保持原上游域与原 owner，不自动升级 | 同左 |

首次请求先到的可能是 notes 辅助调用：服务在写日志前先按同一规则确定来源，未判定会话因此不会漏记第一次写入；
开关关闭时它被固定成 legacy，以后打开开关也不会把已漏记的会话冒充完整托管。

`fixed` 与普通 `/v1` 不受开关影响，也不会进入这些服务

## 5. history 采集与查询

### 5.1 采集

- 输入：在隐私规则处理之后、实际发送之前保存本次请求的 `input` 条目与逻辑窗口/agent；落库失败直接不发送
- 输出：随流保存已交付条目的原对象与可读投影；同一窗口内按 ordinal 追加，条目 ID 用官方声明的 ID
- 父响应：客户端续接 `previous_response_id` 时，发送前校验父响应属于本会话与同一窗口；未知或跨窗口直接让客户端
  改发完整输入，不猜前缀
- 无 ID 对齐：条目身份是原对象的完整语义摘要（忽略 `id/status` 与文本分段承载差异，密文、图片 data URL、工具参数
  全部参与），只在同一位置比较，不全局去重、不改写原对象、不解密
- 无父响应的回填：分别与当前窗口最近响应的 input+output 引用、最近一次发送前逻辑输入快照按位置对齐，
  取真实匹配更长的前缀，未对齐部分按本次输入追加；失败 attempt 在响应锚产生前保存的输入也能复用，
  不把整窗口历史当当前上下文，不按正文做全局去重

### 5.2 查询

四个工具全部由本地事实回答，形状按官方合同：

- `list_windows`：真实窗口 ID（`<thread>:<window_number>`）、条目数、agent 范围与 limit
- `list_items`：窗口/role/tool 过滤、稳定 ordinal、按字符预算截断
- `read_item`：窗口 + 条目 ID 精确匹配、字符范围；不可读对象返回 `readable=false` 与附件，不伪造明文
- `search_contents`：对已保存的可读投影做大小写敏感 literal substring 匹配与同一套过滤

托管会话的模型请求在出站时只删除 `history` namespace 中 `search_contents.query.encrypted` 声明，让模型产生网关可
检索的明文词；工具名、描述、其它字段与 notes 的三个加密声明都不动。客户端保持原样

边界：搜索只覆盖可读投影，纯密文对象（reasoning/compaction、加密工具参数与输出）保留原对象但不参与匹配；
负数结果是真实无匹配，不是“没找到就不存在”的推断

## 6. notes 日志与重放

### 6.1 写入

六端点仍由上游执行，CPRS 只做域映射与记录：

1. 写类请求先取同一 `(Key, 逻辑会话)` 的 notes 互斥，再读已发布落点，避免等待期间写回旧 owner
2. 会话内分配单调 seq（从 1 开始，0 表示空日志/尚未重放），保存原始请求体（含加密参数与 `context`）与目标副本
3. 状态只记录真实观察：`unsent`（意图已存、尝试未开始）、`in_flight`（已开始、无终结观察）、
   `responded`（收到完整响应）、`failed`（发送前拒绝或非成功响应）、`unknown`（结果不明）。HTTP 200 不等于业务成功，
   所以不存在以 200 命名的“已应用”状态
4. 读类端点（read/list/search/thread_hint）只读已发布副本，不持互斥，也不接受模型额度门禁；最终凭据必须与所读域
   属于同一账号，不能拿 A 的凭据访问 B 的新域

legacy 会话不建立日志，保持官方域与既有行为

### 6.2 重建与发布

切号目标确定后，在发送模型请求之前完成：

1. 重新解析目标账号凭据并校验凭据版本，取新的域代次（UUID，全局唯一不复用）
2. 冻结目标：连续已判定前缀中最后一个 seq；存在未终结操作时不能冻结跳过它
3. 按 seq 重放状态为 `responded` 的操作（`failed` 不重放但占位），每步完整读取响应后才推进进度
4. 发布：在存储事务内以读取时的代次做 CAS，并复验冻结目标与进度；发布成功后 owner 与新域同时生效，
   可丢失的软亲和再跟到新账号（失败只影响后续选号偏好）
5. 任何重放或发布失败都不发送新账号的模型请求，也不回退到旧账号发送

### 6.3 读与后续写入

发布后六端点都指向新副本域；B 产生的新 write/append 继续进入同一 seq 日志，下一次迁移按同一规则在新域重放。
只重放收到完整响应的正常操作；结果不明或未终结时如实报错，不重发 append，也不造恢复系统

## 7. 换号时序

只有“已 managed 会话的持久 owner 被确切判定额度耗尽”才迁移：

1. 模型请求准备阶段读取会话来源；仅 managed 且装配了 notes 能力的会话向选择器声明可迁移
2. 选择器读到持久 owner 后，若该 owner 在本次候选里的账号状态确为 `QuotaExhausted`，且请求不是持续写/续接、
   没有显式固定账号，则本次解除永久 pin：把 owner 排除后按既有规则（账号范围、模型权限、OAuth 资格、健康度、
   租约、排队）选候选。这一步不 claim、不改写 owner、不推进软亲和
3. 候选租约取得并完成会话、作用域与凭据复验后，若请求仍带 `previous_response_id`，直接让客户端改发完整输入，
   不把 A 的增量交给 B
4. 否则先按 §6.2 重建并发布 notes，再建立 history 采集与上游流；模型字节在发布之后才出站
5. 后续请求以 PostgreSQL owner 为准钉住 B；软亲和若仍指向 A，会在下一次选号的 CAS 中自愈

不迁移的情形：legacy 与未判定会话、`fixed`、普通 `/v1`、excluded/并发忙/临时限流/认证失效/停用、无可用候选。
这些按真实原因失败，不做透明换号。上游额度拒绝的恢复资格沿用既有判据（`QuotaExhausted`、`replay_is_safe`、
`BeforeSemanticOutput`、非 `Ambiguous`）与既有 attempt/rotation 预算，不新增调度器

## 8. 入口路由

| 入口 | 归属 | 状态行为 |
| --- | --- | --- |
| Responses HTTP/SSE、WS `/backend-api/codex/responses` | session_pool 会话 owner | 采集 history；切号时先重建发布 notes 再发送 |
| 原生 standalone search `/backend-api/codex/alpha/search` | 跟随已发布 owner | 不写 notes、不触发迁移、不推进重放进度 |
| history 四端点 | 会话 owner | managed 会话本地回答；legacy/未判定照旧上游 |
| notes 六端点 | 会话 owner + 已发布副本 | 写类记录 seq 并发送到副本域；读类只读已发布副本 |
| 身份投影与账号级只读 | 入口账号 | 无会话状态读写 |

## 9. 存储、安全与运维

- 本地托管意味着 PostgreSQL 会保存会话业务内容（含不透明原对象）。普通日志、观测、管理列表不输出正文
- Key 级归属贯穿所有查询、写入与索引；删除 Key 即失去访问，迟到写入不能复活已删除状态
- 前向迁移为 `0036`–`0040`，不改既有冻结迁移；`0039` 保存 history 条目语义身份，`0040` 保存每窗口最近的发送前逻辑输入快照
  部署前需在隔离数据库执行迁移并验证读写
- 数据随 PostgreSQL 备份恢复。恢复旧库不会把远端 notes 副本一起回滚，此时按新的重建与发布流程继续，
  旧代次不自动复用；这是单实例个人用法的已知边界，不做自动编排
- 多宿主并发假设不成立：同一会话由进程内互斥与数据库 CAS 串行，不做接管

## 10. 验证范围

个人用途验收覆盖：A 建立状态、CPRS 自动迁移到 B、B 换窗读取原 notes、一次追加并读回、仅网关重启后新窗口读取，以及通过 history 四工具恢复 A 的早期事实

该结论限于固定官方客户端、模型与两个账号下的单实例合成场景。B→A 回切、多宿主、灾备和旧云端归档导入不在该验收范围内；已有平台相关测试失败与 UI 视觉验证缺口单列，不据核心链路通过推断全仓全绿

具体命令、结果、隔离方式与失败边界见[验收报告](reports/codex-native-state-continuity-acceptance.md)，能力边界见
[连续性目标 §当前实现与剩余边界](codex-native-state-continuity.md#当前实现与剩余边界)
