# Codex 原生状态连续性：个人用途验收报告

## 结论与范围

**个人单实例核心链路通过，未部署，不代表全仓测试全绿**

代码以 `36c87b8201b14d7c3cb11ae0d0cec1a2cf44c520` 为变更基线，具体方案见[架构说明](../codex-native-state-continuity-plan.md)。实现只改 CPRS，不改官方客户端：history 由 PostgreSQL 保存并回答官方四工具，notes 保留上游实现，保存原始加密写操作并在换号前按序重放到新域

本报告覆盖同一逻辑会话分段恢复后补齐的真实验证，不是一次无中断的新运行，也不包括多宿主、灾备或 B→A 回切

真实验收对象是 `54fd955e68d818b1ddfdf48279391b2252411f8a` 的代码。其后的独立审查修复改变了 history 窗口解析与 notes 交付生命周期，
本报告的运行结果不自动适用于修复后的代码，见[审查后续修复](codex-native-state-continuity-followup-2026-10-25.md)

## 真实环境与方法

- 官方 CLI：`0.164.0-alpha.11.1`；模型：`gpt-6.1-sol`，effort `low`
- 固定两个上游账号，以 A/B 表示；保持各自原出口，不换第三账号或模型
- 使用隔离 CPRS、PostgreSQL、Redis、Client Key 和 `CODEX_HOME`；生产服务和数据库不改动
- A→B 由模型请求的 quota 故障注入触发 CPRS 自身选号与迁移；不是实际耗尽账号余额，也没有手工改 owner
- 恢复提示词只给文件或历史标记、公开计算规则与新 Q，不预填旧系数或期望答案；notes 恢复先于 history 检索
- 判据组合实际工具调用、窗口与账号事实、读回计算结果及本任务 PostgreSQL 元数据复算，不单凭 HTTP 200 或模型声称成功

## 逐段结果

| 阶段 | 结果 | 运行证据 |
| --- | --- | --- |
| A 建立状态 | 通过 | notes write/append/read 成功；早期 history 使用独立合成事实 |
| CPRS A→B 迁移 | 通过 | 注入额度拒绝后网关自行选择 B；B 接收原写操作重放；owner 从 A 变为 B，generation 从 0 变为 1，notes 新域已发布 |
| B 换窗口恢复原 notes | 通过 | 官方 compact 完成后生成新窗口，B 调用 notes search/read，未借 history 取答案，读回计算正确 |
| B 维护原 notes | 通过 | 对迁移文件新增一次 append 并读回；中断后先读确认，未重复追加；独立诊断文件不作为迁移文件的替代 |
| 重启网关后恢复 | 通过 | 只重启隔离网关，relay、PostgreSQL 数据和逻辑 thread 保留；再次换窗后读到追加内容并计算正确 |
| history 恢复 A 事实 | 通过 | 再次换窗，真实调用 list_windows、list_items、search_contents、read_item；未借 notes 取答案，计算结果与 A 早期事实一致 |

同一 thread 的窗口从 `:0` 推进到 `:9`。维护与维护后读回可以处于同一窗口；不能将窗口数量当成多账号所有时期均已检索的证明

### 装置故障与纠正

- compact 的旧 `turn/completed` 曾被误当作下一轮结束，导致提前清理；装置改为使用 `turn/start` 返回的 ID，并按 thread/turn 关联完成事件，五项离线反例通过后继续真实验证
- 恢复装置曾从同一隔离库另一条尝试取 seed，提示词实际读取了未写入的路径；限定当前 session 并与原写操作路径交叉核对后，同一原笔记恢复成功，撤回“产品 notes 重放不可读”的推断
- 读取错误路径也可能返回 HTTP 200 和加密输出，因此状态码、包装形状或长度不能单独证明命中
- worker 执行服务曾因余额不足中断，充值后按同协议恢复；这是执行服务故障，不是 A/B 测试账号额度不足
- 续测中两次上游瞬时限流与 quota 注入分开记账，没有通过切换账号、模型或放宽业务断言绕过

## 本地验证

以下为对应产品源码的既有运行证据；提交前复核时产品代码与验证时的文件摘要一致，不把未重跑的检查写成本轮新执行

| 检查 | 结果 |
| --- | --- |
| Rust 编译、workspace Clippy（all-targets/all-features，`-D warnings`）、fmt | 通过 |
| gateway-core | 624 项通过 |
| gateway-protocol | 107 项通过 |
| gateway-api / gateway-store / gateway 二进制组合 | 1090 项通过 |
| provider-openai | 1056 项通过，6 项失败，见下方边界 |
| Provider native notes 定向 | 13 项通过，包含 HTTP/WS quota 后完整重发与迁移 |
| 组合根架构检查 | 14 项通过，未放宽 allowlist |
| 真实 PostgreSQL 18 / Redis 隔离用例 | 46 项通过，包含状态、Key 级联与会话状态读写 |
| 一次性迁移演练 | schema 33→40、35→40 通过，隔离数据保留与读写有证据 |

Rust 验证使用 `cargo +1.99.1`、`backend/Cargo.toml`，较大测试需要 `RUST_MIN_STACK=16777216`。数据库用例须指向专用隔离 PostgreSQL/Redis；没有服务时跳过不算通过。一次性迁移演练脚本不作为维护测试提交

### 提交前补查

在产品代码未再变化的前提下补跑以下检查，均通过：

```bash
export PATH="$HOME/.cargo/bin:/opt/homebrew/bin:$PATH"
cargo +1.99.1 fmt --manifest-path backend/Cargo.toml --all -- --check
RUST_MIN_STACK=16777216 cargo +1.99.1 test --manifest-path backend/Cargo.toml --locked -p codex-proxy-rs --test main architecture::
cd frontend
./node_modules/.bin/vue-tsc -b --pretty false
./node_modules/.bin/eslint src/api/modules/keys.ts src/views/keys/components/ApiKeyFormModal.vue src/views/keys/composables/useApiKeyEditor.ts src/views/keys/index.vue
```

架构检查为 14 项通过；链接器保留 macOS `__eh_frame` 过大警告，非用例失败。初次命令因 PATH 缺少 cargo/pnpm 未启动，随后使用已安装工具执行上述命令，没有安装或更换依赖；类型和 lint 检查不能替代 UI 视觉验收

## 账目与证据

取消固定次数上限后的续测沿原账累计，新增 50 次模型、91 次辅助预留：模型 48 条 200、2 条上游 429；辅助 90 条 200、1 条无终态回执。本阶段本地 quota 注入为 0，迁移注入发生在此前 A→B 阶段

无终态项是收尾复核启动隔离网关时的 models 预取，结果未知，不退款、不算成功。当前可靠账本累计为 86/163；更早一批无法重建的调用总量单列，不拼成整个任务的精确总消费

本地证据保存在 `.runtime/codex-native/personal-state-implementation/`，不随 Git 提交：

- `validation/local-report.md`、`validation/command-results.json`、`validation/change-manifest.json`
- `personal-acceptance-run/parent-continued-acceptance.md`
- `personal-acceptance-run/continue/{chain-verify,fixture-evidence,parent-accounting}.json`

仓库外的 reviewer 未拿到这些本地产物时，应将本节视为已报告的运行结果，而不是其本人独立复验。需要补证时只分享脱敏摘要，不索取真实令牌、旧用户会话、原始 SSE 或密文转储

## 清理与未覆盖项

- A/B 模拟注入已撤销并确认，测试客户端和隔离服务停止，数据与失败记录保留；生产未部署
- 6 项 Provider 失败包括 macOS TLS ClientHello 差异及 WebSocket paused-time/socket 就绪竞态；已有基线对照或隔离 flaky 记录，未删除用例、放宽 TLS 或延长门禁来涂绿。不能称这些用例通过
- 前端随提交保留 Key 模式合同收尾修复；既有截图未完成视觉审阅，不声称 UI 已验收
- 未实际覆盖 B→A 回切、B 阶段历史的独立恢复、认证失效/撤权等其他换号原因、多宿主和灾备；不声明机器解密或所有 opaque 上下文来源的形式证明
- 测试通过不替代独立代码审查，也不意味着维护者批准部署
