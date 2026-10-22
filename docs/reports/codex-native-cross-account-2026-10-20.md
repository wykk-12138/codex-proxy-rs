# Codex 原生接入：跨账号状态、重放与缓存研究报告

- 实验日期：2026-10-20
- CPRS 实现基线：`bbeae800e56a84d590819a39f9a31afbe33ba5c2`
- 官方客户端：`codex-cli 0.163.0-alpha.10.2`
- 官方源码：[openai/codex@4ebc485ca321243727539187169425fcf972f2c4](https://github.com/openai/codex/tree/4ebc485ca321243727539187169425fcf972f2c4)
- 实验模型：`gpt-6.1-sol`
- 文档性质：有版本和样本范围的研究归档，不是账号池功能的使用说明或验收承诺

## 1. 结论与适用范围

本报告分开记录四类事实：已取得密文的模型可用性、官方存储访问、客户端有效上下文重放、缓存用量

| 问题 | 实测结果 | 不能推出的结论 |
| --- | --- | --- |
| A 的 notes 工具密文能否给 B 模型使用 | 同一份 `encrypted_output` 原样交给 A、B，两者均恢复正确随机码 | 所有加密对象、账号组合和未来版本都能跨号 |
| B 能否凭相同 session_id 直接取 A 的官方 notes/history | A 恢复对照成功，B 未恢复；notes 文件不存在，history 无旧内容 | 官方内部具体加密方式、所有存储命名空间或全量迁移一定不可行 |
| 官方客户端能否向 B 重放当前有效上下文并继续 | 成功；B 收到 13 个 input 项，没有 previous_response_id，恢复当前窗口标记 | 运行中 WS 已实现自动换号 |
| 重放内容能否在 B 后续新窗口通过 history 恢复 | 本次成功，替换上下文没有随机码，实际工具恢复出当前窗口标记 | 更早归档及 notes 已全部迁移 |
| 换号是否必然零缓存 | 否；独立对照和真实客户端重放均出现大量缓存命中 | 账号之间必然共享缓存、重复请求必然命中、输入免费 |

[当前原生入口](../api.md#原生-codex-入口)支持 `fixed` 与 `session_pool` 两种模式，每个逻辑会话的归属固定在对应的会话 owner；
跨账号迁移与归档搬家仍未实现，上述实验不改变这些边界

## 2. 官方源码核对

以下结论仅针对上方固定提交。引用的是客户端可见合同，不能据此推断未公开的服务端实现

### 2.1 PAT 身份、扩展启用与工作区一致性

- [`login/src/auth/personal_access_token.rs:39–75`](https://github.com/openai/codex/blob/4ebc485ca321243727539187169425fcf972f2c4/codex-rs/login/src/auth/personal_access_token.rs#L39-L75)：PAT 初始化读取 `CODEX_AUTHAPI_BASE_URL`，请求 whoami 并使用返回的用户、workspace、套餐等元数据；覆盖地址本身不授予上游 OAuth 权限
- [`ext/history-notes/src/extension.rs:45–65`](https://github.com/openai/codex/blob/4ebc485ca321243727539187169425fcf972f2c4/codex-rs/ext/history-notes/src/extension.rs#L45-L65)：history/notes 扩展同时要求启用相应 token_budget 配置、内置 OpenAI provider 和 Codex 后端认证模式；只开放服务端路由不会自动启用工具
- [`login/src/auth/manager/workspace_routing.rs:64–89`](https://github.com/openai/codex/blob/4ebc485ca321243727539187169425fcf972f2c4/codex-rs/login/src/auth/manager/workspace_routing.rs#L64-L89)：工作区发现会检查认证 owner 代次和 account_id 一致性；不能通过每次 whoami 随机换账号来替代稳定接入身份

这些合同支持研究“稳定下游身份与实际上游执行账号解耦”，但不证明合成身份投影已经满足生产权限、套餐及多租户隔离要求

### 2.2 history/notes 请求与工具结果

- [`ext/history-notes/src/backend.rs:37–105`](https://github.com/openai/codex/blob/4ebc485ca321243727539187169425fcf972f2c4/codex-rs/ext/history-notes/src/backend.rs#L37-L105)：客户端注入 `context.session_id`、`current_agent_name`，通过 Provider 构造请求并应用认证；搜索/写入等操作设置加密参数标记，截断策略有独立请求头
- [`ext/history-notes/src/tools.rs:143–237`](https://github.com/openai/codex/blob/4ebc485ca321243727539187169425fcf972f2c4/codex-rs/ext/history-notes/src/tools.rs#L143-L237)：工具参数包含不透明 window/item ID、可选 agent/window 过滤、虚拟文件路径及明确标记的加密 query/text；读取结果不能简单当成写入参数
- [`ext/history-notes/src/tools.rs:332–343`](https://github.com/openai/codex/blob/4ebc485ca321243727539187169425fcf972f2c4/codex-rs/ext/history-notes/src/tools.rs#L332-L343)：顶层字符串 `encrypted_output` 转成工具输出的 `EncryptedContent`，否则返回普通 JSON 文本；这不是把任意多个密文或嵌套密文数组自动合并的合同
- [`protocol/src/protocol.rs:3496–3501`](https://github.com/openai/codex/blob/4ebc485ca321243727539187169425fcf972f2c4/codex-rs/protocol/src/protocol.rs#L3496-L3501)：截断策略序列化使用 `mode`、`limit`，不是 `type`、`limit`

官方工具描述将 history 定义为只读、最终一致的历史查询，将 notes 定义为 rollout 内的虚拟命名空间；这些是客户端工具语义，不足以证明共享上游账号时存在 CPRS Key 级权限隔离

### 2.3 会话元数据、完整输入与 WS 错误

- [`core/src/responses_metadata.rs:330–408`](https://github.com/openai/codex/blob/4ebc485ca321243727539187169425fcf972f2c4/codex-rs/core/src/responses_metadata.rs#L330-L408)：session/thread/window 与结构化 turn metadata 同时投影到 client_metadata 和兼容请求头；命名空间隔离不能只处理其中一个承载位置
- [`core/src/responses_metadata.rs:234–271`](https://github.com/openai/codex/blob/4ebc485ca321243727539187169425fcf972f2c4/codex-rs/core/src/responses_metadata.rs#L234-L271)：结构包含 history_initialization、history_ingest_requested；字段存在不等于公开了全归档导入能力
- [`core/src/client.rs:1007–1036`](https://github.com/openai/codex/blob/4ebc485ca321243727539187169425fcf972f2c4/codex-rs/core/src/client.rs#L1007-L1036)：OpenAI 与非 OpenAI 路径对内部/加密信息的处理不同，客户端请求 reasoning 加密内容；迁移不能只拼用户和助手的可见文本
- [`core/src/client.rs:2085–2157`](https://github.com/openai/codex/blob/4ebc485ca321243727539187169425fcf972f2c4/codex-rs/core/src/client.rs#L2085-L2157)：WS 存在增量与完整请求分支，无法准备 continuation 时使用完整 input，previous_response_id 与增量项来自真实续接状态
- [`codex-api/src/endpoint/responses_websocket.rs:670–686`](https://github.com/openai/codex/blob/4ebc485ca321243727539187169425fcf972f2c4/codex-rs/codex-api/src/endpoint/responses_websocket.rs#L670-L686)：previous_response_not_found 被识别为可重试错误；源码分支存在不等于本报告完成了实时额度耗尽自动恢复验收

CPRS 对应的交付合同见[额度耗尽与续写](../api.md#额度耗尽与续写)。native continuation owner、客户端完整重放和云端归档 owner 必须分别判断

### 2.4 工具定义的最终 wire 形状

[`tools/src/json_schema/types.rs:35–75`](https://github.com/openai/codex/blob/4ebc485ca321243727539187169425fcf972f2c4/codex-rs/tools/src/json_schema/types.rs#L35-L75) 定义实际序列化的 JsonSchema 子集，包含 encrypted，但不包含 minimum。仅复制 history-notes 的 json! 宏字面值，并不等于复制客户端实际发送的 schema

本次模型服务实际拒绝了不匹配的保留工具定义；最终使用完整工具定义及官方 JsonSchema 序列化投影后，模型工具调用闭环才成立。该前置失败不属于产品行为证据

## 3. 实验方法与控制变量

### 3.1 共用边界

- 使用两个已授权的 OpenAI OAuth 账号，标记为 A、B；两者真实 user_id、workspace 均不同，不在报告中保留其邮箱或实际标识
- 每次使用全新合成会话、窗口、笔记名和不可猜测的随机码，不读取用户旧认证或原始会话
- 沿各账号当前出口发送：A 使用配置的 SOCKS 出口，B 使用其现行直连出口；未通过切换出口掩盖失败
- AT 只在远端工作进程内存使用，不读取 RT、不复制完整凭据、不启动并行刷新宿主
- 不修改生产 Key、绑定、账号或配置，不部署、迁移或重启生产
- 不自行解密工具内容；将真实工具密文交回模型，再校验模型实际输出与合成码是否匹配
- HTTP 200 及 encrypted_output 存在均不足以证明读取成功；同账号正对照和内容恢复才构成判据

### 3.2 实验 A：官方存储访问

1. A 模型实际生成 notes.write_file 的加密参数，发送到官方端点，并调用 notes.read_file
2. note 标记写入笔记，另一个 history 标记只出现在模型对话中
3. A 用同 session 的新窗口恢复，输入不带原码、旧 transcript 或工具结果，notes 与 history 两项均恢复成功
4. B 用自己的凭据、workspace 和相同 session_id 执行相同读取
5. B 消费工具结果后报告 notes 文件不存在，history 无旧窗口或旧内容，两个标记均未恢复

这是本轮样本的存储访问结果，不是账号级加密算法或全部官方权限模型的逆向结论

### 3.3 实验 B：已取得密文的可移植性

1. 另建会话，由 A 模型生成真实加密写入参数并写入笔记
2. A 模型生成真实读取调用，A 后端返回 encrypted_output
3. 使用这一次实际工具调用和密文构造后续模型输入，不带原写入提示或随机码明文
4. 将同一份密文原样分别交给 A、B 的模型
5. 两者均 response.completed，实际输出均精确匹配合成码

结论针对该 notes 工具输出，不覆盖其他加密项，也不说明 B 可以自行从 A 的存储取出它

### 3.4 实验 C：官方客户端重放与窗口恢复

- 使用官方精确版本、全新独立 CODEX_HOME，以及仅监听本机的临时 HTTPS 入口
- 临时 CA 只经 CODEX_CA_CERTIFICATE 注入测试子进程，系统信任和用户默认配置不变
- 下游使用稳定的合成测试身份；上游使用实际执行账号的 AT、workspace 和安装身份
- 入口只提供 HTTP/SSE，WS upgrade 返回 426，由客户端自身回退；上游响应被实验工具缓冲，因此不将客户端端到端时延当成正常流式首 token 延迟
- A 第一窗口写入 notes 和独立的归档标记，随后真实 new_context，进入新的有效上下文
- 切换的是测试入口的上游执行账号以及本轮 CLI 进程，不是 CPRS 服务或用户登录配置
- B 的 exec resume 发出 13 个 input 项、无 previous_response_id，正确恢复当前窗口标记
- B 再调用 new_context，替换上下文不含三个标记，通过真实 history 工具恢复刚才重放的当前窗口内容
- 同次恢复未找回第一窗口归档标记；notes.read_file 的结果为 Note file not found

**实验偏差：** A 在重置后额外调用 notes.read_file、history.list_items，未遵循“不读取旧内容”的提示。这些工具结果可能携带旧内容密文并随有效上下文重放给 B。因此本实验不构成严格的全归档隔离对照，旧归档结果只描述本次恢复流程，不证明所有迁移方式都失败

## 4. 实测用量

### 4.1 相同业务输入的四请求对照

相同模型、instructions、合成表格、prompt_cache_key 和逻辑 session，依次发送 A1、A2、B1、B2。各次使用实际账号认证和安装身份，没有为了命中而扩大权限

| 请求 | input_tokens | cached_tokens | cache_write_tokens | output_tokens |
| --- | ---: | ---: | ---: | ---: |
| A1 | 8692 | 0 | 0 | 7 |
| A2 | 8692 | 8576 | 0 | 7 |
| B1 | 8692 | 8576 | 0 | 7 |
| B2 | 8692 | 0 | 0 | 7 |

四次均正常完成并输出指定短文本。B1 的缓存比例约 98.7%，B2 则为零；没有追踪具体机器或每个缓存块来源，不推断跨账号缓存必然共享，也不重复请求以追求更好结果

### 4.2 实际客户端 A→B 重放

| input 项数 | previous_response_id | input_tokens | cached_tokens | cache_write_tokens | output_tokens |
| ---: | --- | ---: | ---: | ---: | ---: |
| 13 | 无 | 14243 | 13952 | 0 | 27 |

本次约 98.0% 输入报告为 cached。以上是上游 usage，不等同于 OAuth 订阅额度、API 账单或绕过 CPRS 执行链时的 CPRS 实际扣费

## 5. 未覆盖与不能替代的验收

- 真正上游 WS 上的额度拒绝、previous_response_not_found 和运行中自动完整重放
- 所有 reasoning/compaction 对象、附件、反向账号组合或其他模型
- 把全部云端旧窗口下载并导入新账号
- notes 手动转存、原状态账号永久不可访问后的恢复保证
- 双 Key 共享上游账号的权限隔离
- 池化实现的数据库事务、调度、热更新、租约或界面行为
- 缓存命中概率、全局隔离规则或压力/性能基准

Key 级会话归属固定在会话 owner 是[当前 CPRS 产品合同](../architecture.md#原生-codex-访问)，不能写成由工具密文必然不可跨账号推出的官方要求。客户端可以重放当前上下文，也不能据此宣称官方云端历史和 notes 已经迁移

## 6. 回执、前置失败与清理

本机原始脱敏回执位于原仓库的 `.runtime/codex-native/`，不随本报告提交：

| 相对目录/文件 | 用途 |
| --- | --- |
| `cross-account-20261020/receipt-official-wire.jsonl` | 同账号正对照与 B 的直接存储读取 |
| `cross-account-20261020/receipt-cipher-portability.jsonl` | 同一工具密文的 A/B 模型恢复 |
| `switch-replay-20261020/results.json` | 最终有效客户端阶段、窗口检查及清理 |
| `switch-replay-20261020/request-traces.json` | 阶段/账号/路径/状态、输入项数量与哈希、标记布尔值及 usage |
| `switch-replay-20261020/cache-results.json` | 四请求缓存实测 |

这些回执不含 AT、RT、代理认证或用户旧 transcript。本报告提供可随仓库保留的数据摘要和源码链接，但不能代替外部审查者独立复现；原始合成 rollout 已按清理约定删除，涉及上下文重置的结论依赖当时提取的回执

以下前置失败不计为产品通过或失败：SOCKS 客户端依赖缺失、IPv6 出口不可达、手写工具 schema/截断字段错误、本机入口缺少 HTTPS、转发器重复 Content-Type、exec resume 不支持 --color。它们的过程记录保留在本机报告中，不通过修改产品检查或用户测试目标掩盖

实验结束时：

- 生产 healthz 204，版本 3.29.5+codex-native.1，最高迁移 34
- 最终客户端实验前后配置 revision 均为 92；更早直接端点实验没有前置 revision 快照，不声称其前后比较成立
- 临时 CLI、HTTPS 入口、SSH 工作进程已退出，临时认证、证书私钥和 CODEX_HOME 已删除
- 合成笔记留在官方测试空间；已知路由白名单没有删除端点，未尝试扩大权限清理
- 上述运行数据是实验结束快照，不是当前线上实时状态

## 7. 可支持的后续决策

- 推理账号池可以优先复用客户端完整有效上下文重放，不应把永久锁定 Key 的推理账号当作唯一方案
- 下游访问身份、实际推理账号、原生 continuation owner、官方状态归属必须分开设计
- 归档和 notes 需要明确的迁移或路由合同；未关闭前不宣称完整池模式完成
- 多租户隔离必须有实际授权和命名空间验证，不能依赖随机 session_id 难猜
- 缓存和费用以实际 usage 记录，允许冷输入，不以缓存推断权限或恢复安全性
- 方案审查应优先关闭 WS、严格归档/notes 对照和双 Key 隔离，不能重复已完成的缓存对照来替代这些问题

## 8. 三方参考：Krill 的客户端配置

### 8.1 来源与边界

来源：[Krill「Openai 官渠 / Codex」教程](https://www.krill-code.com/app/tutorial?channel=Openai+%E5%AE%98%E6%B8%A0&client=codex)，2026-10-20 通过用户已打开的 Chrome 页面实际读取

Krill 是第三方中转站，不是 CPRS 旧 `/krill` 路由或 `wykk.codex-bridge` 插件。后两者是本项目自己的桥接实现，不能把它们的内部设计归到 Krill 服务上

以下保存的是当时教程展示的客户端配置，密钥仅使用占位符。没有用 Krill Key 完成连接验收，也没有获得其服务端源码；页面今后可能变化

### 8.2 教程配置快照

教程将配置放在 `~/.codex/config.toml`，使用内置 OpenAI provider：

```toml
model = "gpt-6-astra"
model_provider = "openai"
openai_base_url = "https://api.krill-code.net/backend-api/codex"
chatgpt_base_url = "https://api.krill-code.net/backend-api/"
web_search = "live"
suppress_unstable_features_warning = true

[features]
fast_mode = true
context_management = true

[features.token_budget]
enabled = true
use_history_notes_extension = true

[analytics]
enabled = false
```

教程在 `~/.codex/auth.json` 中将同一个 Krill API Key 同时填入两个字段，并要求不添加 `auth_mode` 等字段：

```json
{
  "personal_access_token": "<Krill API Key>",
  "OPENAI_API_KEY": "<Krill API Key>"
}
```

另设置以下环境变量，教程要求重开终端，桌面版需退出并重启：

```text
CODEX_AUTHAPI_BASE_URL=https://api.krill-code.net
```

教程特别提示：桌面版不要通过「Sign in another way」重新登录，否则认证文件会改回普通 API Key 模式，所述上下文管理和速度档将失效。这是教程的操作说明，不是本报告对所有客户端版本的实测结论

### 8.3 教程说明的能力

- `openai_base_url` 用于模型请求，`chatgpt_base_url` 用于启动时的账号校验，教程提到 Codex 0.160 及以上版本和 ChatGPT 桌面版
- 模型列表从服务端自动同步，新模型可出现在 `/model`，不要求手工填写目录
- 上述 context_management、token_budget 配置用于开启上下文管理
- `/fast`、`/ultrafast` 切换速度档；默认档位分别可用顶层 `service_tier = "priority"`、`service_tier = "ultrafast"`，教程说明倍率更高且 Ultrafast 仅在支持的模型上出现
- 页面可以选择不同线路，教程中的地址会随线路替换；本节快照使用 `api.krill-code.net`，不是其全部线路清单

这些是 Krill 对外教程提供的配置和能力说明，不能据此证明其实际账号调度、历史存储、加密、租户隔离、故障恢复或计费实现

### 8.4 与 CPRS 接入的区别

| 项目 | Krill 教程快照 | 当前 CPRS 原生接入 |
| --- | --- | --- |
| Provider | 内置 `openai` | 内置 `openai` |
| 模型请求 | `/backend-api/codex` | `/backend-api/codex` |
| 后台请求 | `/backend-api/` | `/backend-api` |
| 认证服务基址 | 域名根地址 | 域名根地址 |
| auth.json | 同一个 Krill Key 同时填 PAT 和 OPENAI_API_KEY | 已验证的最小配置只需 PAT 字段，值为 CPRS Client Key |
| 功能开关 | 教程包含 fast_mode、context_management 等 | 以 CPRS 文档及对应官方版本为准，不能因 Krill 写了某字段就认定 CPRS 必须添加 |

可借鉴的是使用内置 provider，同时覆盖认证、模型和后台入口的客户端接入方式，不是照抄域名、路径或推断其内部架构。CPRS 的实际配置以[部署与运维中的原生接入](../../deploy/README.md#原生-codex-接入)为准；实验或试用使用独立 CODEX_HOME，本节不是修改用户默认配置的操作授权
