# Effect 实现审查

日期：2026-09-30。基准：仓库实际依赖的 `effect@4.0.0-rc.117`，不机械套用 v3 的 API 写法。

本次从 CLI 入口追踪 application/services、Actor runtime、Context workflow、Goal/Signal/task/approval、Lark IM/mail、Agent adapters、memory 与本地持久化。目标是判断依赖、错误、取消、资源和状态所有权是否一致。仅审查，未修改业务源码。

## 修复记录

2026-09-30：下列 7 项已修复，原审查内容保留为问题依据。新增回归测试覆盖定时快照增长、Goal 存储故障恢复、过期评估回调、Run 与存活 Delegation 的重新连接、Codex 取消、邮件错误通道，以及 history 并发持久化和异步分页。

history 服务现返回 Effect；文件格式不变，按 Goal 串行执行异步 I/O，并在首次读取时重建偏移索引。没有迁移或删除用户数据。“后续设计改进”中的全域服务 Effect 化、System One 取消与 IM admission queue 重构不属于这 7 项修复，仍作为后续工作。

## Findings

### 1. [P1] 定时 Signal 的 source 快照递归包含全部历史

位置：`packages/core/src/signals/actors.ts:700`、`:713`。

每次 Tick 把 `registry.get(path)` 整体放进新 occurrence 的 `source`。该 record 的 state 已包含旧 occurrences，而每个旧 occurrence 又带有之前的 state。由此形成无环但递归嵌套、近乎每轮翻倍的数据，而不是正常的线性历史增长。

本地真实 Actor + TestClock 连续触发 10 次，序列化记录大小依次为：

```text
921, 2091, 4431, 9111, 18471, 37191, 74631, 149511, 299271, 598791 bytes
```

这会同时放大持久化、structuredClone、Context 通知、HTTP snapshot 和推理输入；长期运行可导致进程严重卡顿或内存耗尽。不能仅靠给外层 messages 加上下文限制解决。

建议：source 只保留明确挑选的 Signal 定义、触发时刻和必要证据，不包含 occurrences 等运行历史；完整事件写入独立 journal，工作状态保留有限的投递信息。

### 2. [P1] Goal 的恢复流程与 Actor 保留子节点的重启语义冲突

位置：`packages/core/src/goals/actors.ts:231`；runtime 契约见 `packages/actor/src/system.ts:419`、`:431`。

Actor 重启关闭本次 behavior 的 Scope，但保留子 Actor。Goal 的 started 却对已有 runs 无条件 spawn。同一个 Goal 已有 Run 时，只需一次临时存储失败，恢复就会遇到同名子节点，继而耗尽重启次数并停止整个 Goal。

本地注入一次 save 失败，实际事件为：一次原始错误、反复 `Actor already exists`、最后 `ActorStopped`。这不是持续磁盘故障造成的。

建议：恢复时先 child 查找并复用存活节点，再恢复订阅；只有节点不存在才 spawn。统一检查 Goal/Signal root 和 SignalRun 的同类恢复入口。不要通过一律杀掉子节点规避问题，否则可能破坏已经启动的外部任务。

### 3. [P1] 已取消评估的旧回调仍能修改重启后的 Goal

位置：`packages/core/src/goals/actors.ts:560`、`:583`、`:594`；接收端 `:292`。

Evaluate 外层 tryPromise 提供了 AbortSignal，但 Compacted、Tool、Transcript 回调用独立 `Effect.runPromise(self.ask(...))`，没有把 signal 传进去。重启复用 ActorRef，而消息不携带评估轮次，接收端不能区分旧评估和当前评估。

本地 mock reasoner 在 Goal 重启、第二轮 plan 开始后调用第一轮的 tool 回调，得到：

```json
{ "oldSignalAborted": true, "plans": 2, "taskCreated": true }
```

该 mock 验证的是取消后迟到回调的防护边界，不代表真实模型每次取消都会发起迟到回调。当前边界确实允许旧操作污染新实例。

建议：内部工作流保持 Effect 组合；必须提供 Promise callback 时，捕获运行 Context 并传递取消信号。同时给评估加 generation ID，Tool/Transcript/Compacted 在接收时检查 generation。仅取消等待不能撤回已经进入邮箱的消息，因此需要两层防护。

### 4. [P1] Codex adapter 丢弃取消信号，取消后的任务仍会提交

位置：`packages/integrations/src/codex/agent.ts:199`、`:227`。

上层 external-agent 契约传入 AbortSignal，但 submit/resume 和 RPC call 链没有消费它。提交包含 ensure、创建目录、thread/start、turn/start 等多个异步阶段，取消后仍可能继续启动外部工作。

使用本地假 Codex executable，给 submit 传入调用前已经 aborted 的信号，仍得到新 session：

```json
{ "aborted": true, "sessionId": "started-despite-abort" }
```

建议：在每个有副作用的 RPC 前检查 signal；RPC pending request 支持取消等待并清理 timer/listener。对于已经发出的外部请求，继续保留 outcome-unknown/查询恢复语义，不能把本地取消视为远端一定没执行，也不能自动重复提交。

### 5. [P2] Goal tools 把基础设施 defect 转成普通工具错误

位置：`packages/core/src/goals/actors.ts:492`。

`catchCause(cause => Effect.fail(new Error(String(cause))))` 将操作中的 defect 一起降成普通失败，随后返回给模型。存储错误、程序错误与用户提供的非法工具参数被混在同一通道，绕开 Actor supervision。

本地向 task_create 注入一次存储异常，收到 `{ error: "Cause([Die(Error: injected one-time disk failure)])" }`，ActorRestarting 数为 0。

建议：业务校验用带 tag 的 GoalToolError，经 Effect.fail 返回；只处理预期错误通道。意外 defect 按 runtime 的监督契约传播。当前操作里大量 `throw new Error` 也要按用途区分，不能只机械删除 catchCause。

### 6. [P2] Mail 把可预期响应解析失败当成 defect，可能重置邮件基线

位置：`packages/lark-integration/src/mail/client.ts:35`、`:50`、`:66`；基线见 `packages/lark-integration/src/mail/channel-actor.ts:84`。

parseRecentIds 等解析器会对非法 JSON、`ok:false`、缺失字段抛异常，但被直接放在 Effect.map 中。其异常进入 defect 通道，不能到达现有 Listed/Failure 的轮询重试分支。Actor 重启重新创建 seen 和 initialized，下一次成功列表会被当成初始基线，期间的新邮件可能被跳过。

本地假 lark-cli 以 exit 0 返回 `ok:false`，实际 Exit 为 defect，预期错误通道为空。基线丢失后果根据 channel 恢复代码推导，未连接真实邮箱验证。

建议：在外部响应边界用 Effect.try 或 Schema 的 Effect 解码，映射到明确的 LarkResponseError，让预期上游错误走现有重试分支。

### 7. [P2] 完整 history 的同步扫描位于每次 Goal 保存的热路径

位置：`packages/integrations/src/storage/file-goal-history.ts:24`、`:81`；调用处 `packages/core/src/goals/actors.ts:156`。

history.read 的 after 只过滤结果，不定位文件偏移；每次从文件头同步 readSync + JSON.parse。Goal 保存工作窗口时都会调用 read，即使 historyThrough 已推进到尾部，也要重新扫描压缩过的旧历史。随着完整 history 增长，读取最近一页仍需遍历旧记录，并占用整个 Node 事件循环。

另有 append 的 fsyncSync 加重热路径阻塞。Effect 的 fiber、timeout 和 Scope 不能使这类同步调用可抢占；同进程 HTTP、轮询、取消都要等待它返回。

这是源码确认的复杂度与阻塞问题，未做生产规模延迟基准。建议保存 seq 到文件偏移的索引，或提供持久化 tail/checkpoint；存储服务使用异步 Effect 接口并串行化需要有序落盘的写入，保留现有持久性保证。仅用 Effect.sync 包住同步 I/O 不会解决阻塞。

## 从入口看架构

| 层次                         | 判断                                                                                                              |
| ---------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| cli.ts                       | 解析命令和执行边界适合留在这里；CLI 使用 async/Promise 本身不是问题，也不必为了减少代码行数把所有步骤变成 Layer。 |
| application.ts / services.ts | Layer 组合与启动顺序已经分离；外层服务资源晚于内层 Actor/订阅释放，方向合理。                                     |
| Actor runtime                | 有明确 Scope、邮箱、pipeToSelf、监督和持久化边界；主要问题是业务 Actor 没有一致遵守恢复契约。                     |
| Goal / Signal                | 错误和取消语义在内部 Promise 桥接处断开；这是比“有没有使用 Layer”更优先的架构问题。                               |
| Lark IM                      | summary gate 归 LarkIntegration 自己提供、SystemOneClient 由全局注入，这个所有权应保留。                          |
| Agent / integrations         | SDK 边界使用 Promise 合理；必须保证 signal、回调、外部回执与 Scope 的对应关系。                                   |
| storage / memory             | 有明确持久化与资源清理意图；存储的同步服务契约限制了调度、取消和错误表达。                                        |

整体是具备 Effect 骨架、但内部仍混用两套执行模型的实现。应优先补齐生命周期和失败语义，再继续抽 Layer。

## 后续设计改进，不与已复现缺陷混同

- **内部服务返回 Effect**：GoalRuntime、GoalReasoner、TaskPreparation、SystemOne 等由调用者反复 tryPromise/runPromise 转换。建议让领域服务保留 Effect 的错误和环境类型，Promise 转换集中在 HTTP、CLI、第三方 SDK callback 边界。合法边界不需要一律移除 runPromise。
- **保留运行环境**：`goals/agent-reasoner.ts:22` 只重新注入 Models；`actor/system.ts:634` 从空 Context 建立服务环境。需要明确继承哪些 Clock、Tracer、日志服务，避免顶层提供的观测/测试配置无法进入内部执行。不可无差别继承实例 Scope。
- **System One 支持取消**：当前 service 接口只有 request，SDK 支持的每次调用 signal 没有暴露。SDK 自带默认 10 秒请求超时，不能据此声称请求会无限挂起；但超时不替代调用方取消。
- **IM admission queue**：当前自实现 Promise FIFO、计数器、setTimeout/Date.now。可以用 Effect Queue、Semaphore、Clock 和 scoped worker 统一测试与关闭语义。现有实现已经处理等待者 abort，未确认无条件泄漏，不应直接重写后宣称修好了泄漏。
- **可预期错误建模**：广泛使用 Error 难以按类型表达“重试、用户纠正、外部结果未知”；应优先为 I/O 和业务边界定义 tagged error，而不是仅为消除诊断而替换所有 Error。

## 应保留的实现

- Agent SDK 包装有 acquireUseRelease、abort、等待 idle；SDK 的异步 transcript listener 会被 await，未发现“异步 listener 完全无人等待”的问题。
- pipeToSelf 绑定实例 Scope，并区分预期失败、defect、interruption。
- Signal 的 revision/deadline 检查、确认后执行、外部结果未知状态有明确价值。
- IM 先完成持久化交接再推进游标，符合恢复要求。
- Goal 的完整 history 与 summary + messages 工作窗口分离，设计方向正确；问题在具体快照内容和读取方式。
- 持久化异常进入 supervision 在部分 Actor 路径中是有意设计，不能把所有 orDie 都判为坏实践。

## 验证与范围限制

- `pnpm test` 通过：全工作区 build 成功，后端 122 个测试全部通过（agent 8、actor 30、core 24、memory 7、integrations 10、local 43）。Lark 场景主要通过 local 测试覆盖；本次未另外运行浏览器测试。
- 对 7 个后端 TS 项目运行 Effect language-service 诊断，涉及 94 个文件（包括部分测试）：4 errors、72 warnings、145 messages。4 个 floatingEffect error 已逐一核对，分别为 Effect-able 类的 Object.assign 返回值及刻意的类型测试调用，不作为已确认生产缺陷。其余诊断多数是错误类型和写法建议。
- 上述 1–6 使用本地 Actor、内存 store 或假 CLI/SDK 回调验证；第 7 是静态路径与复杂度分析。未启动真实应用，未向真实 Lark、Codex、Doubao 提交任务。
- 临时复现脚本保存在 `/tmp/aster-review-{timer-growth,errors,stale-callback,cancel,mail-error}.mjs`；它们是审查辅助材料，尚未加入正式回归测试。
- React 前端不是 Effect 实现，本次重点审查其 HTTP/事件消费边界，不把这份报告视为完整的 UI 交互审计。
- 当前工作区没有 Git 元数据，无法按 commit diff 做增量审查；报告基于当前文件。按用户要求检查了根目录 AGENTS.md，文件不存在。

建议处理顺序：先修复 1–4 的数据膨胀与生命周期问题，再修复 5–6 的错误通道，随后处理 history 存储复杂度和内部服务的 Effect 化。每类修复增加对应故障注入测试，不需要先全仓重写。
