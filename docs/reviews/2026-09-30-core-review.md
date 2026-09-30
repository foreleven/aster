# Core 边界与生命周期审查

本轮基于 `effect@4.0.0-rc.117` 的实际实现，追踪 runtime → Context reactions → Goal/Signal → Run → Delegation → approval/history 的调用链。重点是依赖、状态和取消的所有权，不以文件行数作为拆分标准。以下问题已修复。

## 已修复的发现

1. **P1：readiness 在 defect 或关闭时可能永远不完成。** 原初始化 Fiber 只捕获 typed error，defect 不会结束 Deferred；Fiber 在开始执行前被取消时也没有可运行的清理回调。现在使用启动 Exit 完成 readiness，并由关闭流程结束仍然等待的 Deferred。测试覆盖原始 defect 与关闭后再次等待 readiness。
2. **P1：一个集成停止失败会跳过后续清理。** 原顺序 generator 遇到 defect 后退出，其他集成停止与 capture drain 均可能遗漏。现在各阶段通过 `Effect.ensuring` 组合，保持逆序停止集成、关闭 Actors、drain 的顺序，并保留多个失败原因。
3. **P1：跨 Actor 的回复等待阻塞控制消息。** Goal 的 Signal 工具操作、任务取消和完成后停用，以及独立 Signal 的 Run 初始化，在 handler 内等待其他 Actor。现在远端操作通过 `pipeToSelf` 回传；任务修订先持久化，再发送取消通知。Signal 编辑通过每个 Goal 的 Semaphore 保持顺序，排队操作在真正发送前重新检查 Goal/generation。测试让回复保持未完成，同时验证 End 与 Configure 仍能处理。
4. **P2：Goal 内部 Promise 桥接丢失执行环境和取消。** `makeGoalRuntime` 曾通过 `Effect.runPromise(root.ask(...))` 创建独立执行，调用者的 Clock 和取消无法延续。reconcile/editSignal/deactivate 现在返回 Effect；测试注入 TestClock 并中断调用 Fiber，确认下游等待及时清理。
5. **P2：ContextStore 可以通过保留的对象引用绕过 registry 写状态。** load/save 边界现在都使用 detached copy，与对外 get/snapshot 的隔离契约一致。测试分别修改 loadAll 原对象和 save 参数，registry 状态保持不变。
6. **P2：memory capture 去重早于交接成功。** capture 抛出 defect 时，原 Set 已记录 session，后续变化不能再次交接。现在只有 sink 接受后才记为已处理；测试验证失败后重试、成功后去重。

另修复了无 Actor 服务依赖的集成无法注册的问题：register 保留调用端泛型，在异构安装列表内部统一擦除类型，允许 `Context.empty()`。

## 分层结果

| 边界          | 下沉后的职责                                                                          | 上层保留的职责                                          |
| ------------- | ------------------------------------------------------------------------------------- | ------------------------------------------------------- |
| Context       | `model.ts` 定义数据；`registry.ts` 负责验证、落盘交接和通知                           | `actor.ts` 只处理注册生命周期和路径映射                 |
| Goal          | `evaluation.ts` 负责工作窗口、压缩和推理；`runtime.ts` 负责 Effect 形式的 Signal 协调 | GoalActor 串行提交状态、调度副作用和校验评估 generation |
| Signal / Task | `tasks/run.ts` 负责准备、可执行性检查、确认与委托                                     | SignalActor 管理监控条件、定时器和持久化 occurrence     |
| 应用 runtime  | 构建内部领域服务、启动 roots 与集成、管理 readiness 和关闭                            | local 选择配置来源、基础设施适配器与传输                |

`SignalRunActor` 的公共名称和 Actor identity 保持不变；实现移动到 tasks，使 Goal 无需经过 Signal 监控层即可复用执行流程。`GoalRuntime.reconcile(goal, subscriber)` 只恢复已持久化的监控与订阅，移除了不再使用的 plan/inputs 参数。

外部提供 ConfigProvider、ContextStore、GoalHistoryStore、Models、SystemOneClient、ExternalAgents，以及 MemoryRecall/ContextCaptureSink（可由 MemoryIntegration 提供）。ContextRegistry、SignalCommands、TaskPreparation、GoalRuntime 和 Actor 生命周期由 core 构建，不增加供应用层逐个组装的内部服务。

## 后续优化已落地

- **Goal Task 规则下沉：** `goals/tasks.ts` 的 `decideTaskOperation` 负责创建、修订、删除和执行复用规则，返回 `Read` / `Save` / `Execute`。GoalActor 只提交状态并调度副作用，仍是唯一写入者。额外修复了提案已持久化但 Run 尚未注册时的重复执行窗口；提案记录可选 revision，旧数据保持兼容。修订仅替换尚未提交的提案，运行中和结果不确定的委托仍复用。
- **Run / Delegation 状态模型：** 新增 `tasks/run-state.ts` 和 `delegation/state.ts`，按阶段明确 task/session/result 的要求，写入与恢复时均验证。保留已有 `status` 字段，不引入新的持久化 `_tag`。允许提交结果不确定时没有 session，以及 Run 恢复完成时没有收到新的 Submitted 通知。损坏的恢复状态在外部调用前停止，不自动覆盖或删除。
- **Effect 服务契约：** SystemOneClient、TaskPreparation、Signal/Goal screening 和 readiness 已迁移为 Effect。决策请求失败使用 `DecisionError`，准备和检查失败使用 `TaskPreparationError`；defect 仍交给监督处理。SDK 的请求与重试接收调用 Fiber 的 AbortSignal，内部 Task Agent 直接在调用 Fiber 执行。应用层继续只提供基础设施，不需要新增内部服务组装。

后续轮次已将 memory recall、extractor/description 改为 Effect，详见下文。外部执行适配器在本轮也已迁移为 Effect，IM admission 仍保留显式 Promise 边界。

## Goal 推理链路继续收敛

`GoalReasoner.plan/compact` 及工具、transcript 回调已改为 Effect，新增 `GoalReasoningError` 表达模型、输出校验和压缩失败。`evaluation.ts` 直接组合历史读取、压缩、mailbox 确认和规划，不再使用 `tryPromise` / `runPromise`；历史边界仅在全部压缩成功并持久化后推进。

Promise 转换集中在 `agent-reasoner.ts` 的 SDK 回调边界。每次 plan 捕获调用方 Context，回调保留 Clock 等服务；每次调用拥有独立取消范围，关闭时先取消回调等待，再等待 Agent idle，避免 transcript/tool 的未完成确认阻塞关闭。memory 工具使用同一桥接；后续轮次已给后端查询增加取消信号，避免只停止本地等待。

Goal End 使用 Deferred 触发评估 Fiber 中断；重启/停止由 Behavior scope 中断。保留 generation 校验并拒绝退休回调。SDK 可能把工具 Promise rejection 转为模型可见错误，因此 Effect 回调 defect 另由原生失败通道终止评估并进入 Actor 监督，不能被吞掉后继续生成成功计划。

## Memory 与内部推理边界

`MemoryRecall.search/expand`、SignalExtractor 和 DescriptionInitializer 已迁移为 Effect。共享 memory 能力及 `MemoryRecallError` 归 core 的 `context/memory.ts` 所有，移除原先定义在 GoalReasoner 旁的 `GoalMemory`。MemoryIntegration 使用 `makeMemoryRecall` 接入后端；应用层仍只提供原来的基础设施 Layer。

MemoryClient 的 Promise 仅留在后端适配器：search options 和 expand 接收 AbortSignal，HTTP request 合并调用方取消和原有 15 秒超时，读取响应前后检查取消，阻止取消后继续发起 consolidated-memory fallback。CLI recall 同样传入 Effect 的信号。capture/drain 不变，避免破坏持久化交接。

Goal 与 InternalAgent 共用 `reasoning/agent-callbacks.ts`，统一调用方 Context、取消顺序和 callback defect 传播。description/extraction 在访问模型返回值前使用 Schema 解码，null 或缺失字段进入 tagged error；提取结果仍过滤非候选 ID 并去重。

## 剩余边界

- **剩余 Promise ports：** memory capture/drain 与部分集成工作队列保留 Promise 边界；外部执行的 Promise 仅留在基础设施 transport 内。需要结合外部会话恢复和持久化交接约束继续审查，不能仅替换返回类型。
- **长期数据容量：** Signal occurrences、seenSources 与部分 Context messages 持续增长。应先定义保留和重放需求，再设计持久化历史与工作集边界，不能直接截断作为去重依据的数据。

## 验证边界

新增测试使用真实 Actor runtime、内存存储、TestClock 和受控适配器，覆盖上述生命周期、环境、隔离及消息进展契约。没有启动真实应用、调用外部模型或提交真实委托。保留既有状态字段，仅新增可选的提案 revision；旧数据仍按各阶段的必需字段验证，不删除用户数据。

追加测试覆盖 Task 去重、修订、删除和执行复用，Run/Delegation 各阶段前置条件及损坏数据恢复拒绝，以及 readiness 的 typed error、Fiber 取消和 SDK fetch 的真实 abort。

本轮验证结果：全量 `pnpm test` 通过 164 项测试（追加 10 项），`pnpm check` 通过。Core、integrations、Lark 和 local 的 Effect language-service 均无 error/warning；Core 保留 5 条非阻断的信息级建议。

Goal 推理链路这一轮追加 7 项边界测试，全量 `pnpm test` 通过 171 项，`pnpm check` 通过。Core 的 Effect language-service 无 error/warning，剩余 4 条信息级建议。测试使用受控 Agent 和存储适配器，覆盖 Context/Clock 传递、等待持久化、工具/transcript/memory 等待取消、压缩失败保留边界、Goal End、退休回调和 defect 监督；未调用真实模型。

Memory/内部推理这一轮追加 5 项测试，全量 `pnpm test` 通过 176 项，`pnpm check` 通过。Core、memory 与 local 的 Effect language-service 均无 error/warning。Core 保留 4 条信息级建议。另将 local 的快照测试改为按事件快照判断、用 Deferred 控制交接顺序，去掉对 Promise 微任务时序的依赖。未调用真实模型或记忆后端。

## 外部执行与终态重放

`ExternalAgent.submit/status/resume/wait/respond` 已统一返回 Effect 和 `ExternalAgentError`，Delegation 不再负责 Promise 桥接和 AbortSignal 参数。`integrations/external-agent.ts` 收拢 transport 适配，`ExternalAgentsLive.layer` 负责资源释放；core 的领域端口移除 `close`，应用仍只注入原来的基础设施 Layer。

修复恢复漏洞：已保存为 failed/cancelled 的 Delegation 原先恢复后进入 Poll，Poll 又跳过终态，导致 Run 永远收不到结果。现在 completed/failed/cancelled/unknown 在查找执行器之前重放，父 Actor 重启后再次 Start 也遵循同一规则。权威失败状态先持久化再通知 Run，避免通知与保存之间中断留下 uncertain 状态。

取消只结束本地等待，并向传输传递信号，不推断外部执行已取消，也不自动重试提交。Doubao 在排队命令开始及多个副作用之间检查取消，即便前一个 transport 忽略 AbortSignal，也不会继续创建会话或发送审批回复；wait 取消不触发 status fallback。memory capture/drain 保留持久化交接与关闭前排空语义，不机械替换成可中断查询。

外部执行这一轮新增 5 项回归测试，全量 `pnpm test` 通过 181 项，`pnpm check` 通过。Core、integrations 和 local 的 Effect language-service 均无 error/warning；Core 保留 4 条信息级建议。测试覆盖终态重放、失败持久化顺序、惰性执行、错误原因保留、Fiber/transport 取消和取消后的副作用阻断；全部使用模拟传输或本地假 RPC 进程，没有提交真实外部委托。
