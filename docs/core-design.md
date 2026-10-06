# Core architecture

Core owns Contexts, Goals, Signals, Task delivery, execution and approvals. It imports no concrete integrations or infrastructure adapters. `AsterRuntime` assembles these capabilities and owns their lifetime.

```text
Context sources → durable Context events → System One
                                       ├→ matching Goals → Context-only Agent gate → conversation
                                       └→ matching Context Signals → Task
Schedule → Signal → Task
Task → Goal message
     → TaskActor → internal Agent / confirmed external executor → feedback to replyTo Goal
```

Each active Goal and each eligible Context Signal is matched independently. Matching a Signal owned by a Goal does not exclude that Goal from screening. System One is a routing hint; the Goal's separate read-only Agent gate protects its conversation from unrelated Context changes. Direct user input, Task messages and execution feedback bypass that gate.

## Ownership

| Module       | Responsibility                                                                                                            |
| ------------ | ------------------------------------------------------------------------------------------------------------------------- |
| `context/`   | Schema validation, revisioned commits, detached reads, public views and durable source events                             |
| `reactions/` | Freeze screening evidence, match targets, persist decisions, deliver exact commands and reconcile receipts                |
| `goals/`     | Persistent conversation, Context relevance gate, input admission, business progress and public Pi conversation projection |
| `signals/`   | Context or schedule trigger, definition ownership, frozen Task occurrences and delivery                                   |
| `tasks/`     | Persistent Task admission, internal/external execution, follow-up, recovery and optional publication                      |
| `approvals/` | Durable human decisions and delivery to the Actor that requested them                                                     |
| `memory/`    | Backend-independent recall and durable capture orchestration                                                              |
| `runtime/`   | Assembly, source activation, root readiness, application API and shutdown                                                 |

The default personal assistant is the ordinary Goal `/goals/personal`. It has the same conversation, tools and input protocol as other Goals. It starts idle so startup does not manufacture a user request. There is no PersonalActor, Personal RPC family, notification root or business-notification outbox.

## Task and Signal contracts

A Task is a Goal message, an internal Agent Task, or a Delegate Task selecting an external executor. Execution payloads contain prepared instructions and material. A TaskMessage adds request identity, source, creation time, causal budget and optional frozen evidence.

A Signal has `slug`, `trigger` and `task`. The trigger is either `{ _tag: "Context", when }` or `{ _tag: "Schedule", schedule }`. Schedules support an absolute one-time timestamp or a cron expression with timezone. Both triggers freeze and deliver the same TaskMessage protocol. Signal state adds owner, active/configuration revision, timer position, occurrences and command receipts; it does not own external execution.

Agent and Delegate Tasks create `/tasks/<sha256(source, requestId)>`. One TaskActor owns admission, input delivery, executor handles, follow-ups and results. External execution requires confirmation. The main Goal handles simple dialogue and routes sustained work or follow-ups to Tasks. All Goal and Task messages use Pi; Actor state retains references and business state. There is no separate Run, Delegation or GoalHistory store.

## Durability and cancellation

Actor mailboxes own lifecycle and execution scheduling. Goal business writes use one Actor-local GoalState service that serializes both mailbox requests and local tool calls; other domain state remains mailbox-owned. Accepted inputs, decisions and occurrences commit before acknowledgement. Exact retries reuse their receipt; identity collisions fail. Long operations use scoped Effects and `pipeToSelf`; late results carry generation checks. Context and Goal queries expose public projections rather than private recovery state.

Unknown external outcomes never authorize automatic resubmission. Task observes or looks up the original execution. Explicit Task resumption retains its own durable receipt and uncertainty markers. Goal End interrupts local conversation work and revokes unstarted Tasks and owned Signals; already-submitted external work keeps its existing owner.

See [Context design](context-design.md), [Goals](goals-design.md), [Task delegation](task-delegation-design.md), [runtime](runtime-design.md) and [Actor](actor-design.md) for the detailed contracts. This architecture replaces the previous domain protocols; no historical domain-state migration is provided.
