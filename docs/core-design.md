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
| `signals/`   | Context or schedule trigger, definition ownership and durable Task delivery                                               |
| `tasks/`     | Persistent Task admission, internal/external execution, follow-up and recovery                                            |
| `approvals/` | Durable human decisions and delivery to the Actor that requested them                                                     |
| `memory/`    | Backend-independent recall and durable capture orchestration                                                              |
| `runtime/`   | Assembly, source activation, root readiness, application API and shutdown                                                 |

The default personal assistant is the ordinary Goal `/goals/personal`. It has the same conversation, tools and input protocol as other Goals. It starts idle so startup does not manufacture a user request. There is no PersonalActor, Personal RPC family, notification root or business-notification outbox.

`publications/` independently owns result publication, its approval and transport recovery.

## Task and Signal contracts

A Task is a Goal message, an internal Agent Task, or a Delegate Task selecting an external executor. Execution payloads contain prepared instructions and material. A TaskMessage adds request identity, source, creation time, causal budget and optional frozen evidence.

Configured Signals have a `slug`, `trigger` and `task`. Their public snapshot retains only optional `owner` (a full Goal path), `trigger`, `task`, `status` (`active | paused | deleted`), `version`, and the schedule cursor. `version` tracks definition and lifecycle changes independently of the Context revision. The trigger is either `{ _tag: "Context", when }` or `{ _tag: "Schedule", schedule }`. Schedules support an absolute one-time timestamp or a cron expression with an IANA timezone.

`nextDue` is a timezone-qualified ISO 8601 string, normalized to UTC `Z` when calculated. A scheduled Signal always has `nextDue`, with `null` meaning no further occurrence. A Context Signal has no cursor. Cron calculations retain `schedule.timeZone`, including daylight-saving transitions; milliseconds are transient scheduling arithmetic only.

SignalActor owns timers and asynchronous delivery. Its Actor-local SignalState owns rule changes and trigger admission; its private Store appends Pi events, commits the public Context projection and then updates a committed Ref. The mailbox is the sole state writer. Pi retains frozen TaskMessage envelopes, causal provenance, receipts and delivery history, rather than exposing those records in the public snapshot. A one-shot trigger freezes its message and exhausts its cursor in one event. Timer callbacks must match both definition version and due time.

Pause retains pending messages for resume. Delete cancels never-started messages. Durably started deliveries remain `sending` until a receipt or rejection is known; paused/deleted Signals only inspect receiver receipts and never submit again. Active Signals may retry the same immutable message and request identity. Rule edits do not rewrite accepted messages. Receivers validate Signal authority against its Pi journal. Delivery phases do not change the public rule or invalidate System One screening. Signals never own Task execution or System One matching. `signals/protocol.ts` owns reaction inputs; `reactions/policy.ts` selects candidate Signals and matches their conditions against Context evidence. State schemas, eligibility and public views live together in `signals/state/snapshot.ts`; business transitions and persistence remain in `state/model.ts` and `state/store.ts`. Signal protocols are `Change`, `ListByOwner` and `PauseByOwner`; owner queries use full Goal paths. Goal lifecycle coordination uses ActorContext directly. `tasks/delivery.ts` owns both submission and receiver receipt lookup, so Signals do not read Goal or Task receipt layouts.

Agent and Delegate Tasks create `/tasks/<sha256(source, requestId)>`. TaskActor schedules admission, follow-ups and results. TaskState owns business transitions; TaskExecution privately owns provider handles and delivery checkpoints in Pi. External execution requires confirmation. The main Goal handles simple dialogue and routes sustained work or follow-ups to Tasks. All Goal and Task messages use Pi; Actor state retains references and business state. There is no separate Run, Delegation or GoalHistory store.

## Durability and cancellation

Actor mailboxes own lifecycle and execution scheduling. Goal business writes use one Actor-local GoalState service that serializes both mailbox requests and local tool calls; TaskState exposes accept/start/settle/cancel, with the Task mailbox as its sole caller. TaskExecution serializes executor transitions outside that mailbox, and PublicationsActor owns publication state. Each model owns a private committed snapshot Ref; executor adapters and publication workers do not write snapshots. Accepted inputs, decisions and occurrences commit before acknowledgement. Exact retries reuse their receipt; identity collisions fail. Long operations use scoped Effects and `pipeToSelf`; late results carry generation checks. Context and Goal queries expose public projections rather than private recovery state.

Unknown external outcomes never authorize automatic resubmission. Task observes or looks up the original execution. `CheckTask` observes original work; separate `RetryTask` restarts only known failed work. Executor checkpoints retain delivery uncertainty in Pi. Goal End interrupts local conversation work and revokes unstarted Tasks and owned Signals; already-submitted external work keeps its existing owner.

See [Context design](context-design.md), [Goals](goals-design.md), [Task delegation](task-delegation-design.md), [runtime](runtime-design.md) and [Actor](actor-design.md) for the detailed contracts. This architecture replaces the previous domain protocols; no historical domain-state migration is provided.
