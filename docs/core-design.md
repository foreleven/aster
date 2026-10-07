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

Each active Goal and each eligible Context Signal is matched independently. A failed match retains its target and frozen input while successful matches remain deliverable. Explicit screening recovery retries only failed targets, including after restart; it never repeats successful delivery. Matching a Signal owned by a Goal does not exclude that Goal from screening. System One is a routing hint; the Goal's separate read-only Agent gate protects its conversation from unrelated Context changes. Direct user input, Task messages and execution feedback bypass that gate.

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
| `services/`  | Shared decision transport, Actor addressing and query communication contracts                                             |

`services/` holds capabilities shared across domains without one domain owner. `SystemOneClient` is implemented by infra and consumed by Goal screening, Signal matching and integration gates. `CurrentActors` exposes only the current execution's Actor addressing capability; Goal, Task, Reaction and tools depend on this contract directly. Shared query replies and cancellation live alongside CurrentActors in `services/actors.ts`; tool-specific ask/error handling remains in `tools/actors.ts`. Reaction recovery validation stays private to `reactions/model.ts`, while `json.ts` owns pure public JSON normalization. Domain state, persistence, execution services stay next to their owning modules. Services are injected through Effect Context; this directory introduces no service registry or additional forwarding Layers.

The default personal assistant is the ordinary Goal `/goals/personal`. It has the same conversation, tools and input protocol as other Goals. It starts idle so startup does not manufacture a user request. There is no PersonalActor, Personal RPC family, notification root or business-notification outbox.

ApprovalState and ReactionState are Actor-local Effect services. Their mailboxes are the sole writers; state transitions persist before updating a private Ref. Approval replies are typed Accepted/Rejected values; resolving a request triggers immediate delivery, while a periodic retry retains unacknowledged decisions. ReactionPolicy reads frozen evidence and uses the injected current Actor addressing capability for delivery. There is no separate root-ref binding phase.

## Task and Signal contracts

A Task is a Goal message, an internal Agent Task, or a Delegate Task selecting an external executor. Execution payloads contain prepared instructions and material. A TaskMessage adds request identity, source, creation time, remaining Agent-turn budget and optional frozen evidence.

Configured Signals have a `slug`, `trigger` and `task`. Their public snapshot retains only optional `owner` (a full Goal path), `trigger`, `task`, `status` (`active | paused | deleted`), `version`, and the schedule cursor. `version` tracks definition and lifecycle changes independently of the Context revision. The trigger is either `{ _tag: "Context", when }` or `{ _tag: "Schedule", schedule }`. Schedules support an absolute one-time timestamp or a cron expression with an IANA timezone.

`nextDue` is a timezone-qualified ISO 8601 string, normalized to UTC `Z` when calculated. A scheduled Signal always has `nextDue`, with `null` meaning no further occurrence. A Context Signal has no cursor. Cron calculations retain `schedule.timeZone`, including daylight-saving transitions; milliseconds are transient scheduling arithmetic only.

SignalActor owns timers and asynchronous delivery. Its Actor-local SignalState owns rule changes and trigger admission; its private Store appends Pi events, commits the public Context projection and then updates a committed Ref. The mailbox is the sole state writer. Pi retains frozen TaskMessage envelopes, remaining Agent-turn budgets, receipts and delivery history, rather than exposing those records in the public snapshot. A one-shot trigger freezes its message and exhausts its cursor in one event. Timer callbacks must match both definition version and due time.

Pause retains pending messages for resume. Delete cancels never-started messages. Durably started deliveries remain `sending` until a receipt or rejection is known; paused/deleted Signals only inspect receiver receipts and never submit again. Active Signals may retry the same immutable message and request identity. Rule edits do not rewrite accepted messages. Receivers validate Signal authority against its Pi journal. Delivery phases do not change the public rule or invalidate System One screening. Signals never own Task execution or System One matching. `signals/protocol.ts` owns reaction inputs; `reactions/policy.ts` selects candidate Signals and matches their conditions against Context evidence. State schemas, eligibility and public views live together in `signals/state/snapshot.ts`; business transitions and persistence remain in `state/model.ts` and `state/store.ts`. Signal protocols are `Change`, `ListByOwner` and `PauseByOwner`; owner queries use full Goal paths. Goal lifecycle coordination uses ActorContext directly. `tasks/delivery.ts` owns both submission and receiver receipt lookup, so Signals do not read Goal or Task receipt layouts.

Agent and Delegate Tasks create `/tasks/<sha256(source, requestId)>`. TaskActor schedules admission, follow-ups and results. TaskState owns business transitions; TaskExecution privately owns provider handles and delivery checkpoints in Pi. External execution requires confirmation. The main Goal handles simple dialogue and routes sustained work or follow-ups to Tasks. All Goal and Task messages use Pi; Actor state retains references and business state. There is no separate Run, Delegation or GoalHistory store.

## Durability and cancellation

Actor mailboxes own lifecycle and execution scheduling. Goal business writes use one Actor-local GoalState service that serializes both mailbox requests and local tool calls; TaskState exposes accept/start/settle/cancel, with the Task mailbox as its sole caller. TaskExecution serializes executor transitions outside that mailbox. Each model owns a private committed snapshot Ref; executor adapters do not write snapshots. Accepted inputs, decisions and occurrences commit before acknowledgement. Exact retries reuse their receipt; identity collisions fail. Long operations use scoped Effects and `pipeToSelf`; late results carry generation checks. Context and Goal queries expose public projections rather than private recovery state.

Unknown external outcomes never authorize automatic resubmission. Task observes or looks up the original execution. `CheckTask` observes original work; separate `RetryTask` restarts only known failed work. Executor checkpoints retain delivery uncertainty in Pi. Goal End interrupts local conversation work and revokes unstarted Tasks and owned Signals; already-submitted external work keeps its existing owner.

See [Context design](context-design.md), [Goals](goals-design.md), [Task delegation](task-delegation-design.md), [runtime](runtime-design.md) and [Actor](actor-design.md) for the detailed contracts. This architecture replaces the previous domain protocols; no historical domain-state migration is provided.

## Context matching and delivery

System One freezes only eligible Signal conditions/versions and Goal slugs/titles/descriptions/summaries alongside the source event. Each Signal and Goal uses a single-target request. `config.reactions.matchConcurrency` bounds their shared pool; `config.reactions.deliveryConcurrency` bounds independent delivery slots. Both default to 4 and accept integers from 1 through 32. A Context's matching pass completes before its plan is committed. Delivery of earlier plans does not delay matching the next Context, and deliveries to the same target serialize.

Queued, unstarted revisions of the same Context coalesce to the newest snapshot without resetting that source's queue position. Already-started matching, failed decisions and pending deliveries retain their identities. Persisted per-source admission watermarks prevent superseded or pruned work from replaying after restart. Live admission consumes events attached to commit notifications; journal scanning is reserved for startup recovery. The reaction state keeps the latest 100 completed entries for diagnostics and all unfinished/failed work. Source journal retention is unchanged.

Matchers return explicit `Matched` or `NotMatched` decisions. Every attempted target retains its decision and reason; expected errors become `Failed` outcomes at the planning boundary. Negative decisions complete without delivery and remain visible in processing inspection and the public diagnostic projection. A missing or invalid model answer is a failed match, not a negative decision. Explicit retry uses frozen candidates and replaces only failed target outcomes, preserving both positive and negative decisions and their existing deliveries. Goal admissions append evidence without a stale whole-Context revision guard, then run the existing Goal-only second gate. Signal admission checks the frozen rule version and current eligibility before creating a Task. Durable receipt identities, bounded retries of unacknowledged internal delivery and external submission uncertainty rules remain unchanged.

Reaction state has two persisted phases: `queued` may coalesce, while `frozen` retains its source event and per-target inputs. Each target owns `Pending`, `Failed`, `NotMatched`, or `Matched`; only `Matched` contains a frozen command and its delivery state. Overall planning/failure/completion and error summaries are derived for inspection. Explicit matching recovery resets only failed targets to `Pending`, so existing deliveries can settle while matching runs, and their latest receipts survive restart. `sourceRevisions` prevents replay after coalescing or pruning; `recoveryReceipts` is always an array. Delivery attempts retain their retry limit; there is no aggregate matching attempt counter. Full Goal screening audits belong to the optional ScreeningStore, not the reaction snapshot.
