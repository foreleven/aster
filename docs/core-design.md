# Core architecture

Core owns Contexts, Goals, Signals, Task delivery, execution and approvals. It imports no concrete integrations or infrastructure adapters. `AsterRuntime` assembles these capabilities and owns their lifetime.

```text
Context sources → durable Context events → System One
                                       ├→ matching Goals → Context-only Agent gate → conversation
                                       └→ matching Context Signals → Task
Schedule → Signal → Task
Task → Goal message
     → Run → human confirmation → Delegate → execution feedback to replyTo Goal
```

Each active Goal and each eligible Context Signal is matched independently. Matching a Signal owned by a Goal does not exclude that Goal from screening. System One is a routing hint; the Goal's separate read-only Agent gate protects its conversation from unrelated Context changes. Direct user input, Task messages and execution feedback bypass that gate.

## Ownership

| Module        | Responsibility                                                                                             |
| ------------- | ---------------------------------------------------------------------------------------------------------- |
| `context/`    | Schema validation, revisioned commits, detached reads, public views and durable source events              |
| `reactions/`  | Freeze screening evidence, match targets, persist decisions, deliver exact commands and reconcile receipts |
| `goals/`      | Persistent conversation, Context relevance gate, input admission, business progress and history            |
| `signals/`    | Context or schedule trigger, definition ownership, frozen Task occurrences and delivery                    |
| `tasks/`      | Typed Actor messages, Run admission, execution confirmation, recovery and optional result publication      |
| `delegation/` | External executor handles, status, input requests, resumption and ambiguous-outcome reconciliation         |
| `approvals/`  | Durable human decisions and delivery to the Actor that requested them                                      |
| `memory/`     | Backend-independent recall and durable capture orchestration                                               |
| `runtime/`    | Assembly, source activation, root readiness, application API and shutdown                                  |

The default personal assistant is the ordinary Goal `/goals/personal`. It has the same conversation, tools and input protocol as other Goals. It starts idle so startup does not manufacture a user request. There is no PersonalActor, Personal RPC family, notification root or business-notification outbox.

## Task and Signal contracts

A Task is either `{ _tag: "Goal", target, text }` or `{ _tag: "Delegate", agent, task, replyTo, action? }`. The Delegate payload contains prepared instructions and source material. A TaskMessage adds stable request identity, source, creation time, causal budget and optional frozen evidence.

A Signal has `slug`, `trigger` and `task`. The trigger is either `{ _tag: "Context", when }` or `{ _tag: "Schedule", schedule }`. Schedules support an absolute one-time timestamp or a cron expression with timezone. Both triggers freeze and deliver the same TaskMessage protocol. Signal state adds owner, active/configuration revision, timer position, occurrences and command receipts; it does not own external execution.

Delegate Tasks create `/runs/<sha256(source, requestId)>`. The Run saves admission before acknowledgement, freezes executor policy, obtains human confirmation, and starts a Delegation. Task preparation and a second System One readiness check are not separate execution stages. Run results return directly to the specified Goal; approvals contain only requests that need a human decision or information.

## Durability and cancellation

Actor mailboxes are the single writers. Accepted inputs, decisions and occurrences commit before acknowledgement. Exact retries reuse their receipt; identity collisions fail. Long operations use scoped Effects and `pipeToSelf`; late results carry generation checks. Context and Goal queries expose public projections rather than private recovery state.

Unknown external outcomes never authorize automatic resubmission. Delegation observes or looks up the original execution. Explicit Run resumption retains its own durable receipt and uncertainty markers. Goal End interrupts local conversation work and revokes unstarted Tasks and owned Signals; already-submitted external work keeps its existing owner.

See [Context design](context-design.md), [Goals](goals-design.md), [Task delegation](task-delegation-design.md), [runtime](runtime-design.md) and [Actor](actor-design.md) for the detailed contracts. This architecture replaces the previous domain protocols; no historical domain-state migration is provided.
