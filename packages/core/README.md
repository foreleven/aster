# @aster/core

Aster's domain and application runtime, implemented with Effect 4. Concrete storage/model/executor adapters live in `@aster/infra`; external business connections live in `@aster/integrations`.

`AsterRuntime.layer({ integrations })` assembles shared services and owns root Actors. The host supplies infrastructure Layers and ConfigProvider, then uses `runtime.api` and `runtime.ready`. It does not assemble internal domain services or start integrations separately.

Startup subscribes to Context changes before registering/activating source integrations, starts public Context queries, registers Signal owners, activates Memory, registers publication, Task and Goal owners, then starts durable reaction processing. Signal execution starts after Goal routing registration; each Signal, Task and Goal restores independently and queues incoming messages in its mailbox. Required source readiness precedes Goal activation. Shutdown stops producers, cancels scoped processing, stops Actors and drains accepted capture work before infrastructure release.

## Domain modules

- `context/`: validated snapshots, revisioned persistence, public views and durable source events.
- `reactions/`: System One matching of each active Goal and Context Signal, frozen decisions and receipt-based delivery.
- `goals/`: durable conversation, Context-only Agent gate, user and Task inputs, business summaries and public Pi conversation projection. `/goals/personal` is the default assistant using the ordinary GoalActor.
- `signals/`: `protocol.ts` defines commands and reaction inputs; `root.ts` registers and watches owners; `actor.ts` schedules triggers and delivery; `state/snapshot.ts` defines state, eligibility and public views; `state/model.ts` owns business operations; `state/store.ts` owns Pi persistence and the committed Ref. Signal candidate selection and System One matching live in `reactions/policy.ts`. Both trigger kinds deliver frozen Task messages. Schedule cursors are timezone-qualified ISO strings, with `null` marking exhaustion.
- `tasks/`: Goal/Agent/Delegate dispatch, persistent Tasks, follow-up, confirmation and feedback.
- `publications/`: independent result publication, approval and transport recovery.
- `approvals/`: durable human decisions, validation and acknowledgement by the requesting Actor.
- `memory/`: recall contracts and durable capture orchestration.
- `tools/`: shared Agent tool catalogues, injected GoalState operations and cross-Actor asks; domain owners retain queries and mutations.
- `runtime/`: assembly, policy registration, integrations, readiness, API and shutdown.

Within `goals/`, `actor.ts` and `root.ts` own scheduling and registration, `protocol.ts` defines messages, `agent.ts` groups model execution and prompts, and `view.ts` groups public projections. `state/` contains the business model and its persistence helpers; `screening/` contains System One decisions and frozen delivery envelopes.

Within `tasks/`, `actor.ts` owns scheduling, `state/` owns admission, business transitions and persistence, and `execution/` owns internal Agent and external executor calls. `delivery.ts` groups cross-Actor delivery and feedback; `view.ts` groups read-only projections, inspection and capture. TaskState exposes `accept`, `start`, `settle` and `cancel`; TaskExecution exposes `run`, `send` and `cancel`, retaining provider checkpoints in Pi. `publications/actor.ts` owns publication independently. Root startup registers and watches children without awaiting their recovery.

A Goal handles simple dialogue and lightweight reads directly. Sustained work uses a persistent Task with internal or external execution; further instructions can steer active work or reactivate completed work. Pi owns all Goal/Task message bodies and native transcripts. Actor state retains references, receipts and lifecycle bookkeeping. The public Goal conversation includes actual users and selected assistant replies; Task details expose execution messages and available tools.

Context changes first pass System One matching. A Goal additionally runs its read-only Agent gate before admitting that evidence to the conversation. User input, Task messages and execution feedback bypass this second gate. Model failures remain visible; ignored evidence stays outside public chat. Tool callbacks and asynchronous Actor work preserve their owning Effect scope and generation.

All durable domain admissions acknowledge only after commit. Exact retries retain their receipt. Unknown external submission, resume or publication does not permit automatic resubmission. Actor mailboxes own execution scheduling; GoalState serializes local Goal mutations; TaskState exposes mailbox-only business transitions; both own a committed snapshot Ref; public projections exclude credentials, provider handles and native frames. `@aster/core/testing` provides isolated Context fixtures for tests.

See [core architecture](../../docs/core-design.md), [Context](../../docs/context-design.md), [Goals](../../docs/goals-design.md), [Task delegation](../../docs/task-delegation-design.md) and [runtime](../../docs/runtime-design.md). Old domain protocols and historical domain states have no compatibility path.

```sh
pnpm --filter @aster/core build
pnpm --filter @aster/core test
pnpm exec effect-language-service diagnostics --project packages/core/tsconfig.json --format json --severity error,warning
```
