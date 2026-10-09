# @aster/core

Aster's domain and application runtime, implemented with Effect 4. Concrete storage/model/executor adapters live in `@aster/infra`; external business connections live in `@aster/integrations`.

`@aster/core/contracts` is the browser-safe, explicitly exported domain schema entry. Definitions stay with their domain owners; internal delivery envelopes and inbox records are not public exports. Core has no dependency on `@aster/api`. API-only response schemas, RPC and query keys belong to that package.

`AsterRuntime.layer({ integrations })` assembles shared services and owns root Actors. The host supplies infrastructure Layers and ConfigProvider, then consumes `runtime.ready`, `runtime.inspect`, Actor addressing and the domain services published by that Layer. RPC adaptation belongs to `@aster/api/server`; Runtime has no application API facade. It does not assemble internal domain services or start integrations separately.

Startup starts public Context queries and the independent Memory consumer before activating source integrations, registers Signal, Task and Goal owners, then starts durable reaction processing. Signal execution starts after Goal routing registration; each Signal, Task and Goal restores independently and queues incoming messages in its mailbox. Each Goal begins execution after its own restoration, independently of source readiness; runtime readiness still waits for all integrations. Shutdown stops producers, cancels scoped processing, stops Actors and drains accepted capture work before infrastructure release.

## Domain modules

- `services/`: shared capabilities without a single domain owner. `system-one.ts` defines the decision transport used by Goal screening, Signal matching and integration gates; `actors.ts` defines the execution-scoped Actor addressing capability, shared query replies and cancellation. Domain policies and Actor-local state services remain with their owners; concrete decision adapters remain in infra.
- `context/`: validated snapshots, public views and durable source events. `store.ts` owns per-path commits and recovery using `{ snapshot, events }`; `queries/` groups Actor queries, integration routes and retained Pi evidence. Owners provide descriptions directly. Public JSON normalization lives in `json.ts`.
- `reactions/`: System One matching, frozen decisions and receipt-based delivery. `model.ts` owns committed state transitions, `policy.ts` matches targets independently, and the Actor schedules work. Signal and Goal requests share bounded matching concurrency. Pending revisions coalesce per source; persisted watermarks prevent replay. Matching and delivery have separate slots, with per-target delivery ordering. Each frozen target owns its matching result and, when matched, its delivery. Retry resets only failed targets; aggregate status is derived. Full screening audit records remain in the ScreeningStore.
- `goals/`: durable conversation, Context-only Agent gate, user and Task inputs, business summaries and public Pi conversation projection. `/goals/personal` is the default assistant using the ordinary GoalActor.
- `signals/`: `protocol.ts` defines commands and reaction inputs; `root.ts` registers and watches owners; `actor.ts` schedules triggers and delivery; `state/snapshot.ts` defines state, eligibility and public views; `state/model.ts` owns business operations; `state/store.ts` owns Pi persistence and the committed Ref. Signal candidate selection and System One matching live in `reactions/policy.ts`. Both trigger kinds deliver frozen Task messages. Schedule cursors are timezone-qualified ISO strings, with `null` marking exhaustion.
- `tasks/`: Goal/Agent/Delegate dispatch, persistent Tasks, follow-up, confirmation and feedback.
- `approvals/`: `state.ts` owns durable human decisions and validation; the Actor immediately delivers committed responses and retries until the requesting Actor acknowledges.
- `memory/`: recall contracts and durable capture orchestration. Capture identity selection precedes lazy evidence reads outside the mailbox; captured sessions skip Pi reads.
- `tools/`: shared Agent tool catalogues, injected GoalState operations and cross-Actor asks; domain owners retain queries and mutations.
- `runtime/`: assembly, policy registration, integrations, readiness, API and shutdown.

Within `goals/`, `actor.ts` and `root.ts` own scheduling and registration, `protocol.ts` defines messages, `agent.ts` groups model execution and prompts, and `view.ts` groups public projections. `state/` contains the business model and its persistence helpers; `screening/` contains System One decisions and frozen delivery envelopes.

Within `tasks/`, `actor.ts` owns scheduling, `state/` owns admission, business transitions and persistence, and `execution/` owns internal Agent and external executor calls. `delivery.ts` groups cross-Actor delivery and feedback; `view.ts` groups read-only projections, inspection and capture. TaskState exposes `accept`, `start`, `settle` and `cancel`; TaskExecution exposes `run`, `send` and `cancel`, retaining provider checkpoints in Pi. Root startup registers and watches children without awaiting their recovery.

A Goal handles dialogue, supplied evidence, summaries and Task/Signal coordination directly. Context discovery, Context/integration queries and memory retrieval are available only to internal Tasks; the main Goal catalogue has seven coordination tools. The system prompt includes configured Context paths and descriptions for routing; this metadata does not expose business data or guarantee connection readiness. Work requiring fresh evidence uses a persistent Task with internal or external execution; further instructions can steer active work or reactivate completed work. Pi owns all Goal/Task message bodies and native transcripts. Actor state retains references, receipts and lifecycle bookkeeping. The public Goal conversation includes actual users and selected assistant replies; Task details expose execution messages and available tools.

Context changes first pass System One matching. A Goal additionally runs its read-only Agent gate before admitting that evidence to the conversation. User input, Task messages and execution feedback bypass this second gate. Model failures remain visible; ignored evidence stays outside public chat. Tool callbacks and asynchronous Actor work preserve their owning Effect scope and generation.

All durable domain admissions acknowledge only after commit. Exact retries retain their receipt. Unknown external submission or resume does not permit automatic resubmission. Actor mailboxes own execution scheduling; GoalState serializes local Goal mutations; TaskState exposes mailbox-only business transitions; both own a committed snapshot Ref; public projections exclude credentials, provider handles and native frames. `@aster/core/testing` provides isolated Context fixtures for tests.

See [core architecture](../../docs/core-design.md), [Context](../../docs/context-design.md), [Goals](../../docs/goals-design.md), [Task delegation](../../docs/task-delegation-design.md) and [runtime](../../docs/runtime-design.md). Old domain protocols and historical domain states have no compatibility path.

```sh
pnpm --filter @aster/core build
pnpm --filter @aster/core test
pnpm exec effect-language-service diagnostics --project packages/core/tsconfig.json --format json --severity error,warning
```

Memory and reactions own their subscriptions and Behavior-scoped workers. Their defects enter Actor supervision, and Runtime reports permanent core-owner termination as failed health. Context maintenance no longer uses a shared Fiber or capture-sink bridge. Module Settings consume the host ConfigProvider directly; there is no legacy whole-file CoreConfig parser. Private state models and reasoning helpers are not exported from the public package entry point.
