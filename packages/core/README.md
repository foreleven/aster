# @aster/core

Aster's domain and application runtime, implemented with Effect 4. Concrete storage/model/executor adapters live in `@aster/infra`; external business connections live in `@aster/integrations`.

`AsterRuntime.layer({ integrations })` assembles shared services and owns root Actors. The host supplies infrastructure Layers and ConfigProvider, then uses `runtime.api` and `runtime.ready`. It does not assemble internal domain services or start integrations separately.

Startup subscribes to Context changes before registering/activating source integrations, restores Signals and Runs, registers Goals, then starts durable reaction processing. Signal execution starts after receiver restoration. Required source readiness precedes Goal activation. Shutdown stops producers, cancels scoped processing, stops Actors and drains accepted capture work before infrastructure release.

## Domain modules

- `context/`: validated snapshots, revisioned persistence, public views and durable source events.
- `reactions/`: System One matching of each active Goal and Context Signal, frozen decisions and receipt-based delivery.
- `goals/`: durable conversation, Context-only Agent gate, user and Task inputs, progress and history. `/goals/personal` is the default assistant using the ordinary GoalActor.
- `signals/`: Context-condition and schedule triggers; both deliver frozen Task messages.
- `tasks/`: Goal/Delegate message dispatch, admitted Runs, confirmation, feedback and optional publication.
- `delegation/`: executor sessions, status, information requests and uncertain-outcome reconciliation.
- `approvals/`: durable human decisions, validation and acknowledgement by the requesting Actor.
- `memory/`: recall contracts and durable capture orchestration.
- `runtime/`: assembly, policy registration, integrations, readiness, API and shutdown.

A Task is a message to a Goal or a Delegate. Delegate instructions and evidence are prepared by the caller; the Run freezes executor policy, confirms execution and returns results to the specified Goal. There is no separate PersonalActor, notification processor, Task-preparation model or execution-readiness model.

Context changes first pass System One matching. A Goal additionally runs its read-only Agent gate before admitting that evidence to the conversation. User input, Task messages and execution feedback bypass this second gate. Model failures and ignored evidence remain visible. Tool callbacks and asynchronous Actor work preserve their owning Effect scope and generation.

All durable domain admissions acknowledge only after commit. Exact retries retain their receipt. Unknown external submission, resume or publication does not permit automatic resubmission. Actor mailboxes own state; public projections exclude credentials, provider handles and native frames. `@aster/core/testing` provides isolated Context fixtures for tests.

See [core architecture](../../docs/core-design.md), [Context](../../docs/context-design.md), [Goals](../../docs/goals-design.md), [Task delegation](../../docs/task-delegation-design.md) and [runtime](../../docs/runtime-design.md). Old domain protocols and historical domain states have no compatibility path.

```sh
pnpm --filter @aster/core build
pnpm --filter @aster/core test
pnpm exec effect-language-service diagnostics --project packages/core/tsconfig.json --format json --severity error,warning
```
