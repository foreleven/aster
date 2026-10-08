# Core Agent tools

Shared Context, Memory, Goal, Task and Signal operation tools live in `packages/core/src/tools/`. Goal and Task Agents assemble explicit catalogues from these implementations. The Goal-only `submit_context_relevance` result tool is private to `goals/agent.ts`. There is no common invocation service or tool Actor.

## Ownership and injection

Tools own model-facing names, descriptions, schemas, replay declarations, Effect-native execution, Actor asks and result presentation. Domain owners own validation, public projections, persistence and backend work. A Goal exposes local business operations through its Actor-scoped GoalState service. Runtime owns root registration and readiness.

`CurrentActors`, defined in `services/actors.ts`, is an Effect Context service exposing only the current Actor system's `select` capability. Goal and Task executions provide it from their owning ActorContext; runtime reasoning provides its existing ActorSystem. Tool Effects resolve this dependency when executed. Factories bind the current Goal or Pi owner only where needed. Local Goal tools additionally resolve the current GoalState business service. No tool receives a callback invoker, Registry, Memory backend, raw storage service or global system singleton.

`EffectTool<T, E, R>` and `AgentRequest<E, R>` in `packages/agent` preserve tool and hook dependencies in the runner's Effect requirement. Tools return Effects; `AgentRunner.run` accepts a request directly. The agent package captures the caller's Context and adapts tools and response observers to native SDK callbacks within one invocation scope. Promise conversion and AbortSignal handling remain private to that SDK boundary.

Goal execution retains accepted-input identity and causal policy. It binds a stable operation identity derived from Goal path, input ID and SDK tool-call ID for Task/Signal commands. Task execution likewise derives query identity from its path, accepted input and tool-call ID. These are execution bindings, not model arguments or a universal metadata envelope.

The summary tool calls the injected GoalState.updateSummary directly. That method validates and serializes the mutation with other Goal transitions and returns only after persistence. The native SDK boundary retires callbacks at invocation completion/cancellation; the model Layer rejects writes after Actor retirement. There is no summary command, generation argument or Accepted/Rejected transport round trip. The existing automatic propagation budget remains Goal/Task/Signal policy.

## Organization

```text
packages/core/src/tools/
  actors.ts                  # Typed tool ask transport using services/actors.ts
  define.ts                  # Effect tool construction and domain error/result shaping
  catalogues.ts              # Explicit Goal and Task tool sets
  context/
    list-contexts.ts
    describe-context.ts
    query-context.ts
    read-query-result.ts
  goal/
    goal-current.ts
    update-summary.ts
  task/
    schema.ts
    task-list.ts
    start-task.ts
    task-send.ts
  signal/
    schema.ts
    signal-list.ts
    set-signal.ts
  memory/
    memory-search.ts
    memory-expand.ts
```

Domain protocols and handlers remain next to their owners. The previous Goal/reasoning tool files and duplicated inline memory/result definitions are removed, without compatibility re-exports.

## Commands

| Tool                | Owner                          | Behavior                                                                                                       |
| ------------------- | ------------------------------ | -------------------------------------------------------------------------------------------------------------- |
| `list_contexts`     | `/user/contexts`               | `ListContexts` pages active capability paths and descriptions without state.                                   |
| `describe_context`  | `/user/contexts`               | `DescribeContext` returns public command descriptions and argument schemas.                                    |
| `query_context`     | `/user/contexts`               | `QueryContext` invokes the existing read-only integration route and retains the result in Pi.                  |
| `read_query_result` | `/user/contexts`               | `ReadQueryResult` reads retained evidence under the bound conversation owner.                                  |
| `goal_current`      | Injected GoalState             | Reads the current definition and public state; the catalogue supplies safe executor names.                     |
| `update_summary`    | Injected GoalState             | Validates and persists the summary through its serialized business method.                                     |
| `task_list`         | Injected GoalState             | Resolves GoalSnapshot.tasks to current public Task views, including completed Tasks.                           |
| `start_task`        | Tasks root or destination Goal | Existing StartTask/SubmitInput and AttachTask admission; returns acceptance, not execution completion.         |
| `task_send`         | Target TaskActor               | Existing FollowupTask admission with stable identity.                                                          |
| `signal_list`       | Signal root                    | `ListByOwner` takes a full owner path and returns public Signal views, status and definition version.          |
| `set_signal`        | Signal root                    | `Change` validates definition version and retains receipts; supports create, update, pause, resume and delete. |
| `memory_search`     | MemoryActor                    | `Search` executes backend recall asynchronously.                                                               |
| `memory_expand`     | MemoryActor                    | `Expand` retrieves original evidence asynchronously.                                                           |

`submit_context_relevance` is a local invocation return. It validates through the existing SDK/result decoders and terminates the gate invocation. It has no business recipient to ask. Gate results still return through `GateSettled`.

`update_summary` replaces `update_goal`; it has only a `summary` argument and cannot complete a Goal. The old End/completed lifecycle elsewhere is separate pending work, not implemented by this tools refactor.

## Read consistency and retained pages

Goal and Task Context tools read live public projections on each call. Task reads no longer freeze a snapshot at invocation start. Search returns up to 20 matches. Context pages contain up to 4,000 characters for Goal tools and 12,000 for Task tools. Continuations supply the first page's revision; a changed revision returns a conflict rather than mixing versions.

ContextsActor coordinates queries but owns no duplicate Context state. Public paths are resolved through the Context reader, not by assuming every public path addresses a live Actor. Integration queries continue through registered ContextQueries routes; core never imports integration-private protocols.

A successful integration query appends a `tool.query-result` entry to the caller's Pi conversation before returning its entry ID as `resultId`. The entry retains the input and full serialized result. Reusing a committed request identity uses the Pi request index and requires identical query arguments; it does not scan the full conversation or reread the matched entry. Paging supplies `resultId` and `offset`; the Pi owner is bound by the application and is not a model argument. Pi checks conversation ownership and the reader checks entry kind.

Query pages contain 2,000 characters. Results over 1,000,000 characters are rejected with a request to narrow the query. Different queries for the same Context retain distinct IDs. Actor restarts, native tool replay and compaction do not depend on rebuilding an invocation-local Map. These internal entries stay outside the public Goal timeline.

## Concurrency, cancellation and failures

ContextsActor and MemoryActor each admit at most four slow queries at once. Excess work receives an error. A duplicate in-flight Context query identity is also rejected. Fast catalogue reads remain available while remote queries run.

Slow operations run through `pipeToSelf`. Only mailbox handlers modify pending-request maps. Result commands correlate to transient reply IDs; a retired Behavior's result cannot match the replacement's pending requests. Behavior shutdown cancels workers.

Each ask supplies explicit query cancellation. Timeout or caller interruption signals the receiver, whose worker races that signal against backend work. Cancellation propagates into the existing integration or memory Effect. Transient reply refs and cancellation handles are never persisted. Local interruption does not establish that remote work stopped.

The SDK adapter in `packages/agent` preserves the caller's Effect Context and AbortSignal. Known query errors become rejected tool results so the Agent may respond or choose another query. Known command rejections also become rejected results. A missing write acknowledgement remains a failed callback with an uncertain outcome; moving tool implementations does not authorize automatic resubmission. Defects and interruption retain their failure semantics.

## Catalogues and runtime

| Invocation             | Tools                                                                                                  |
| ---------------------- | ------------------------------------------------------------------------------------------------------ |
| Goal conversation      | `goal_current`, `update_summary`, `task_list`, `start_task`, `task_send`, `signal_list`, `set_signal`. |
| Internal Task          | Context and memory tools, including memory expansion.                                                  |
| Context relevance gate | `submit_context_relevance` only.                                                                       |

Context and memory tools are absent from the main Goal catalogue. A lookup requiring fresh evidence starts or continues an internal Task. The Goal can still interpret evidence supplied by its Context gate or Task feedback, and coordinate Signals directly. There is no automatic timeout-based handoff or added execution budget.

Runtime starts the Context query root and activates Memory before restoring Tasks that may immediately resume tool execution. Source integration readiness and Goal activation retain their existing contracts. No host wiring or generic Actor runtime changes are needed.

Tests use real Actor mailboxes and Pi with fake backends. They cover public projections and dormant records, live reads and revision conflicts, retained paging and owner isolation, stable identity reuse, cancellation, bounded recall concurrency, responsive mailboxes, summary updates, Task admission and stopped-worker fencing.
