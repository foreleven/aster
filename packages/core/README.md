# Core

This package owns the Aster domain: configuration schemas/validation, Contexts, Signal and Goal actors, delegation lifecycle, description policy, and state-change processing. Its package dependencies are Actor, Agent and Effect. Goal prompts, planning tools, Signal extraction policy and execution-readiness decisions belong here; Agent encapsulates pi.

`AsterRuntime.layer({ integrations })` owns the shared domain graph, integration activation, root Actors, Context reactions, readiness and shutdown. It exposes `api` (Context reads, Goals, approvals and diagnostics) and `ready`. Infrastructure supplies `ContextStore`, `GoalHistoryStore`, `Models`, `SystemOneClient`, `ExternalAgents` and memory capabilities; runtime builds internal reasoning, Task preparation and Goal services. Integrations register typed activation capabilities through `RuntimeIntegrations`, using `defineIntegration` to capture their Actor services. Only runtime activates these capabilities.

`GoalSettings`, `signalSettings` and `internalAgentSettings` read module-owned Config declarations. `ConfigLocation` carries source locations, not domain configuration. `parseConfig` remains a standalone validation utility; the application does not use it as a service bag. `makeGoalRuntime` receives narrow Goal settings, a Signal command port, a reasoner and history storage. Core does not load YAML, open persistence files, start CLI tools or import Lark/memory implementations. See [runtime design](../../docs/runtime-design.md).

## Internal ownership

| Module                                      | Owns                                                                                   |
| ------------------------------------------- | -------------------------------------------------------------------------------------- |
| `runtime/`                                  | Shared Layer composition, integration activation/readiness/shutdown, application API   |
| `context/model.ts`                          | Public record schemas and private Context definitions                                  |
| `context/registry.ts`                       | Snapshot validation, persistence handoff and change notification                       |
| `context/actor.ts`                          | Actor lifecycle registration and public-path mapping                                   |
| `goals/actors.ts`                           | Serialized Goal state commits, execution orchestration and evaluation generations      |
| `goals/tasks.ts`                            | Pure Task validation, revision decisions and execution reuse                           |
| `goals/evaluation.ts`                       | Bounded history compaction and reasoning; writes return through Goal Commands          |
| `goals/runtime.ts`                          | Effect-native coordination with Signal Actors                                          |
| `signals/actors.ts`                         | Monitor configuration, timers and durable occurrence delivery                          |
| `tasks/run.ts`                              | Shared preparation, readiness and confirmation workflow for Goal and Signal executions |
| `tasks/run-state.ts`, `delegation/state.ts` | Phase-specific durable schemas, including recovery prerequisites                       |
| `delegation/actor.ts`                       | External session submission, recovery, status and approval delivery                    |

`SignalRunActor` keeps its public name and Actor identity, but its implementation belongs to `tasks/run.ts`: a Goal can propose execution without creating a Signal. The application host supplies adapters, while core builds these internal capabilities. They do not each need an externally configured Layer.

`GoalRuntime.reconcile(goal, subscriber)`, `editSignal` and `deactivate` return Effects. They run in the caller's Fiber, preserving Clock, cancellation and typed errors. Reconciliation restores subscriptions from stored Signal records; it does not replace a desired Signal list from a planning result. `GoalReasoner.plan/compact` and its tool/transcript callbacks also return Effects. `goals/evaluation.ts` composes history reads, compaction, mailbox acknowledgements and planning directly; it contains no Promise runtime. Retired callbacks are interrupted before starting a new ask, and queued commands still pass mailbox generation checks.

`decideTaskOperation` computes a tagged decision without I/O. The Goal mailbox persists its resulting tasks before cancelling an old proposal or spawning a Run. Persisted proposals reserve execution even before their Run exists. Each new proposal records the Task revision: editing an unstarted proposal allows replacement, while submitted/running/uncertain execution remains reserved across revisions. Older proposals without a revision are conservatively reused.

Run and Delegation states use phase-specific Schema unions with the existing `status` discriminator. Checking/confirmation/execution require a prepared Task; active Delegations require a session and completed Delegations require a result. An uncertain submission may legitimately have no session. Both writes and Actor recovery validate these prerequisites; invalid restored records stop the Actor without replacing data or submitting external work.

`SystemOneClient.systemOne`, Signal/Goal screening, execution readiness and `TaskPreparation.prepare/ready` return Effects. SDK rejection becomes `DecisionError`; Task preparation and readiness use `TaskPreparationError`. The System One adapter forwards the fiber's AbortSignal to SDK requests and retry waits. Internal Task reasoning stays in the calling fiber; no extra domain `runPromise` is needed. External infrastructure still supplies SystemOneClient, Models, MemoryRecall and ExternalAgents; runtime builds InternalAgent and TaskPreparation.

`ExternalAgent.submit/status/resume/wait/respond` return Effects with `ExternalAgentError`. Delegation composes these directly; infrastructure adapters forward Fiber cancellation to RPC/CLI calls and own process release through `ExternalAgentsLive.layer`. The domain port has no `close` method. Local interruption does not prove external cancellation, so ambiguous submissions are never automatically retried. Recovery and parent reattachment replay saved completed/failed/cancelled/unknown outcomes before looking up an executor; terminal failure is persisted before notifying its parent.

`MemoryRecall.search/expand`, `SignalExtractor` and `DescriptionInitializer` return Effects too. The memory port and `MemoryRecallError` belong to `context/memory.ts`, shared by Goals and Task reasoning; the former Goal-specific `GoalMemory` interface is removed. Model output is decoded before accessing it, so malformed or null output becomes a tagged description/detection error. Descriptions retain their fixed-identity policy; extraction filters unknown IDs and deduplicates candidate matches.

`MemoryIntegration` adapts its Promise backend to the domain port, forwarding fiber cancellation to the actual fetch and consolidated-memory fallback. The backend's 15-second request timeout remains in force. Memory capture/drain retains its explicit Promise boundary for durable handoff semantics. IM admission and summary workflows now compose Effects directly.

`reasoning/agent-callbacks.ts` owns the shared SDK Promise bridge for Goal tool/history/transcript callbacks and InternalAgent memory tools. Each invocation captures the caller's Effect Context and binds callback cancellation to its own scope. Release aborts callbacks before waiting for the Agent to become idle, avoiding deadlock on an outstanding mailbox acknowledgement. Callback defects bypass SDK tool-error recovery and reach Actor supervision; expected model/validation failures use `GoalReasoningError`. Goal End completes a Deferred that interrupts the evaluation; Behavior restart/stop also interrupts the scoped work. Compaction advances the durable history boundary only after all summary pages succeed and the mailbox acknowledges the write.

Remote Actor replies are awaited through `pipeToSelf`, keeping Goal End/UserMessage and Signal Configure/Tick responsive. Goal Signal operations share a per-Behavior semaphore to preserve revision order; queued edits recheck Goal/generation validity before sending. Signal occurrences remain pending until a Goal accepts them or a Run acknowledges initialization. Failed delivery is retried from the persisted occurrence; an in-flight set prevents duplicate concurrent sends within one Behavior.

Runtime readiness settles on success, typed failure, defect or cancellation, including shutdown before the startup Fiber begins. Shutdown attempts every integration and subsequent cleanup phase even if earlier finalizers defect, preserving failure causes and release order. Integrations with no Actor service dependencies can register an empty Context.

The Context implementation maintains the current public snapshots and emits changes. Public records contain `path`, `description`, `state`, and `messages`; implementation handles and behavior remain private.

Context Actors declare their public definition alongside their Command Schema:

```ts
class ChatActor extends ContextActor.Service<ChatActor, ChatServices>()("app/Chat", {
  command: ChatCommand,
  context: defineContext({ identity: "Work conversation", state: ChatState, message: ChatMessage }),
}) {
  static readonly layer = Layer.effect(
    ChatActor,
    Effect.gen(function* () {
      const registry = yield* ContextRegistry;
      return ChatActor.of({
        receive: (command, actor) =>
          registry.set({
            path: contextPath(actor),
            description: "Work conversation",
            state: command.state,
            messages: command.messages,
          }),
      });
    }),
  );
}
```

`ContextActor.Service` adds `ContextRegistry` to the required services. Its `of` wraps the lifecycle and automatically registers the definition before `started` or any Command runs, including after supervision restarts. The definition is also available as `ChatActor.context` for inspection and test fixtures. Business Actors only write through `registry.set(record)`; they do not register separately. The generic Actor package has no dependency on this package.

Registration selects Schemas directly, without a Context type string or central catalog. Repeated registration of the same implementation supports restart and passivation; replacing a path with a different implementation is rejected. Stopping an Actor retains its public record and persistent data.

Optional private `capture` and `signalSource` behavior informs the core Context processor. Changes notify automatically after actual public content updates; registration alone does not notify. Public records and notifications are detached snapshots, Schemas strip undeclared state/message fields, and a fixed description cannot be replaced after initialization.

Storage reads and writes also cross a detached-copy boundary: adapters cannot mutate the registry by retaining a loaded record or save argument. Memory capture deduplication starts only after the sink accepts the handoff; a failed handoff remains eligible on a later change.

Each integration reads its own typed configuration from ConfigProvider. `AsterRuntime` activates installed integrations, whose capabilities register their root Actors through `ActorSystem.spawn`. Descendants are created by their parents through `ActorContext.spawn` or `spawnContextChild` for virtual public path segments.

Public paths default to the Actor hierarchy without `/user`, decoding virtual segments created by `spawnContextChild`. Use `contextPath(actor)` to obtain the public path. `contextSpawnOptions("/delegations/id")` supplies an independent public path when spawning. This is carried by the generic runtime's spawn metadata; the Actor runtime does not interpret Context paths. Children spawned through a Context Actor inherit the parent's public path, including an explicitly overridden parent path. `spawnContextChild(actor, "me/id", ChildActor)` appends multiple public path segments while creating one direct runtime child.

Change notifications include `stateChanged`, computed from the validated previous and next public state (creation counts as a change). Message-only and description-only writes still notify subscribers but set this flag to false. The source processor requires this flag before System One screening; memory capture and UI updates retain their own policies.

Source is grouped by capability: `config/, context/, signals/, goals/, delegation/, decisions/`. Consumers use the package root exports rather than internal paths.

Task preparation, readiness and human confirmation are coordinated by SignalRunActor. DelegationActor owns one external Task execution and its durable session/run handle. External adapters implement submit/status/resume/wait/respond. The internal Agent model is selected with `config.agent.model`; it performs extraction, description initialization and Task preparation using the shared Agent package.

ApprovalQueueActor owns the persistent `/approvals` Context, with `signalSource: false`. Decisions are stored before delivery, addressed by normalized runtime Actor path and request ID, and retained until the receiving Actor acknowledges durable receipt. This acknowledgement is distinct from external approval delivery and task completion. The owning Actor records external delivery errors; ambiguous responses are not automatically repeated.

Goal API mutations wait for the owning Goal's durable acknowledgement. User messages persist both history and evaluation intent before acceptance; End persists completion before acceptance. An unavailable router or acknowledgement timeout returns an application error, and a timeout does not prove that the mutation was rejected.

Delegation reports a tagged `ExecutionOutcome` instead of collapsing business failure into a generic Error. Run preserves confirmed failure/cancellation separately from an uncertain external outcome, and replays every terminal result on recovery or parent reattachment. New terminal writes persist `outcomeText`; existing records can recover their text from terminal events. Explicit new execution proposals may replace confirmed failed/cancelled work; uncertain work remains reserved and is never automatically resubmitted.

Approval input validation checks provider-declared options and single-choice limits before resolving the queue entry. Providers explicitly declare whether custom answers are allowed. Invalid input leaves the request pending for correction; one-question free text is normalized into keyed answers before persistence. External delivery failures still retain the conservative uncertain outcome rather than being automatically resent.

Signal recovery decodes the complete persisted state, including occurrence delivery flags, revisions and timer fields, before configuration writes or execution. Invalid recovery data reaches supervision and remains unchanged.

Goal mailbox implementation separates the typed `GoalState`, history/working-window commits, task execution and recovery, and evaluation scheduling. Evaluation phases keep their generation and cancellation Deferred together. Task and Signal tools have operation-specific Schemas, decoded at the Agent boundary; task decisions remain pure Results. Run and Delegation persist typed transitions that derive their state and event together, with phase prerequisites checked before commit.

See [Reactive application API](../../docs/reactive-api-design.md) for the shared Schema contracts, AtomRpc queries/mutations and committed SSE invalidation bridge. Core owns the public operations and query-key mapping; the host owns transports and the browser owns its Reactivity instance.
