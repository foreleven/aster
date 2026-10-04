# Core

Goal reasoning uses a stable personal-assistant system policy in `goals/agent-prompt.ts`. Each turn reads its frozen Goal definition, summary, turn ID, admission purpose and Context overview through the paginated `goal_current` tool. Task, Signal, Context and history tools provide further details. Current facts are not embedded in the system prompt. Catalogue `aster.goal.v10` identifies this tool/policy contract; durable request conflicts remain fenced instead of silently resubmitting accepted work.

Goal planning has no fixed whole-run time limit. Goal End and runtime shutdown still interrupt the owning Fiber, drain SDK callbacks and release the Agent session. Interrupted durable exchanges retain their request identity for reconciliation. History compaction retains a three-minute limit per page.

Durable Goal planning passes `config.goals.contextTokens` and `reserveTokens` to Pi's native compaction policy (defaults 200,000 and 8,192 tokens; compaction threshold 191,808 tokens, capped by the model context window). The native transcript is compacted within the original request before further generation. The isolated-run byte guard does not run on durable sessions: GoalHistory compaction alone cannot shrink Pi's retained conversation. Catalogue `aster.goal.v6` freezes this changed policy; existing failed evaluations remain auditable and a new retry uses the new policy.

This package owns the Aster domain: configuration schemas/validation, Contexts, Signal and Goal actors, delegation lifecycle, description policy, and state-change processing. Its package dependencies are Actor, Agent and Effect. Goal prompts, planning tools, Signal extraction policy and execution-readiness decisions belong here; Agent encapsulates pi.

`AsterRuntime.layer({ integrations })` owns the shared domain graph, integration activation, root Actors, Context reactions, readiness and shutdown. It exposes `api` (Context reads, Goals, approvals and diagnostics) and `ready`. Infrastructure supplies `DurableContext`, `GoalHistoryStore`, `Models`, `SystemOneClient`, `ExternalAgents` and MemoryBackend; runtime builds Memory capture/recall, internal reasoning, Task preparation and Goal services. Integrations register typed activation capabilities through `RuntimeIntegrations`, using `defineIntegration` to capture their Actor services. Only runtime activates these capabilities.

`GoalSettings`, `signalSettings` and `internalAgentSettings` read module-owned Config declarations. `ConfigLocation` carries source locations, not domain configuration. `parseConfig` remains a standalone validation utility; the application does not use it as a service bag. `makeGoalRuntime` receives narrow Goal settings, a Signal command port, a reasoner and history storage. Core does not load YAML, open persistence files, start CLI tools or import Lark/memory implementations. See [runtime design](../../docs/runtime-design.md).

Runtime also owns `/user/personal`, whose public Context is `/personal`. Its mailbox accepts versioned business requests and atomically records pending input plus ordered message before acknowledging. Stable request IDs replay the original receipt across restarts; altered payloads conflict. `api.personal` provides snapshot reads and message acceptance, with `GetPersonal` and `SendPersonalMessage` RPCs. Optional configured Personal processing uses a read-only durable Pi session, persists replies and run attempts, and proposes Goal messages through a persisted outbox. Explicit retry and Goal-message commands retain request identities and receiver receipts. Signal/Task/Approval mutation capabilities and the final durable backend adapter seams remain pending. It never subscribes to ContextChange.

Goal, Signal and Run owners publish filtered business progress through atomic source outboxes. The internal `/notifications` Actor recovers and delivers these events to Personal with stable identities, bounded attempts and durable receipts. Personal validates committed source evidence and records ordered `ProgressEvent` messages; ordinary Context changes never become Personal inputs. Host-owned causal budgets and durable admission counts bound automatic Personal/Goal follow-up, while exhausted outcomes remain visible. Goal Task reservations retain their causal identity for recovery before Run creation. See [implementation status](../../docs/personal-agent-implementation.md) for verified coverage and remaining runtime work.

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

Goal screening policy `goal-relevance-v3` uses ten zero-indexed rubric levels (`0..9`), normalizes continuous scores by dividing by nine, and retains the `0.7` admission threshold. The prompt requires an evidenced link to the Goal's specific outcome or responsibility; unverified connections score at most three. Shared terminology, owners, urgency and overdue bugs do not establish project identity. Goal planning independently verifies admitted evidence within its existing evaluation and uses `ignored` with no mutations when no input has a verified link. These are model instructions, not a deterministic semantic guarantee. Expected request failures emit `goal.screening.failed` and append a screening audit record before propagating the error to the owning Actor. Failed records have `admitted: false`, an error and a placeholder zero score; that zero is not a model judgment. Defects and interruption remain outside screening decisions. Operator retry remains explicit through `RetryScreening`.

`ExternalAgent.submit/status/resume/wait/respond` return Effects with `ExternalAgentError`. Delegation composes these directly; infrastructure adapters forward Fiber cancellation to RPC/CLI calls and own process release through `ExternalAgentsLive.layer`. The domain port has no `close` method. Local interruption does not prove external cancellation, so ambiguous submissions are never automatically retried. Recovery and parent reattachment replay saved completed/failed/cancelled/unknown outcomes before looking up an executor; terminal failure is persisted before notifying its parent.

`MemoryRecall.search/expand`, `SignalExtractor` and `DescriptionInitializer` return Effects too. The memory port and `MemoryRecallError` belong to `context/memory.ts`, shared by Goals and Task reasoning; the former Goal-specific `GoalMemory` interface is removed. Model output is decoded before accessing it, so malformed or null output becomes a tagged description/detection error. Descriptions retain their fixed-identity policy; extraction filters unknown IDs and deduplicates candidate matches.

The agentmemory adapter in infra adapts its Promise backend to the domain port, forwarding fiber cancellation to the actual fetch and consolidated-memory fallback. The backend's 15-second request timeout remains in force. Memory capture/drain retains its explicit Promise boundary for durable handoff semantics. IM admission and summary workflows now compose Effects directly.

`reasoning/agent-callbacks.ts` owns the shared SDK Promise bridge for Goal tool/history/transcript callbacks and InternalAgent memory tools. Each invocation captures the caller's Effect Context and binds callback cancellation to its own scope. Release aborts callbacks before waiting for the Agent to become idle, avoiding deadlock on an outstanding mailbox acknowledgement. Callback defects bypass SDK tool-error recovery and reach Actor supervision; expected model/validation failures use `GoalReasoningError`. Goal End completes a Deferred that interrupts the evaluation; Behavior restart/stop also interrupts the scoped work. Compaction advances the durable history boundary only after all summary pages succeed and the mailbox acknowledges the write.

Remote Actor replies are awaited through `pipeToSelf`, keeping Goal End/UserMessage and Signal Configure/Tick responsive. Goal Signal operations share a per-Behavior semaphore to preserve revision order; queued edits recheck Goal/generation validity before sending. Signal occurrences remain pending until a Goal accepts them or a Run acknowledges initialization. Failed delivery is retried from the persisted occurrence; an in-flight set prevents duplicate concurrent sends within one Behavior.

Runtime readiness settles on success, typed failure, defect or cancellation, including shutdown before the startup Fiber begins. Shutdown attempts every integration and subsequent cleanup phase even if earlier finalizers defect, preserving failure causes and release order. Integrations with no Actor service dependencies can register an empty Context.

The Context implementation maintains the current public snapshots and emits changes. Public records contain `path`, `description`, `state`, and `messages`; implementation handles and behavior remain private.

Versioned snapshots also contain `revision`. New owner commands use `registry.commit(record, { expectedRevision })`: a stale write returns `ContextConflict`, schema failures return `ContextValidationError`, and storage failures return `ContextCommitError` without publishing a change. A failed storage path is fenced until Actor registration recovers its authoritative snapshot. Legacy unversioned snapshots read as revision zero. The implicit-revision `set` API has been removed; every commit and description initialization supplies an observed revision; see [Personal Agent implementation](../../docs/personal-agent-implementation.md) for the remaining end-to-end requirements.

`config.personal.model` enables Personal processing. Runtime supplies the processor to `/user/personal`; the Actor durably binds each attempt to one accepted input, keeps later messages queued, and atomically commits its reply, cursor, and proposed Goal-message/Signal-command outbox intents. Model tools only read public Contexts; the mailbox delivers committed operations through domain command ports. `SendPersonalGoalMessage` also accepts explicit, versioned operations. Goal receipts and idempotent history projection make lost acknowledgements recoverable without another input. Failed model attempts require `RetryPersonalInput`; running attempts recover with their existing identity. Without model configuration, the inbox and explicit Goal/Signal command APIs remain available. `ApplyPersonalSignal` creates or fully updates a `personal--<slug>` Signal, including active state and immediate/once/cron timing. The Signal mailbox enforces ownership, expected Context revision, exact request deduplication and configured executors; definition and receipt commit together. Each resulting Run requires confirmation. Delivery attempts persist before sending, and the UI can reconcile unknown results with the same request identity. Explicit `RespondPersonalApproval` operations use the same outbox and atomically commit queue decisions with deduplication receipts; model results cannot propose approval decisions. Durable SystemOne reactions, ownerless agent-runtime migration and execution policy remain pending.

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
          registry
            .commit(
              {
                path: contextPath(actor),
                description: "Work conversation",
                state: command.state,
                messages: command.messages,
              },
              { expectedRevision: command.expectedRevision },
            )
            .pipe(
              Effect.asVoid,
              // Reply to caller conflicts in the command protocol; storage faults enter supervision.
              Effect.catchTag("ContextConflict", (conflict) =>
                command.replyTo.tell({
                  _tag: "Conflict",
                  expectedRevision: conflict.expectedRevision,
                  actualRevision: conflict.actualRevision,
                }),
              ),
              Effect.orDie,
            ),
      });
    }),
  );
}
```

`ContextActor.Service` adds `ContextRegistry` to the required services. Its `of` wraps the lifecycle and automatically registers the definition before `started` or any Command runs, including after supervision restarts. The definition is also available as `ChatActor.context` for inspection and test fixtures. New business operations write through `registry.commit(record, { expectedRevision })`; they do not register separately or infer a missing write revision. Derived state uses the same detached snapshot for computation and compare-and-swap. The generic Actor package has no dependency on this package.

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

## Durable Context boundary

`ContextRegistry` owns domain definitions, validation, and the public index interface. It delegates canonical snapshots, compare-and-swap commits, uncertain-write fencing, recovery, and ordered commit notifications to the injected `DurableContext`. `LocalDurableContext` is the model-free implementation. `LocalDurableContext.make` accepts Effect persistence capabilities; `fromStore` adapts the existing synchronous `ContextStore` driver at the native I/O boundary. The standalone `makeContextRegistry(store?)` helper selects Local for tests and compatibility. Production provides a backend Layer explicitly.

A complete Context record is the transaction unit: owner state, ordered messages, and receipts/outbox records inside that state commit together. Accepted commits drain through persistence and publication on cancellation; a writer cancelled while waiting for the semaphore does not start. Typed storage failures and driver defects both fence the affected path, while original defects remain defects. Recovery validates the owner schema and refuses missing or regressed snapshots before releasing the fence. Notifications from uncertain-write reconciliation suppress automatic source evaluation; durable reaction delivery remains separate work.

`makeDurableContext(kind, persistence)` is the shared canonical kernel used by Local and concrete Pi storage adapters. Drivers provide lazy typed load/save Effects; the kernel owns revisions, detached snapshots, serialized commits, no-op suppression and recovery fencing. SDK transactions and resource ownership remain in infra.

`ExternalAgent.lookupSubmission` is an optional read-only admission capability. Delegation uses it after a lost submission handle or restart without a handle, passing its original Task and stable path request ID. Found handles persist before notifying Run; missing/failed/unsupported lookup retains uncertainty and never resubmits. Personal exposes `inspectDelegation` through its mailbox, typed RPC and a replay-safe model tool. The projection includes business status/results/requests and source references while excluding provider metadata and native messages.

`StartPersonalTask` accepts an exact prepared Task and executor through Personal’s durable outbox. The independent `/user/runs` root owns `/runs/personal--<sha256(requestId)>` children; it does not create a Signal. Each Run commits its frozen Task and exact-input admission receipt before acknowledgement, then checks readiness and requires user confirmation before delegation. Repeated commands return the original receipt, including after restart or completion. Structured Personal replies may propose Tasks, but cannot approve them.

`ResumePersonalRun` accepts an explicit, versioned request for a failed or uncertain execution. Personal saves it in its outbox; the existing Run owner saves exact-input admission before handing it to its Delegation. Both retain replayable receipts. Delegation observes the original handle and resumes only a reported resumable failure. An external resume marker precedes the SDK call, and the updated handle commits with its successful outcome. Interrupted/ambiguous resume calls remain unknown and cannot be repeated by using a new command ID. Pending confirmation and terminal completion cannot be bypassed.

`RequestPersonalApproval` queues a request for an existing pending Run confirmation or Delegation input/permission demand. The queue validates the source revision, current demand, queue revision and exact retry identity, then commits the derived entry and receipt together. Callers cannot supply the prompt or Actor target. Model replies may propose approvalRequests but cannot decide approvals. Revocation tombstones prevent delayed enqueue or restart from recreating withdrawn demands.

### Public Context views

Canonical Context state belongs to its owner and remains available for recovery. External reads use `ContextRegistry.project` / `publicSnapshot`: explicit schema allowlists strip private fields and unsupported messages. `contextView` declares a read policy on a ContextDefinition; integration family policies register through `registerViews` so retained children remain readable without a live Actor. Core includes policies for its own domain paths. Missing policies or invalid view data return only path, description and revision with a restricted projection marker. Policies must expose explicit nested schemas rather than opaque objects; description and selected text fields must already contain business content.

Application queries, Personal mailbox reads and model read tools share these views. Goal reasoning, Signal tool results, Task preparation/readiness and Context reactions also receive projected evidence. Memory capture projects both its initial handoff and recovered backend delivery. A queued reaction retains its original source revision. Internal owners continue reading complete canonical snapshots; projection never changes stored data. Public Goal history retains only business text and preserves the original sequence cursor when private tool/provider entries are omitted.

### Durable Context reactions

Source commits atomically retain private public-evidence envelopes. `/system-one` consumes that journal through its mailbox, including after restart without live source Actors. It freezes the target catalogue when screening starts, retains that input across interrupted planning, commits decisions before sending, and records attempts/receipts. Live changes are wakeups, while description and Memory follow-up remain separate. Public diagnostics omit frozen evidence and catalogues.

Goal Intent admission saves the exact input and receipt before idempotently projecting timeline evidence. Signal reaction admission saves its exact input, receipt and occurrence in one versioned commit. Both reject stale target versions and conflicting identities; exact retries replay receipts. Unknown delivery acknowledgements allow bounded retries of the same input, followed by explicit reconciliation. Rejected decisions remain visible and require a new screening decision rather than silently changing the frozen command. Public processing inspection and versioned recovery commands allow explicit retry of failed screening or uncertain delivery with the original frozen input. Source journal compaction is not implemented.

### Explicit Channel publication

A Task-producing Signal may declare `action: { _tag: "PublishResult", channelPath, identity }`. Completion freezes the exact result and action into the Run's `writeback` in the same commit. Publication has its own approval displaying the destination, sending identity and content; Task confirmation never authorizes it. Removing an action from a Signal affects future Runs only. Goal-owned Signals deliver evidence to GoalActor and reject publication actions.

The Run mailbox verifies the persisted ApprovalQueue decision, saves authorization, then saves `sending` before invoking the optional `ChannelWrites` adapter through `pipeToSelf`. It records `published`, `rejected` or `unknown` separately from Task completion, with a stable idempotency key, receipt and bounded business notifications. Recovery never resends `sending`/`unknown` operations; a lost sending acknowledgement conservatively becomes unknown even if transport was not called. Public views expose this business record. Adapters own credentials and verify the retained exact request/grant before external submission.

## Memory ownership

`MemoryBackend` supplies capture, recall, drain, and public description/retrieval/model metadata. `AsterRuntime` assembles the Memory consumer internally; hosts do not install a Memory integration or construct its Actor graph. `memory/actor.ts` owns the durable pending/captured state, duplicate suppression, periodic recovery, and two concurrent captures. Capture replies follow the pending-state commit; successful backend results are committed through the mailbox. Typed backend failures remain pending, defects reach supervision, and local interruption preserves pending work. Shutdown stops producers and Actors before joining already-admitted backend operations and releasing infrastructure. No daemon credentials or backend configuration enter core.

## Goal command boundary

The public Goal requests are `SubmitInput`, `End`, `RetryTurn`, and `RetrySignalDelivery`, with stable request IDs and durable receipts. Runtime controls activation/readiness; private mailbox messages own turn admission and atomic result application. A turn freezes inputs and read models before invoking the durable session. `finish_turn` proposes Task/Signal changes and Continue, WaitForInput, WaitForEvent or Complete. See [the implemented design](../../docs/goal-command-redesign.md) for causal budgets and legacy reconciliation.
