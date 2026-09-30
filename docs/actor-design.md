# Actor Module Design

Status: accepted design; implemented in `packages/actor`.

## Boundary

The `actor` module is domain-neutral. It must not import or name Context, Channel, Signal, workspace, Lark, Agent delegation, or domain path conventions.

It owns:

- typed actor references and local delivery;
- actor paths, parent/child creation, and hierarchical supervision;
- per-actor in-memory mailboxes and serial message processing;
- scoped actor lifecycle, stopping, restart, and `ReceiveTimeout`;
- local interaction patterns such as tell, ask, and pipe-to-self;
- optional persistence abstractions and recovery lifecycle.

The core package maps domain Context paths to actor paths and defines concrete Actor protocols and persistent Messages. Persistent Actors use their ActorPath as the default persistence identity; a concrete Actor definition may override it only when its durable domain identity differs from its runtime path.

## Confirmed runtime semantics

- The runtime is local to one process and does not use distributed RPC or Effect Cluster.
- Parent actors create and supervise their children. Stopping a parent stops its active descendants.
- Each actor processes one mailbox message at a time; separate actors execute concurrently.
- Actor behavior must handle expected domain errors itself. Only defects, lifecycle failures, and persistence infrastructure failures enter supervision.
- The default supervision direction is one-for-one restart with backoff and a restart limit. A parent may select a different strategy for a child when spawning it.
- An actor implementation may enable `ReceiveTimeout` in code. Timeout-based stop requires no message in progress, an empty mailbox, and no active child.
- Actor handlers do not synchronously wait on another actor's ask; asynchronous results are piped back as messages.
- Persistence follows Akka's separation: incoming commands are transient, while events and snapshots are durable. Reliable delivery is opt-in per scenario.

## Actor definitions and behaviors

Actor implementations are Effect service definitions, not mutable subclasses instantiated with `new`. The actor module provides `Actor.Service`, analogous to `Context.Service`; an implementation class is the service key and exposes a static Layer that builds its Behavior:

```ts
class WorkerActor extends Actor.Service<WorkerActor, WorkerServices>()("@app/WorkerActor", {
  command: WorkerCommand,
}) {
  static readonly layer = Layer.effect(
    WorkerActor,
    Effect.gen(function* () {
      const dependency = yield* Dependency;

      return WorkerActor.of({
        receive: (command, context) => Effect.void,
        started: (context) => Effect.void,
        receiveSignal: (signal, context) => Effect.void,
      });
    }),
  );
}
```

`receive` is required. `started` and `receiveSignal` have no-op defaults and may be omitted. Every behavior method receives that ActorCell's typed `ActorContext<Command, Services>` explicitly; per-actor Context is not installed as a system-wide Effect service. `Services` is the minimum Runtime capability set required by the Actor's own Layer and any children it may spawn. The Layer resolves and captures its direct services from the ActorSystem Runtime and may acquire scoped resources, so the built Behavior is environment-closed: `receive` and `receiveSignal` return `Effect<void>`, while `started` may return `Effect<void, unknown>`. The runtime builds the Layer once in each Actor instance Scope and extracts the implementation service. Restart closes that Scope and rebuilds the same Layer to obtain a fresh Behavior.

`PersistentActor.Service` extends the same definition model with Schema-defined Event and State types, recovery, persistence operations, Snapshots, and compaction semantics. The Schema form of `Service` infers Command, Event, and State types from the Schemas supplied at definition time. Its Layer returns `initialState`, required `receive`, and pure `applyEvent`, plus the same optional lifecycle methods; `Service.of` attaches the Event and State Schemas to the Behavior. Command, Event, and State types are inferred from these Schemas rather than supplied as explicit generics. `ActorPersistence` is automatically included in every PersistentActor's required Services, so spawning one without a persistence Layer is a static type error. Guardians and runtime coordinators use ordinary `Actor.Service`; domain Context actors normally use `PersistentActor.Service`. Temporary ask reply references use a one-shot Deferred without building an Actor Behavior.

Persistent behavior methods receive a `PersistentActorContext<Command, Event, State>`, which extends ordinary `ActorContext` with a dynamic read-only `state` getter and `persist`, `persistAll`, and `saveSnapshot` Effects. After a successful persist, another `context.state` read in the same `receive` observes the new State. The Behavior never owns or assigns State directly; the runtime owns recovery, write ordering, and state replacement.

A Persistent Behavior may provide a pure synchronous `persistenceId(path)` function, evaluated after Layer construction and before recovery. If omitted, the normalized ActorPath is used directly. An override is responsible for returning a stable, collision-free identity; it cannot access Effect services or change between restarts.

```ts
class ChatActor extends PersistentActor.Service<ChatActor, ChatServices>()("@app/ChatActor", {
  command: ChatCommand,
  event: ChatEvent,
  state: ChatState,
}) {
  static readonly layer = Layer.effect(
    ChatActor,
    Effect.succeed(
      ChatActor.of({
        initialState: ChatState.empty,
        receive: (command, context) => context.persist(toEvent(command)),
        applyEvent: (state, event) => ChatState.apply(state, event),
      }),
    ),
  );
}
```

All Actor implementations define their Commands with Schemas. Ordinary actors use `Actor.Service<WorkerActor, WorkerServices>()("@app/WorkerActor", { command: WorkerCommand })` to infer their Command type; the optional second generic declares dependencies needed by the Actor and its children. Command type aliases are derived with `typeof WorkerCommand.Type`. A Command Schema in either form supplies a TypeScript type only: local Commands are not decoded, validated, or persisted by the Actor runtime. For response Commands, the actor package provides both a `ReplyTo<Response>` type alias for `ActorRef<Response>` and a `ReplyTo<Response>()` Schema helper, so a field can be written as `replyTo: ReplyTo<number>()`. The helper checks the reference shape when decoded explicitly; the response type is a TypeScript type and cannot be checked at runtime. The Schema argument is required for both ordinary and persistent Actor definitions.

When no Snapshot or Event exists, persistent recovery starts from the Behavior's `initialState`. Events are encoded and decoded with the Event Schema; Snapshot State uses the State Schema. Commands remain local in-memory values and require no persistence Schema. Current immutable State is owned by the Actor runtime and exposed read-only through the `PersistentActorContext` passed to behavior methods. `applyEvent` is a pure synchronous function that returns the next State. The runtime replaces current State only after successful persistence or during recovery. It never calls `receive` while replaying Events. Restart builds a fresh Behavior service. Journal and Snapshot failures enter supervision rather than appearing as domain errors that behavior must interpret.

`persistAll(events)` is atomic for one `PersistenceId`. The SQLite journal appends the batch in one transaction with contiguous sequence numbers, or appends none of it. Only after the transaction commits does the runtime invoke `applyEvent` for each Event in sequence. If state application defects after commit, the committed Events remain authoritative; supervision restarts the Actor and replay attempts to rebuild State from them.

The SQLite Journal assigns a monotonically increasing `sequenceNumber` within each `PersistenceId`; this is required Journal metadata and the Event stream position, not a business Event ID. It is used for Snapshot boundaries, replay cursors, Event cleanup, and per-stream ordering. Events do not require an additional generic identifier. A scenario that needs a globally sortable ID, idempotency key, external message ID, or domain Event ID may place a UUID v7 (or another domain-chosen value) in its own Event schema; that value does not replace `sequenceNumber`.

The initial Actor module does not provide a generic Schema-migration DSL. Journal and Snapshot payloads are decoded with the `PersistentActor`'s Effect Schema; a recovery decode failure is a persistence recovery failure rather than a business Command error. A concrete actor may keep backward-compatible Schema definitions or migrate its SQLite data outside the generic Actor API. Versioned envelopes and migration hooks remain future extensions driven by an actual use case.

Persistence infrastructure is one deep Effect Context service, `ActorPersistence`, rather than separate public Journal and SnapshotStore plugin APIs. The runtime uses it to recover the latest Snapshot and later Events, append Events atomically against an expected sequence number, save Snapshots, remove covered Events and superseded Snapshots, and retry incomplete cleanup. PersistentActor implementations do not access this service or SQLite directly.

`SqliteActorPersistence.layer({ path })` is the default local implementation supplied to the ActorSystem Runtime. It owns the database resource and schema setup within its Layer Scope. Keeping Journal and Snapshot operations behind one service centralizes their consistency boundary without reproducing Akka's persistence-plugin system in the initial local module.

A `PersistentActor` explicitly calls `saveSnapshot()` when its own semantics indicate a useful recovery point. The Snapshot stores the Actor's exact Schema-encoded State together with the current journal sequence number. Recovery loads the latest valid Snapshot and replays only later Events. The initial runtime has no global every-N-events snapshot policy. After a Snapshot is durably saved, the runtime automatically deletes journal Events through that Snapshot's sequence number; the Actor API does not expose a separate event-deletion operation.

Snapshot persistence and Event cleanup are one logical operation but two durable steps. A cleanup failure never rolls back the saved Snapshot; the operation enters supervision. On restart, recovery uses that Snapshot and retries deletion of Events through its sequence number before invoking `started()`. This keeps the Snapshot authoritative while preventing a failed cleanup from silently becoming permanent journal growth.

The Snapshot Store retains at most the latest Snapshot for each `PersistenceId`. After a new Snapshot is durably stored, the runtime removes the previous Snapshot and the Event prefix covered by the new one. Failures while removing superseded data are cleanup failures handled by the same restart-and-retry recovery rule. The initial API has no multi-generation Snapshot retention setting.

Deleting the Event prefix covered by a Snapshot never resets that `PersistenceId`'s sequence counter. If the Snapshot covers sequence `N`, the next persisted Event is `N + 1`, even when no earlier Event rows remain. Snapshot compaction therefore changes storage size without changing the durable stream position.

Each Behavior Layer is built in a dedicated Effect Scope. Persistent recovery completes before the optional `started` hook, and mailbox processing begins afterward. Scope finalizers handle cleanup on stop or restart; there is no separate imperative `stopped` callback.

Internally, one unexported `ActorCell` represents a live ActorRef incarnation. It owns the Mailbox Queue, consumer Fiber, child registry, stable ActorRef, and a cell Scope. The consumer Fiber is attached to that Scope with Effect structured concurrency. The current Behavior Layer and service have a separate instance Scope inside the cell lifecycle. Restart closes and rebuilds only that instance Scope and Behavior; the cell, Mailbox, Fiber, children, reference, and incarnation remain stable. Final stop closes the cell lifecycle and its descendants. Reusing a terminated path creates a new ActorCell and incarnation.

Commands and runtime Signals execute serially for an Actor, but they enter through separate typed methods. The initial `ActorSignal` protocol contains `Terminated { ref, cause? }`; normal stopping has no cause, while terminal supervised failure includes an observable failure summary. The base `receiveSignal` implementation ignores it. An actor that calls `context.watch` overrides `receiveSignal` when it needs to react to the watched actor's termination. Runtime Signals are not added to the actor's domain `Command` union.

`receive` and `receiveSignal` are environment-closed and expose no typed error channel. Implementations must turn expected business failures into explicit behavior such as a reply, persisted Event, log entry, or deliberate ignore. Building the Behavior Layer and `started` may fail during lifecycle initialization, but their error type is not part of message delivery because callers cannot handle it after asynchronous spawn. The runtime retains the full `Cause<unknown>` for diagnostics and supervision. An uncaught throw, `Effect.die`, invariant violation, or persistence infrastructure failure is likewise an Actor failure and enters supervision.

Hierarchy and interaction operations live on the `ActorContext` passed to Behavior methods, including `context.spawn`, child stopping and watching, `context.self`, actor path access, and `context.pipeToSelf`. Delivery is `ActorRef.tell(message)`.

`ActorContext.pipeToSelf(effect, toCommand)` starts an asynchronous Effect in the current Actor instance Scope and returns after registering that work. Successful values and expected typed failures are mapped into a domain Command and offered to `self`, so the result is handled later through the same serial Mailbox rather than mutating Actor state from a concurrent Fiber. A defect in that background Effect fails the current Actor and enters supervision instead of becoming a business Command. Restart or stop closes the instance Scope and interrupts outstanding pipe-to-self work; interruption caused by that Scope closure neither invokes supervision nor enqueues a result for the replacement instance.

The public API is Effect-first. Operations that observe or mutate runtime state return lazy Effects rather than executing eagerly or returning Promises: `ActorRef.tell`, `ActorContext.spawn`, `stop`, `stopSelf`, `watch`, `child`, and `children`. Expected operation failures such as an invalid or duplicate child name use typed Effect errors that actor behavior must handle. Immutable identity data such as `context.self`, `context.path`, and `ref.path` are pure values. Promise execution belongs only at the application boundary.

## Child creation and identity

Children are created through `ActorContext`, for example `context.spawn(name, WorkerActor)`. The context owns the parent path, live-child registry, supervision relationship, and child Scope. An Actor implementation class is a definition; merely referring to it or building its Layer outside `spawn` does not register an Actor in the Actor System.

`spawn` completes once the ActorCell is registered, its name is occupied, and its ActorRef is available. The definition's Layer build, persistent recovery, and `started` hook then run asynchronously in the cell Fiber; Commands arriving meanwhile remain queued. Initialization failures enter supervision rather than failing the already-completed spawn. The spawn error channel is limited to failures that prevent cell registration, such as invalid or duplicate names. The initial module has no generic `awaitReady`; a caller that needs readiness confirmation uses an explicit domain Command and reply.

A child name is unique among its parent's currently live children. Its logical `ActorPath` is `parentPath / name`. After the child has fully terminated, the name may be reused. Each actor incarnation also receives an opaque identity carried by `ActorRef`, so a stale reference to a terminated child cannot deliver to a later child that happens to reuse the same path.

Actor names are non-empty strings, cannot equal `.` or `..`, and may not contain `/`, `*`, `?`, `#`, `:`, or control characters. This keeps every spawned path addressable by exact local selection. Invalid names fail at `spawn`; the Actor module does not URL-encode, case-normalize, or otherwise canonicalize them. Any domain-specific path normalization remains the responsibility of the consuming Context layer.

`ActorPath`, incarnation identity, and `PersistenceId` serve different purposes. The path identifies a logical runtime location, the incarnation identity protects references from path reuse, and `PersistenceId` selects durable history. By default, a `PersistentActor` uses its `ActorPath` as its `PersistenceId`. Therefore, creating a new persistent incarnation at a reused path intentionally recovers that path's previous history. Only special cases that need a different durable identity may override the default.

## ActorSystem and top-level actors

`ActorSystem.make()` does not require a user-defined RootActor. The resulting system exposes `system.spawn(name, ActorType)` for top-level actors at `/user/{name}`; descendants are created through `ActorContext.spawn`. An internal `/user` root owns top-level registration and supervision but is not an application Actor definition or public reference. The `/system` subtree remains reserved for runtime-owned actors.

The ActorSystem owns a Scope and service Context used to build Actor Layers; it does not create a separate ManagedRuntime or escape to `Effect.runPromise`. `ActorSystem.make()` begins as a scoped acquisition of `ActorSystem<never>`. The dedicated acquisition-pipe operator `ActorSystem.provide(...layers)` builds application Layers inside the ActorSystem Scope, extends its Runtime Context, and accumulates their output types into `ActorSystem<Services>`. This operator is distinct from ordinary `Effect.provide`: the supplied resources must remain alive for the system lifetime, and the resulting system value must retain the provided service type for later spawn checks.

The service environment is frozen when acquisition completes. `ActorSystem.provide` transforms only the acquisition Effect and is not a method on the returned system value; Layers cannot be added after the system becomes usable. All initializations and restarts therefore observe the same Runtime Context. Closing the enclosing Scope coordinates shutdown of every top-level Actor, all descendants, supplied Layer resources, and runtime-owned resources.

`system.terminate()` requests graceful shutdown: it waits for current handlers, stops descendants before closing parent Behavior resources, then closes supplied Layers and runtime resources. Concurrent callers await the same completion, including cleanup defects. Interrupting the caller that owns graceful shutdown forces actor cancellation and still completes resource cleanup. Closing the enclosing Scope enters this shutdown path in forced mode, interrupting blocked handlers. The API does not add a separate `awaitTermination`; callers that intentionally want fire-and-forget termination may fork the termination Effect themselves.

```ts
const system = yield * ActorSystem.make().pipe(ActorSystem.provide(Database.layer, Client.layer));

const ref = yield * system.spawn("worker", WorkerActor);
```

An Actor definition declares a minimum Services capability set containing both its Layer's direct inputs and the requirements of children it may spawn. Its `ActorContext<Command, Services>` uses that set to type-check child spawn. `ActorSystem.spawn` then checks the top-level Actor's Services against `ActorSystem<Services>` before the Layer is built from that Runtime. `ActorRef<Command>` carries only the Command type. Layer construction, persistence recovery, `started`, `receive`, `receiveSignal`, and Scope finalizers execute on the system Runtime. Restart creates a new instance Scope and rebuilds the Behavior while continuing to use that Runtime.

An Actor implementation exposes `static readonly layer = Layer.effect(...)`; there is no separate public `ActorFactory` or `make` protocol. `context.spawn(name, ActorType)` locates and builds that Layer in the new instance Scope. Normal application code provides shared services once around the scoped ActorSystem program.

## Internal ownership and Effect Layers

`system.ts` owns registration, exact-path selection, shared service acquisition, observation and system shutdown. Internal modules remain outside the package exports:

- `internal/cell.ts` owns one actor's mailbox loop, children, DeathWatch, supervision and Behavior Scope.
- `internal/ref.ts` owns delivery references and the one-shot ask protocol, also reused by test probes.
- `internal/persistent-state.ts` owns recovered state, journal sequence, commit-before-apply ordering and snapshot cleanup for one Behavior instance.
- `internal/telemetry.ts` projects command tags and failure summaries without retaining command payloads.

The host supplies shared dependencies with `ActorSystem.provide`; persistent actors additionally require an external `ActorPersistence` Layer. Choosing SQLite versus in-memory persistence, database locations and application services belongs to the host. The actor package owns actor startup/restart/stop and builds each definition's Behavior Layer itself. Callers do not assemble or start internal cells, refs or journals.

Effect 4's built Context carries `CurrentMemoMap`. The system preserves this Context across ordered provider builds, allowing common Layers to be reused. Behavior builds fork that memo map: inherited providers remain shared, while Behavior-local resources are acquired afresh and released on restart. The runtime needs no separate dependency cache. Resources that children must use across a parent's restart belong in shared providers, because children survive replacement of the parent's Behavior Scope.

## Supervision ownership

Supervision policy belongs to the parent-child relationship. A parent selects it through `ActorContext.spawn` options; the child does not decide how its own failure is supervised. When no option is supplied, the Actor System uses its default one-for-one policy: restart the failed child with backoff, stop it after the restart limit is exceeded, and surface that terminal outcome for observation.

The initial supervision directives are `restart`, `stop`, and `escalate`. `restart` reconstructs the failed child and recovers it; `stop` terminates it without another attempt; `escalate` delegates the failure to the parent supervisor. The module does not provide `resume`, because continuing with an in-memory instance after a defect may preserve corrupted state and conflicts with recovery-from-persistence semantics.

Backoff duration, restart count, and the counting window belong to the Actor System's default supervision strategy rather than to individual `spawn` calls. A parent may choose a directive for a child, but the initial API does not expose per-child backoff or retry-limit tuning. The default strategy applies its configured restart policy and converts an exhausted restart budget into `stop`.

The default restart budget is five restarts within one minute. Retry delay uses exponential backoff from 100 milliseconds to a maximum of 10 seconds, with factor 2 and 20 percent jitter. Exhausting the budget converts the outcome to `stop` and produces `Terminated` with the final failure cause.

Supervision and DeathWatch remain distinct. Spawning automatically places a child under its parent's supervision, but does not inject a termination message into the parent's behavior. An actor explicitly registers lifecycle interest with `context.watch(childRef)`. Watch registrations survive Behavior restart and are detached when either cell finally stops. The initial API uses Akka's `watch` name and does not provide a `watchWith` command-mapping variant.

When a child exceeds its restart limit and finally stops, its watchers receive the same `Terminated` Signal used for an ordinary stop, with a failure cause attached. The runtime does not introduce a separate `ChildFailed` business protocol.

An Actor implementation opts into `ReceiveTimeout` through its `ActorContext`, for example `context.receiveTimeout(30_000)`. The timer starts after recovery and `started()` complete. Processing any Command or Runtime Signal resets it. A timeout can stop the Actor only when no handler is running, the in-memory Mailbox is empty, and there are no active children; otherwise the runtime defers the stop and continues normal processing. Timeout shutdown uses the ordinary Scope-closing lifecycle and does not inject a business Command.

`context.stop(childRef)` uses Akka-style stopping. The ActorCell is marked stopping and its Queue is shut down, so queued Commands are not drained and are routed to `DeadLetters`; the currently running handler is allowed to complete rather than being interrupted immediately. The runtime then recursively stops active descendants, closes the child's instance and cell Scopes, runs finalizers, and publishes `Terminated` to registered watchers. Stopping is asynchronous and is not a synchronous completion confirmation. A stopping child continues to occupy its name until termination. The initial API has no graceful mailbox-drain operation.

Following ownership boundaries, `ActorContext.stop(ref)` accepts only a direct child and returns after requesting the stop. An Actor requests its own termination with `context.stopSelf()`. External code may call `ActorSystem.stop(rootRef)` to stop one root and await its subtree while leaving sibling roots and shared services alive. The system rejects attempts to stop a descendant directly. Closing the ActorSystem Scope stops the entire tree.

Closing or interrupting the outer ActorSystem Scope is a stronger structured-concurrency cancellation boundary: it interrupts Actor consumer Fibers and runs their finalizers instead of waiting indefinitely for handlers to finish naturally. This forced system shutdown is distinct from an ordinary actor stop request.

Supervised restart preserves the ActorRef, ActorPath, and incarnation. The Command whose processing failed is not automatically replayed, while other Commands already queued remain in the Mailbox. The old Behavior Layer's Scope is closed, a fresh Behavior is built, and a PersistentActor recovers from its Snapshot and subsequent Events before processing the retained Mailbox. Messages sent during the restart remain queued until the new instance is ready.

`pipeToSelf` work belongs to the current Behavior Scope. Expected errors are mapped back into Commands; defects enter supervision. Closing or replacing that Scope suppresses late results and failures from its asynchronous work, so they cannot affect the replacement Behavior.

Restart replaces only the parent's behavior instance; its active children survive with their ActorRefs, Paths, Mailboxes, Scopes, and supervision relationship intact. The new parent instance can access those existing children through its `ActorContext`. A same-name `spawn` during reinitialization cannot create a second live child: the runtime reports the name collision (or lets the implementation look up the existing child). Only final parent stop recursively stops descendants.

`ActorContext` exposes `child(name)` for one existing child and `children()` for the current live-child set. `spawn(name, ActorType)` only creates a new child; if that name is already live it reports a typed name-collision failure rather than returning the existing reference. Because a generic context cannot infer a child's Command type from `ActorRef<unknown>`, the parent owns the typed protocol boundary when retaining or recovering child references.

## Mailbox

Mailbox is an Actor Model concept; Effect `4.0.0-rc.117` provides the underlying `Queue` primitive rather than a public `Mailbox` module. Each Actor uses an internal `Queue.unbounded<Envelope<Command>>()`, where an Envelope carries either a Command or Runtime Signal for serial processing. `ActorRef` has only the Queue's `Enqueue` capability, while the Actor loop exclusively owns `Dequeue`; neither capability is exposed by the public Actor API.

The initial module provides no configured capacity, overflow strategy, or public mailbox-selection API. `ActorRef.tell` uses `Queue.offer`, completes after enqueueing, and does not wait for command processing. If the Queue has already ended, the runtime routes the Command to `DeadLetters` while `tell` itself remains fire-and-forget. Actor stop uses `Queue.shutdown`, which discards buffered messages and releases the consumer fiber. Backlog visibility and source-side flow control are operational concerns rather than alternate Mailbox behavior in this version.

`ActorRef.tell` is fire-and-forget and has no delivery-error channel. Sending to a stopped or stopping actor, or through a reference whose incarnation has expired, routes the command to the Actor System's observable `DeadLetters` event stream. It does not report a business error or retry automatically.

The ActorSystem exposes runtime observability as a read-only `Stream<ActorSystemEvent>` backed internally by an unbounded Effect `PubSub`. The event union contains `CommandProcessed`, `DeadLetter`, `ActorRestarting`, and `ActorStopped`. These best-effort events are for logs, diagnostics, and monitoring; they are neither persisted nor delivered as business Commands. Subscribers observe only events published after subscription, and the absence or slowness of subscribers never blocks Actor processing. `Terminated` remains a separate Runtime Signal delivered only to explicit DeathWatch subscribers.

`DeadLetter` contains only observability metadata: target ActorPath, target incarnation, an optional string `_tag`, reason, and UTC timestamp. It does not retain or serialize the original Command payload, because Commands may contain credentials or sensitive external content. A concrete Actor that needs payload diagnostics emits its own deliberately redacted domain log.

Commands that support a response explicitly declare a temporary `replyTo: ActorRef<Response>` field in their closed TypeScript protocol. Commands that do not need a response omit it. `ActorRef.ask` creates a temporary reply reference and invokes a command factory with that reference; it does not implicitly add `replyTo` to arbitrary Commands. The reply reference is interaction-only and is never persisted as an Event or State field.

The temporary ask reference is an internal one-shot ActorRef backed by an Effect `Deferred<Response>`, not a complete ActorCell. It lives under `/system/ask/{uuid}` using Node’s `crypto.randomUUID()`; this temporary address requires uniqueness, not time ordering. It has no Actor object, Mailbox, supervision relationship, or persistence. The first reply completes the Deferred; duplicate replies, timeout replies, and replies after caller cancellation go to `DeadLetters`. Timeout or cancellation closes the temporary reference and releases its resources; it does not cancel work already dispatched to the receiver.

## Testing

Tests use the production ActorCell, Mailbox, and supervision implementation rather than a separate deterministic scheduler. Effect `TestClock` controls ReceiveTimeout, ask timeout, and supervision backoff. `InMemoryActorPersistence.layer` replaces SQLite while preserving the same persistence contract. A scoped `ActorTestKit` provides a lightweight `ActorTestProbe<Command>` whose Queue-backed ActorRef records messages and whose Effect API can await the next message or assert that no message arrives during a virtual-time interval.

## Local Actor selection

Expose `ActorContext.select(path)` and `ActorSystem.select(path)` as path-based selections. `selection.resolve()` obtains the current live ActorRef or explicitly reports that no Actor is registered at the destination. Support exact local absolute and relative paths initially; context-relative paths are anchored to the calling Actor, while the system entry point is anchored at the ActorSystem root. Persist a normalized absolute Actor path for cross-restart addressing. Existing ActorRef retains its incarnation-specific identity.

Selection does not automatically spawn or restore Actors, add remote addressing or wildcard matching, or provide durable delivery. Approval processing uses persisted destinations and request IDs with acknowledgement/deduplication above this primitive. A public Context path is not necessarily the runtime Actor path. See [Task delegation design](task-delegation-design.md).

## Local dashboard observation

`ActorSystem.inspect()` reads live cells each time its Effect executes, including when a caller reuses the same Effect. It reports runtime path, parent, incarnation, lifecycle phase, current command tag, mailbox size, handled count, pending `pipeToSelf` effects, recent restart count and last failure. Metadata is empty by default; `inspect({ metadata: [key] })` returns detached copies of explicitly selected fields. The actor package does not interpret those keys. `AsterRuntime` selects `contextPath` and projects it into the dashboard response. Inspection does not expose command payloads or service environments, and the host is responsible for selecting safe diagnostic metadata. `CommandProcessed` events report completion (including failure) without payloads. These observations do not change supervision, persistence, or Signal screening.

The local application retains the most recent 200 system events in memory and serves observations alongside public Context records through `/api/dashboard`, under the existing loopback Host/Origin checks. The dashboard separates live runtime instances from persisted Contexts and business execution status. Runtime history resets on application restart; persistent business messages do not.
