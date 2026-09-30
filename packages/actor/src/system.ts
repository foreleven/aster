import {
  Clock,
  Context,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  PubSub,
  Queue,
  Scope,
  Stream,
} from "effect";
import {
  actorSelectionPath,
  ActorNotFound,
  SpawnError,
  type ActorRef,
  type ActorSelection,
  type ActorSystemEvent,
  type AnyActorDefinition,
  type CommandOf,
  type RequireServices,
  type SpawnOptions,
} from "./actor.js";
import { ActorCell, type CellRuntime, type Envelope } from "./internal/cell.js";
import { commandTag } from "./internal/telemetry.js";

declare const ActorSystemAcquisitionTypeId: unique symbol;

export type ActorSystemAcquisition<Services, E = never, R = Scope.Scope> = Effect.Effect<
  ActorSystem<Services>,
  E,
  R
> & {
  readonly [ActorSystemAcquisitionTypeId]: Services;
};

type MissingLayerInputs<
  Layers extends ReadonlyArray<Layer.Layer<never, any, any>>,
  Available,
> = Layers extends readonly [
  infer First extends Layer.Layer<never, any, any>,
  ...infer Rest extends ReadonlyArray<Layer.Layer<never, any, any>>,
]
  ? | Exclude<Layer.Services<First>, Available>
    | MissingLayerInputs<Rest, Available | Layer.Success<First>>
  : never;

export class ActorSystem<Services = never> {
  private readonly topLevel = new Map<string, ActorCell>();
  // Paths address the current cell; find(ref) also checks object identity so an old
  // incarnation can never resolve to a replacement actor at the same address.
  private readonly cells = new Map<string, ActorCell>();
  private terminated = false;
  private readonly terminationDone = Deferred.makeUnsafe<void>();
  private serviceContext: Context.Context<Services>;
  private readonly cellRuntime: CellRuntime;

  private constructor(
    private readonly scope: Scope.Closeable,
    private readonly pubsub: PubSub.PubSub<ActorSystemEvent>,
    services: Context.Context<Services>,
  ) {
    this.serviceContext = services;
    // eslint-disable-next-line @typescript-eslint/no-this-alias -- The services getter must read the live system context.
    const system = this;
    this.cellRuntime = {
      select: (path) => system.select(path),
      get services() {
        return system.serviceContext;
      },
      publish: (event) => system.publish(event),
      deadLetter: (path, incarnation, command, reason) =>
        system
          .deadLetter(path, incarnation, command, reason)
          .pipe(Effect.provideContext(system.serviceContext)),
      find: (ref) => system.find(ref),
      unregister: (cell) => system.unregister(cell),
      spawnChild: (parent, name, definition, options) =>
        system.spawnAt(parent.children, parent, parent.path, name, definition, options ?? {}),
    };
  }

  /** Acquire a system whose enclosing Scope forces cancellation if graceful shutdown is skipped. */
  static make(): ActorSystemAcquisition<never> {
    return Effect.gen(function* () {
      const outer = yield* Effect.scope;
      const scope = yield* Scope.make();
      const pubsub = yield* PubSub.unbounded<ActorSystemEvent>();
      const system = new ActorSystem(scope, pubsub, Context.empty());
      yield* Scope.addFinalizer(outer, system.forceTerminate());
      return system;
    }).pipe(Effect.uninterruptible) as ActorSystemAcquisition<never>;
  }

  /** Acquire shared Layers in order; earlier outputs satisfy later inputs until system shutdown. */
  static provide<const Layers extends ReadonlyArray<Layer.Layer<never, any, any>>>(
    ...layers: Layers
  ) {
    return <Services, E, R>(
      acquire: ActorSystemAcquisition<Services, E, R> &
        ([MissingLayerInputs<Layers, Services>] extends [never] ? unknown : never),
    ): ActorSystemAcquisition<
      Services | Layer.Success<Layers[number]>,
      E | Layer.Error<Layers[number]>,
      R
    > =>
      Effect.gen(function* () {
        const system = yield* acquire;
        for (const layer of layers) {
          // Effect 4 includes CurrentMemoMap in the built Context. Keeping it lets
          // later builds reuse supplied dependencies while behavior builds fork it.
          const built = yield* Layer.buildWithScope(layer, system.scope).pipe(
            Effect.provideContext(system.serviceContext),
          );
          system.serviceContext = Context.merge(
            system.serviceContext,
            built,
          ) as Context.Context<Services>;
        }
        return system as ActorSystem<Services | Layer.Success<Layers[number]>>;
      }) as any;
  }

  get events(): Stream.Stream<ActorSystemEvent> {
    return Stream.fromPubSub(this.pubsub);
  }

  /** Read-only runtime telemetry. Command payloads and service state are never exposed. */
  inspect(options: { readonly metadata?: readonly string[] } = {}) {
    // Capture membership at execution time: callers may reuse this Effect across spawns/stops.
    return Effect.suspend(() =>
      Effect.forEach([...this.cells.values()], (cell) =>
        Queue.size(cell.mailbox).pipe(
          Effect.map((mailboxSize) => ({
            path: cell.path,
            parent: cell.parent?.path ?? "/user",
            incarnation: cell.incarnation,
            // Metadata is private unless the host explicitly selects diagnostic keys.
            metadata: Object.fromEntries(
              (options.metadata ?? [])
                .filter((key) => Object.hasOwn(cell.options.metadata ?? {}, key))
                .map((key) => [key, structuredClone(cell.options.metadata![key])]),
            ),
            status: cell.status,
            phase: cell.status === "running" ? cell.phase : cell.status,
            pendingEffects: cell.pendingEffects,
            failures: cell.failures,
            lastError: cell.lastError,
            processing: cell.processing,
            currentCommand: cell.currentCommand,
            mailboxSize: Math.max(0, mailboxSize),
            processed: cell.processed,
            restarts: cell.restartTimes.length,
            lastActivity: cell.lastActivity,
          })),
        ),
      ),
    );
  }

  private publish(event: ActorSystemEvent): Effect.Effect<void> {
    return PubSub.publish(this.pubsub, event).pipe(Effect.asVoid);
  }

  private deadLetter(
    path: string,
    incarnation: string,
    command: unknown,
    reason: string,
  ): Effect.Effect<void> {
    return Clock.currentTimeMillis.pipe(
      Effect.flatMap((now) =>
        this.publish({
          _tag: "DeadLetter",
          target: path,
          incarnation,
          commandTag: commandTag(command),
          reason,
          timestamp: new Date(now).toISOString(),
        }),
      ),
    );
  }

  select(path: string): ActorSelection {
    const absolute = actorSelectionPath(path);
    return {
      path: absolute,
      resolve: () =>
        Effect.suspend(() => {
          const cell = this.cells.get(absolute);
          return cell?.status === "running"
            ? Effect.succeed(cell.ref as ActorRef<unknown>)
            : Effect.fail(new ActorNotFound(absolute));
        }),
    };
  }

  private find<Command>(ref: ActorRef<Command>): ActorCell | undefined {
    const cell = this.cells.get(ref.path);
    return cell?.ref === ref ? cell : undefined;
  }

  private unregister(cell: ActorCell): void {
    if (this.cells.get(cell.path) === cell) this.cells.delete(cell.path);
    if (cell.parent === undefined && this.topLevel.get(cell.name) === cell) {
      this.topLevel.delete(cell.name);
    }
  }

  /** Register a root; initialization failures are supervised after this Effect returns its ref. */
  spawn<Definition extends AnyActorDefinition>(
    name: string,
    definition: Definition & RequireServices<Definition, Services>,
    options: SpawnOptions = {},
  ): Effect.Effect<ActorRef<CommandOf<Definition>>, SpawnError> {
    return this.spawnAt(this.topLevel, undefined, `/user`, name, definition, options);
  }

  private spawnAt<Definition extends AnyActorDefinition>(
    registry: Map<string, ActorCell>,
    parent: ActorCell | undefined,
    parentPath: string,
    name: string,
    definition: Definition,
    options: SpawnOptions,
  ): Effect.Effect<ActorRef<CommandOf<Definition>>, SpawnError> {
    return Effect.gen({ self: this }, function* () {
      // eslint-disable-next-line no-control-regex -- Actor names must reject control characters.
      if (!name || name === "." || name === ".." || /[/\x00-\x1f\x7f*?#:]/.test(name)) {
        return yield* Effect.fail(new SpawnError(`Invalid actor name: ${name}`));
      }
      if (this.terminated || (parent?.status !== undefined && parent.status !== "running")) {
        return yield* Effect.fail(new SpawnError("Actor system or parent is stopping"));
      }
      if (registry.has(name))
        return yield* Effect.fail(new SpawnError(`Actor already exists: ${name}`));
      const mailbox = yield* Queue.unbounded<Envelope>();
      const scope = yield* Scope.make();
      const done = yield* Deferred.make<void>();
      const cell = new ActorCell(
        this.cellRuntime,
        name,
        `${parentPath}/${name}`,
        parent,
        definition,
        {
          ...options,
          metadata: Object.freeze({ ...options.metadata }),
        },
        mailbox,
        scope,
        done,
      );
      registry.set(name, cell);
      this.cells.set(cell.path, cell);
      cell.fiber = yield* Effect.forkIn(
        cell.run().pipe(Effect.provideContext(this.serviceContext)),
        scope,
      );
      yield* Effect.forkIn(
        Fiber.await(cell.fiber).pipe(
          Effect.flatMap(() => cell.finish()),
          Effect.flatMap(() => Scope.close(scope, Exit.void)),
          Effect.provideContext(this.serviceContext),
        ),
        this.scope,
      );
      return cell.ref as ActorRef<CommandOf<Definition>>;
    }).pipe(Effect.uninterruptible);
  }

  /** Await active handlers and all resource cleanup. Cancelling the owner forces actor shutdown. */
  terminate(): Effect.Effect<void> {
    return this.shutdown(false);
  }

  /** Stop one root and await its subtree, leaving other roots and shared services alive. */
  stop<Command>(ref: ActorRef<Command>): Effect.Effect<void> {
    return Effect.gen({ self: this }, function* () {
      const cell = this.find(ref);
      if (!cell) return;
      if (cell.parent)
        return yield* Effect.die(new Error("ActorSystem can only stop a root Actor"));
      yield* cell.requestStop();
      yield* Deferred.await(cell.done);
    });
  }

  private forceTerminate(): Effect.Effect<void> {
    return this.shutdown(true);
  }

  private shutdown(force: boolean): Effect.Effect<void> {
    return Effect.uninterruptibleMask((restore) =>
      Effect.gen({ self: this }, function* () {
        const alreadyTerminating = this.terminated;
        this.terminated = true;
        if (force) yield* this.interruptActors();
        if (alreadyTerminating) return yield* restore(Deferred.await(this.terminationDone));
        // One caller owns cleanup. Cancelling its graceful wait escalates to forced
        // actor shutdown before shared services close; it must never abandon waiters.
        yield* Effect.gen({ self: this }, function* () {
          const cells = [...this.topLevel.values()];
          for (const cell of cells) yield* cell.requestStop();
          yield* restore(Effect.forEach(cells, (cell) => Deferred.await(cell.done))).pipe(
            Effect.onInterrupt(() => this.interruptActors()),
          );
        }).pipe(
          Effect.ensuring(
            Scope.close(this.scope, Exit.void).pipe(
              Effect.ensuring(PubSub.shutdown(this.pubsub)),
              // Publish cleanup's Exit, including defects, so repeated terminate calls settle.
              Effect.onExit((exit) => Deferred.done(this.terminationDone, exit)),
            ),
          ),
        );
      }),
    );
  }

  private interruptActors(): Effect.Effect<void> {
    return Effect.gen({ self: this }, function* () {
      const active = [...this.cells.values()];
      for (const cell of active) yield* cell.requestStop();
      // Interrupt the whole tree concurrently: a parent's finalizer waits for its children.
      yield* Effect.forEach(
        active,
        (cell) => (cell.fiber === undefined ? Effect.void : Fiber.interrupt(cell.fiber)),
        { concurrency: "unbounded", discard: true },
      );
    });
  }
}
