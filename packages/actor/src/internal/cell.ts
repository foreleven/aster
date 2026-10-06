import { randomUUID } from "node:crypto";
import {
  Cause,
  Clock,
  Context,
  Deferred,
  Duration,
  Effect,
  Exit,
  Fiber,
  Layer,
  Option,
  Queue,
  Result,
  Scope,
} from "effect";
import {
  actorSelectionPath,
  ActorStartupError,
  type ActorBehavior,
  type ActorContext,
  type ActorRef,
  type ActorSignal,
  type ActorSystemEvent,
  type ActorSelection,
  type AnyActorDefinition,
  type FailureSummary,
  type PersistentActorBehavior,
  type SpawnError,
  type SpawnOptions,
  type SupervisionDirective,
} from "../actor.js";
import { ActorPersistence } from "../persistence.js";
import { ActorRefImpl } from "./ref.js";
import { PersistentState } from "./persistent-state.js";
import { commandTag, failureSummary } from "./telemetry.js";

export type Envelope =
  | { readonly _tag: "Command"; readonly value: unknown }
  | { readonly _tag: "Signal"; readonly value: ActorSignal }
  | { readonly _tag: "Failure"; readonly cause: Cause.Cause<unknown> };

type AnyBehavior = ActorBehavior<any, any> | PersistentActorBehavior<any, any, any, any>;

export interface CellRuntime {
  readonly select: (path: string) => ActorSelection;
  readonly services: Context.Context<any>;
  readonly publish: (event: ActorSystemEvent) => Effect.Effect<void>;
  readonly deadLetter: (
    path: string,
    incarnation: string,
    command: unknown,
    reason: string,
  ) => Effect.Effect<void>;
  readonly find: (ref: ActorRef<any>) => ActorCell | undefined;
  readonly unregister: (cell: ActorCell) => void;
  readonly spawnChild: (
    parent: ActorCell,
    name: string,
    definition: AnyActorDefinition,
    options?: SpawnOptions,
  ) => Effect.Effect<ActorRef<any>, SpawnError>;
}

/** Stable cell lifetime owns refs, mailbox and children; instanceScope owns replaceable behavior. */
export class ActorCell {
  private readonly startup = Deferred.makeUnsafe<void, ActorStartupError>();
  readonly incarnation = randomUUID();
  readonly children = new Map<string, ActorCell>();
  readonly watchers = new Set<ActorCell>();
  // Reverse links let a short-lived watcher detach from long-lived targets.
  // Both sets belong to the cell, so DeathWatch survives behavior restarts.
  readonly watching = new Set<ActorCell>();
  readonly restartTimes: Array<number> = [];
  readonly ref: ActorRefImpl<any>;
  status: "running" | "stopping" | "stopped" = "running";
  receiveTimeoutMs: number | undefined;
  instanceScope: Scope.Closeable | undefined;
  behavior: AnyBehavior | undefined;
  phase: "starting" | "running" | "restarting" = "starting";
  pendingEffects = 0;
  failures = 0;
  lastError: string | undefined;
  processing = false;
  processed = 0;
  currentCommand: string | undefined;
  lastActivity = new Date().toISOString();
  terminalCause: FailureSummary | undefined;
  fiber: Fiber.Fiber<void, never> | undefined;

  constructor(
    readonly system: CellRuntime,
    readonly name: string,
    readonly path: string,
    readonly parent: ActorCell | undefined,
    readonly definition: AnyActorDefinition,
    readonly options: SpawnOptions,
    readonly mailbox: Queue.Queue<Envelope>,
    readonly scope: Scope.Closeable,
    readonly done: Deferred.Deferred<void>,
  ) {
    this.ref = new ActorRefImpl(
      path,
      this.incarnation,
      (command) => this.deliver(command),
      system,
      Deferred.await(this.startup),
    );
  }

  deliver(command: unknown): Effect.Effect<void> {
    return Effect.suspend(() => {
      const status = this.status;
      const accepted =
        status === "running" &&
        Queue.offerUnsafe(this.mailbox, {
          _tag: "Command",
          value: command,
        });
      return accepted
        ? Effect.void
        : this.system.deadLetter(
            this.path,
            this.incarnation,
            command,
            status === "running" ? "mailbox closed" : status,
          );
    });
  }

  signal(signal: ActorSignal): Effect.Effect<void> {
    return Effect.sync(() => {
      if (this.status === "running")
        Queue.offerUnsafe(this.mailbox, { _tag: "Signal", value: signal });
    });
  }

  fail(cause: Cause.Cause<unknown>): Effect.Effect<void> {
    return Effect.sync(() => {
      if (this.status === "running") Queue.offerUnsafe(this.mailbox, { _tag: "Failure", cause });
    });
  }

  requestStop(cause?: FailureSummary): Effect.Effect<void> {
    return Effect.gen({ self: this }, function* () {
      if (this.status !== "running") return;
      this.status = "stopping";
      yield* Deferred.interrupt(this.startup);
      this.terminalCause = cause;
      while (true) {
        const item = yield* Queue.poll(this.mailbox);
        if (Option.isNone(item)) break;
        const envelope = item.value;
        if (envelope._tag === "Command") {
          yield* this.system.deadLetter(
            this.path,
            this.incarnation,
            envelope.value,
            "actor stopping",
          );
        }
      }
      yield* Queue.shutdown(this.mailbox);
    }).pipe(Effect.uninterruptible);
  }

  private context(): ActorContext<any, any> {
    // eslint-disable-next-line @typescript-eslint/no-this-alias -- Preserve the actor instance inside generators and getters.
    const cell = this;
    return {
      select: (path) => cell.system.select(actorSelectionPath(path, cell.path)),
      self: cell.ref,
      path: cell.path,
      metadata: cell.options.metadata ?? {},
      spawn: (name, definition, options) => cell.system.spawnChild(cell, name, definition, options),
      stop: (ref) =>
        Effect.gen(function* () {
          const child = [...cell.children.values()].find((candidate) => candidate.ref === ref);
          if (child === undefined)
            return yield* Effect.die(new Error("Can only stop a direct child"));
          yield* child.requestStop();
        }),
      stopSelf: () => cell.requestStop(),
      watch: (ref) =>
        Effect.gen(function* () {
          const target = cell.system.find(ref);
          if (target === undefined || target.status === "stopped") {
            const cause =
              target?.terminalCause ??
              (ref instanceof ActorRefImpl && ref.terminated ? ref.terminalCause : undefined);
            yield* cell.signal({ _tag: "Terminated", ref, cause });
            return;
          }
          target.watchers.add(cell);
          cell.watching.add(target);
        }),
      child: (name) => Effect.sync(() => cell.children.get(name)?.ref),
      children: () => Effect.sync(() => [...cell.children.values()].map((child) => child.ref)),
      receiveTimeout: (duration) =>
        Effect.sync(() => {
          const milliseconds = Duration.toMillis(duration);
          if (milliseconds <= 0) throw new Error("ReceiveTimeout must be positive");
          cell.receiveTimeoutMs = milliseconds;
        }),
      pipeToSelf: (effect, toCommand) =>
        Effect.gen(function* () {
          if (cell.instanceScope === undefined)
            return yield* Effect.die(new Error("Actor instance is unavailable"));
          // Fork into the Behavior Scope: restart/stop cancels this work before replacement.
          cell.pendingEffects++;
          yield* Effect.forkIn(
            Effect.matchCauseEffect(effect, {
              onSuccess: (value) => cell.ref.tell(toCommand({ _tag: "Success", value })),
              onFailure: (cause) => {
                const expected = Cause.findErrorOption(cause);
                return !Result.isSuccess(Cause.findDefect(cause)) &&
                  !Cause.hasInterrupts(cause) &&
                  Option.isSome(expected)
                  ? cell.ref.tell(toCommand({ _tag: "Failure", error: expected.value }))
                  : Cause.hasInterruptsOnly(cause)
                    ? Effect.void
                    : cell.fail(cause);
              },
            }).pipe(
              Effect.catchCause((cause) =>
                Cause.hasInterruptsOnly(cause) ? Effect.void : cell.fail(cause),
              ),
              Effect.ensuring(
                Effect.sync(() => {
                  cell.pendingEffects--;
                }),
              ),
            ),
            cell.instanceScope,
          );
        }),
    };
  }

  private initialize(): Effect.Effect<ActorContext<any, any>, unknown> {
    return Effect.gen({ self: this }, function* () {
      const instanceScope = yield* Scope.make();
      this.instanceScope = instanceScope;
      // Each behavior build forks the system's Layer memo map: shared providers
      // survive, while behavior-local acquisitions close and rebuild on restart.
      const built = yield* Layer.buildWithScope(this.definition.layer, instanceScope).pipe(
        Effect.provideContext(this.system.services),
      );
      const behavior = Context.get(built, this.definition as any) as AnyBehavior;
      this.behavior = behavior;
      let context: ActorContext<any, any> = this.context();
      if (this.definition.actorKind === "persistent") {
        const persistent = behavior as PersistentActorBehavior<any, any, any, any>;
        const store = Context.get(this.system.services, ActorPersistence);
        const persistentState = yield* PersistentState.recover(persistent, store, this.path);
        context = persistentState.context(context);
      }
      if (behavior.started !== undefined)
        yield* behavior.started(context as any).pipe(Effect.provideContext(this.system.services));
      return context;
    });
  }

  private closeInstance(): Effect.Effect<void> {
    return Effect.gen({ self: this }, function* () {
      if (this.instanceScope !== undefined) {
        const scope = this.instanceScope;
        this.instanceScope = undefined;
        yield* Scope.close(scope, Exit.void);
      }
      this.behavior = undefined;
      this.receiveTimeoutMs = undefined;
    });
  }

  private supervise(cause: Cause.Cause<unknown>): Effect.Effect<boolean> {
    return Effect.gen({ self: this }, function* () {
      const closed = yield* Effect.exit(this.closeInstance());
      if (this.status !== "running") return false;
      const effectiveCause = Exit.isFailure(closed) ? closed.cause : cause;
      const summary = failureSummary(effectiveCause);
      this.failures++;
      this.lastError = summary.message;
      this.phase = "restarting";
      let directive: SupervisionDirective = this.options.supervision?.(summary) ?? "restart";
      const now = yield* Clock.currentTimeMillis;
      while (this.restartTimes.length > 0 && this.restartTimes[0]! <= now - 60_000) {
        this.restartTimes.shift();
      }
      if (directive === "restart" && this.restartTimes.length >= 5) directive = "stop";
      if (directive === "restart") {
        this.restartTimes.push(now);
        yield* this.system.publish({
          _tag: "ActorRestarting",
          path: this.path,
          incarnation: this.incarnation,
          cause: summary,
          timestamp: new Date(now).toISOString(),
        });
        const exponent = Math.min(this.restartTimes.length - 1, 7);
        const delay = Math.min(10_000, 100 * 2 ** exponent) * (0.8 + Math.random() * 0.4);
        yield* Effect.sleep(delay);
        return this.status === "running";
      }
      if (directive === "escalate" && this.parent !== undefined)
        yield* this.parent.fail(effectiveCause);
      yield* this.requestStop(summary);
      return false;
    });
  }

  run(): Effect.Effect<void> {
    const loop = Effect.gen({ self: this }, function* () {
      while (this.status === "running") {
        const initialized = yield* Effect.exit(
          this.initialize().pipe(
            Effect.onExit((exit) =>
              Deferred.done(
                this.startup,
                Exit.asVoid(
                  Exit.mapError(exit, (cause) => new ActorStartupError({ path: this.path, cause })),
                ),
              ),
            ),
          ),
        );
        if (Exit.isFailure(initialized)) {
          if (!(yield* this.supervise(initialized.cause))) break;
          continue;
        }
        this.phase = "running";
        const context = initialized.value;
        while (this.status === "running") {
          const next = yield* Effect.exit(
            this.receiveTimeoutMs === undefined
              ? Queue.take(this.mailbox)
              : Effect.raceFirst(
                  Queue.take(this.mailbox).pipe(Effect.map((value): Envelope | null => value)),
                  Effect.sleep(this.receiveTimeoutMs).pipe(Effect.as(null)),
                ),
          );
          if (Exit.isFailure(next)) {
            if (this.status === "running") yield* this.supervise(next.cause);
            break;
          }
          const envelope = next.value;
          if (this.status !== "running") {
            if (envelope?._tag === "Command") {
              yield* this.system.deadLetter(
                this.path,
                this.incarnation,
                envelope.value,
                "actor stopping",
              );
            }
            break;
          }
          if (envelope === null) {
            if (this.children.size === 0 && (yield* Queue.size(this.mailbox)) === 0) {
              yield* this.requestStop();
            }
            continue;
          }
          if (envelope._tag === "Failure") {
            if (!(yield* this.supervise(envelope.cause))) break;
            break;
          }
          this.processing = true;
          this.currentCommand =
            envelope._tag === "Command" ? commandTag(envelope.value) : envelope.value._tag;
          this.lastActivity = new Date().toISOString();
          const outcome = yield* Effect.exit(
            Effect.suspend(() =>
              envelope._tag === "Command"
                ? this.behavior!.receive(envelope.value, context as any)
                : (this.behavior!.receiveSignal?.(envelope.value, context as any) ?? Effect.void),
            ),
          );
          this.processing = false;
          this.processed++;
          this.lastActivity = new Date().toISOString();
          yield* this.system.publish({
            _tag: "CommandProcessed",
            path: this.path,
            incarnation: this.incarnation,
            commandTag: this.currentCommand,
            success: Exit.isSuccess(outcome),
            timestamp: this.lastActivity,
          });
          this.currentCommand = undefined;
          if (Exit.isFailure(outcome)) {
            if (!(yield* this.supervise(outcome.cause))) break;
            break;
          }
        }
        if (this.status !== "running") break;
      }
    });
    return loop.pipe(Effect.ensuring(this.finish()));
  }

  finish(): Effect.Effect<void> {
    return Effect.gen({ self: this }, function* () {
      if (this.status === "stopped") return;
      yield* Deferred.interrupt(this.startup);
      // Children may still be completing a handler. Keep the parent's behavior
      // resources alive until its descendants have fully stopped.
      for (const child of this.children.values()) yield* child.requestStop();
      for (const child of this.children.values()) yield* Deferred.await(child.done);
      const closed = yield* Effect.exit(this.closeInstance());
      if (Exit.isFailure(closed) && this.terminalCause === undefined) {
        this.terminalCause = failureSummary(closed.cause);
      }
      this.status = "stopped";
      this.ref.terminated = true;
      this.ref.terminalCause = this.terminalCause;
      this.parent?.children.delete(this.name);
      this.system.unregister(this);
      const now = yield* Clock.currentTimeMillis;
      yield* this.system.publish({
        _tag: "ActorStopped",
        path: this.path,
        incarnation: this.incarnation,
        cause: this.terminalCause,
        timestamp: new Date(now).toISOString(),
      });
      for (const watcher of this.watchers) {
        watcher.watching.delete(this);
        yield* watcher.signal({ _tag: "Terminated", ref: this.ref, cause: this.terminalCause });
      }
      for (const target of this.watching) target.watchers.delete(this);
      this.watchers.clear();
      this.watching.clear();
    }).pipe(Effect.ensuring(Deferred.succeed(this.done, undefined).pipe(Effect.asVoid)));
  }
}
