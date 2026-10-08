import { registerGoalQueries } from "../goals/view.js";
import { registerTaskQueries } from "../tasks/view.js";
import { registerSignalQueries } from "../signals/queries.js";
import { ContextsActor } from "../context/queries/actor.js";
import { ContextCaptures } from "../memory/capture.js";
import { DurableContext } from "../context/store.js";
import { coreContextViews } from "./context-views.js";
import { ContextQueries } from "../context/queries/routes.js";
import { MemoryActor, memoryView } from "../memory/actor.js";
import { taskCapture } from "../tasks/view.js";
import { ApplicationError } from "../operations.js";
import { SystemOneActor } from "../reactions/actor.js";
import { ReactionPolicy, makeReactionPolicy } from "../reactions/policy.js";
import { GoalScreeningStore } from "../goals/screening/decision.js";
import { TasksRootActor } from "../tasks/root.js";
import { type ActorSystemEvent, ActorSystem } from "@aster/actor";
import { RuntimeConfigurationError } from "./errors.js";

import { AgentRunner, PiStorageLease, AgentConversations } from "@aster/agent";
import {
  Cause,
  Clock,
  Context,
  ConfigProvider,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Ref,
  Option,
  Scope,
  Stream,
} from "effect";
import { ContextRegistry } from "../context/registry.js";
import { MemoryBackend } from "../memory/contracts.js";
import { GoalSettings, signalSettings } from "../config/settings.js";
import { SignalDefinitions } from "../signals/protocol.js";
import { SignalRootActor } from "../signals/root.js";
import { SystemOneClient } from "../services/system-one.js";

import { GoalsRootActor } from "../goals/root.js";
import { ExternalAgents } from "../tasks/execution/contracts.js";
import { ApprovalQueueActor } from "../approvals/actor.js";

import { RuntimeIntegrations, type IntegrationHandle } from "./integration.js";

type RuntimeDiagnostics = {
  readonly phase: "starting" | "ready" | "failed" | "stopping";
  readonly events: readonly ActorSystemEvent[];
};

type ActorServices =
  | AgentRunner
  | MemoryBackend
  | ContextCaptures
  | ContextQueries
  | AgentConversations
  | GoalSettings
  | ContextRegistry
  | SignalDefinitions
  | SystemOneClient
  | ExternalAgents;

const acquireRuntime = Effect.gen(function* () {
  const registry = yield* ContextRegistry;
  yield* registry.views.register([...coreContextViews, memoryView]);
  const settings = yield* GoalSettings;
  const definitions = yield* SignalDefinitions;
  const decisions = yield* SystemOneClient;
  if (
    (settings.definitions.some((goal) => goal.slug !== "personal") || definitions.length) &&
    decisions.configured === false
  )
    return yield* new RuntimeConfigurationError({
      message: "Signals and Goals require config.system-one",
    });
  const conversations = yield* AgentConversations;
  yield* Effect.all([registerGoalQueries(), registerTaskQueries(), registerSignalQueries()]);
  const memoryBackend = yield* MemoryBackend;
  yield* (yield* ContextCaptures).register([taskCapture(conversations)]);
  const modules = (yield* RuntimeIntegrations).installed();
  const reactionPolicy = yield* makeReactionPolicy({
    client: decisions,
    screening: Option.getOrUndefined(yield* Effect.serviceOption(GoalScreeningStore)),
  });
  const shared = Context.pick(
    DurableContext,
    MemoryBackend,
    ContextCaptures,
    AgentRunner,
    ContextQueries,
    AgentConversations,
    GoalSettings,
    ContextRegistry,
    SignalDefinitions,
    SystemOneClient,
    ExternalAgents,
  )(yield* Effect.context<ActorServices | DurableContext>());
  // Integration environments are captured by their own Layers. Never inject an ambient Scope.
  const actorServices = modules
    .reduce(
      (context, module) => Context.merge(context, module.services),
      shared as Context.Context<any>,
    )
    .pipe(
      Context.omit(Scope.Scope),
      Context.add(Clock.Clock, yield* Clock.Clock),
      Context.add(ConfigProvider.ConfigProvider, yield* ConfigProvider.ConfigProvider),
      Context.add(ReactionPolicy, reactionPolicy),
      Context.add(GoalSettings, settings),
    );
  const workScope = yield* Effect.acquireRelease(Scope.make(), (scope, exit) =>
    Scope.close(scope, exit),
  );
  const system = yield* ActorSystem.make().pipe(
    ActorSystem.provide(Layer.succeedContext(actorServices)),
    Effect.provideService(Scope.Scope, workScope),
  );
  const handles: { phase: "source" | "consumer"; handle: IntegrationHandle }[] = [];
  const stopIntegrations = (phase: "source" | "consumer") =>
    Effect.suspend(() =>
      handles
        .filter((entry) => entry.phase === phase)
        .reduce(
          (remaining, { handle }) => handle.stop.pipe(Effect.ensuring(remaining)),
          Effect.void,
        ),
    );
  const running: {
    initialization?: Fiber.Fiber<void, never>;
  } = {};
  // Diagnostics are shared by lifecycle/event fibers and HTTP readers, outside any Actor mailbox.
  const diagnostics = yield* Ref.make<RuntimeDiagnostics>({ phase: "starting", events: [] });
  const ready = yield* Deferred.make<void, Error>();
  yield* Effect.addFinalizer((exit) =>
    Effect.gen(function* () {
      yield* Ref.update(diagnostics, (state): RuntimeDiagnostics => ({
        ...state,
        phase: "stopping",
      }));
      // Every phase is a finalizer: one defect must not skip later cleanup. Effect
      // retains all failure causes while preserving source -> consumer -> storage order.
      yield* (running.initialization ? Fiber.interrupt(running.initialization) : Effect.void).pipe(
        // The startup Fiber may be cancelled before it begins and installs onExit.
        Effect.ensuring(Deferred.interrupt(ready)),
        Effect.ensuring(stopIntegrations("source")),
        Effect.ensuring(stopIntegrations("consumer")),
        Effect.ensuring(system.terminate()),
        Effect.ensuring(memoryBackend.drain),
      );
    }).pipe(Effect.ensuring(Scope.close(workScope, exit))),
  );
  const requiredRoots = new Set([
    "/user/contexts",
    "/user/memory",
    "/user/system-one",
    "/user/approvals",
    "/user/signals",
    "/user/tasks",
    "/user/goals",
  ]);
  yield* Stream.runForEach(system.events, (event) =>
    Effect.gen(function* () {
      const failed = event._tag === "ActorStopped" && requiredRoots.has(event.path);
      const current = yield* Ref.updateAndGet(diagnostics, (state) => ({
        phase: failed && state.phase !== "stopping" ? ("failed" as const) : state.phase,
        events: [...state.events.slice(-199), event],
      }));
      if (failed && current.phase !== "stopping") {
        yield* Deferred.fail(
          ready,
          new ApplicationError({
            kind: "unavailable",
            message: `Runtime owner stopped: ${event.path}`,
          }),
        );
        yield* Effect.logError({ event: "runtime.owner.stopped", path: event.path });
      }
    }),
  ).pipe(Effect.forkIn(workScope));
  // Subscribe consumer Actors before sources start. Each owns recovery and supervision.
  const contexts = yield* system.spawn("contexts", ContextsActor);
  yield* contexts.awaitStarted;
  yield* (yield* system.spawn("memory", MemoryActor)).awaitStarted;
  for (const module of modules.filter((module) => module.phase === "source")) {
    const handle = yield* module
      .activate(system)
      .pipe(Effect.provideService(Scope.Scope, workScope));
    handles.push({ phase: module.phase, handle });
  }
  yield* system.spawn("approvals", ApprovalQueueActor);
  const signalActivation = yield* Deferred.make<void>();
  const signals = yield* system.spawn("signals", SignalRootActor, {
    metadata: { signalActivation },
  });
  yield* signals.awaitStarted;
  for (const module of modules.filter((module) => module.phase === "consumer")) {
    const handle = yield* module
      .activate(system)
      .pipe(Effect.provideService(Scope.Scope, workScope));
    handles.push({ phase: module.phase, handle });
  }
  const tasks = yield* system.spawn("tasks", TasksRootActor);
  yield* tasks.awaitStarted;
  const goalActivation = yield* Deferred.make<void>();
  const goals = settings.definitions.length
    ? yield* system.spawn("goals", GoalsRootActor, { metadata: { goalActivation } })
    : undefined;
  // The root registers routing targets; each child queues work until its own startup completes.
  if (goals) yield* goals.awaitStarted;
  yield* (yield* system.spawn("system-one", SystemOneActor)).awaitStarted;
  running.initialization = yield* Effect.gen(function* () {
    yield* Deferred.succeed(signalActivation, undefined);
    yield* Effect.forEach(handles, ({ handle }) => handle.ready, { concurrency: "unbounded" });
    if ((yield* Ref.get(diagnostics)).phase === "failed")
      return yield* new ApplicationError({
        kind: "unavailable",
        message: "A core owner stopped during startup",
      });
    yield* Deferred.succeed(goalActivation, undefined);
  }).pipe(
    // Readiness is a completion contract, including defects and cancellation;
    // catching only typed errors strands waiters when startup never succeeds.
    Effect.onExit((exit) =>
      Effect.gen(function* () {
        const settledPhase = Exit.isSuccess(exit) ? "ready" : "failed";
        yield* Ref.update(diagnostics, (state): RuntimeDiagnostics =>
          state.phase === "stopping" || state.phase === "failed"
            ? state
            : { ...state, phase: settledPhase },
        );
        yield* Deferred.done(ready, exit);
      }),
    ),
    Effect.catchCause((cause) =>
      Cause.hasInterruptsOnly(cause) ? Effect.void : Effect.logError(cause),
    ),
    Effect.forkIn(workScope),
  );
  return {
    actors: system as Pick<ActorSystem, "select">,
    ready: Deferred.await(ready),
    inspect: Effect.gen(function* () {
      const actors = yield* system.inspect({ metadata: ["contextPath"] });
      return {
        actors,
        storageOwners: yield* PiStorageLease.inspect,
        ...(yield* Ref.get(diagnostics)),
      };
    }),
  };
});

export class AsterRuntime extends Context.Service<
  AsterRuntime,
  Effect.Success<typeof acquireRuntime>
>()("runtime/Aster") {
  static layer<const Layers extends readonly Layer.Layer<never, any, any>[]>(options: {
    readonly integrations: Layers;
  }) {
    const contextServices = Layer.mergeAll(
      ContextRegistry.layer,
      ContextCaptures.layer,
      ContextQueries.layer,
      GoalSettings.layer,
      RuntimeIntegrations.layer,
      Layer.effect(SignalDefinitions, signalSettings),
    );
    // Registration captures integration services; acquireRuntime activates their Actors.
    const registerIntegrations = Layer.mergeAll(Layer.empty, ...options.integrations);
    const runtimeServices = registerIntegrations.pipe(
      Layer.provideMerge(AgentRunner.layer),
      Layer.provideMerge(contextServices),
    );
    return Layer.effectContext(
      Effect.gen(function* () {
        const services = Context.pick(
          ContextRegistry,
          ContextQueries,
          AgentConversations,
        )(yield* Effect.context<ContextRegistry | ContextQueries | AgentConversations>());
        return Context.add(services, AsterRuntime, yield* acquireRuntime);
      }),
    ).pipe(Layer.provide(runtimeServices));
  }
}
