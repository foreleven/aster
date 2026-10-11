import { SignalDefinitions } from "../signals/settings.js";
import { MemoryActor } from "../memory/actor.js";
import { ActorSystem, type ActorSystemEvent } from "@aster/actor";
import { AgentConversations, DurableHarness } from "@aster/agent/harness";
import { QueryExecution } from "../queries/execution.js";
import { MemoryRecall } from "../memory/recall.js";
import { ContextQueries } from "../queries/registry.js";
import { DurableContext } from "../context/store.js";
import { ContextCaptures } from "../memory/capture.js";
import { ApplicationError } from "../operations.js";
import { SystemOneActor } from "../reactions/actor.js";
import { ReactionPolicy, makeReactionPolicy } from "../reactions/policy.js";
import { TasksRootActor } from "../tasks/root.js";
import { taskCapture } from "../tasks/view.js";
import { RuntimeConfigurationError } from "./errors.js";

import { AgentRunner } from "@aster/agent/agent";
import {
  Cause,
  Clock,
  Context,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Ref,
  Scope,
  Stream,
} from "effect";
import { GoalSettings } from "../goals/settings.js";
import { LanguageSettings } from "../config/language.js";
import { ContextDirectory } from "../config/context-directory.js";
import { ReactionSettings } from "../reactions/settings.js";
import { MemoryBackend } from "../memory/contracts.js";
import { SystemOneClient } from "../services/system-one.js";
import { SignalRootActor } from "../signals/root.js";

import { ApprovalQueueActor } from "../approvals/actor.js";
import { GoalsRootActor } from "../goals/root.js";
import { ExternalAgents } from "../tasks/execution/contracts.js";

import { RuntimeIntegrations, type IntegrationHandle } from "./integration.js";

type RuntimeDiagnostics = {
  readonly phase: "starting" | "ready" | "failed" | "stopping";
  readonly events: readonly ActorSystemEvent[];
};

type ActorServices =
  | AgentRunner
  | DurableHarness
  | MemoryBackend
  | ContextCaptures
  | ContextQueries
  | QueryExecution
  | MemoryRecall
  | AgentConversations
  | GoalSettings
  | LanguageSettings
  | ContextDirectory
  | ReactionSettings
  | DurableContext
  | SignalDefinitions
  | SystemOneClient
  | ExternalAgents;

const acquireRuntime = Effect.gen(function* () {
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
  const memoryBackend = yield* MemoryBackend;
  yield* (yield* ContextCaptures).register([taskCapture(conversations)]);
  const modules = (yield* RuntimeIntegrations).installed();
  const reactionPolicy = yield* makeReactionPolicy({
    client: decisions,
    language: (yield* LanguageSettings).outputLanguage,
  });
  const shared = Context.pick(
    DurableContext,
    MemoryBackend,
    ContextCaptures,
    AgentRunner,
    DurableHarness,
    ContextQueries,
    QueryExecution,
    MemoryRecall,
    AgentConversations,
    GoalSettings,
    LanguageSettings,
    ContextDirectory,
    ReactionSettings,
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
  const goals = settings.definitions.length
    ? yield* system.spawn("goals", GoalsRootActor)
    : undefined;
  // The root registers routing targets; each child queues work until its own startup completes.
  if (goals) yield* goals.awaitStarted;
  yield* (yield* system.spawn("system-one", SystemOneActor)).awaitStarted;
  running.initialization = yield* Effect.gen(function* () {
    yield* Deferred.succeed(signalActivation, undefined);
    if ((yield* Ref.get(diagnostics)).phase === "failed")
      return yield* new ApplicationError({
        kind: "unavailable",
        message: "A core owner stopped during startup",
      });
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
      ContextCaptures.layer,
      ContextQueries.layer,
      GoalSettings.layer,
      LanguageSettings.layer,
      ContextDirectory.layer,
      ReactionSettings.layer,
      RuntimeIntegrations.layer,
      SignalDefinitions.layer,
    );
    // Registration captures integration services; acquireRuntime activates their Actors.
    const registerIntegrations = Layer.mergeAll(Layer.empty, ...options.integrations);
    const runtimeServices = registerIntegrations.pipe(
      Layer.provideMerge(Layer.mergeAll(AgentRunner.layer, DurableHarness.layer)),
      Layer.provideMerge(contextServices),
      Layer.provideMerge(
        Layer.mergeAll(QueryExecution.layer, MemoryRecall.layer).pipe(
          Layer.provide(contextServices),
        ),
      ),
    );
    return Layer.effectContext(
      Effect.gen(function* () {
        const services = Context.pick(
          DurableContext,
          ContextQueries,
          QueryExecution,
          MemoryRecall,
          AgentConversations,
        )(
          yield* Effect.context<
            DurableContext | ContextQueries | QueryExecution | MemoryRecall | AgentConversations
          >(),
        );
        return Context.add(services, AsterRuntime, yield* acquireRuntime);
      }),
    ).pipe(Layer.provide(runtimeServices));
  }
}
