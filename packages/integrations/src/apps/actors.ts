import { ReplyTo, type ActorRef } from "@aster/actor";
import {
  ContextActor,
  ContextRegistry,
  ContextQueries,
  ContextQueryInput,
  ContextQueryResult,
  ContextQueryError,
  contextPath,
  defineContext,
  spawnContextChild,
} from "@aster/core";
import { DateTime, Deferred, Effect, Layer, Match, Schema, Scope } from "effect";
import { AppsSettings } from "./config.js";
import { OpenCli } from "./client.js";
import { commandCatalogue, queryArgv } from "./commands.js";
import { AppsState, AppState, appsView, appView } from "./contexts.js";

const QueryReply = Schema.Union([
  Schema.TaggedStruct("Success", { value: ContextQueryResult }),
  Schema.TaggedStruct("Failure", { error: ContextQueryError }),
]);
type QueryReply = typeof QueryReply.Type;
const Ready = Schema.TaggedStruct("Ready", { replyTo: ReplyTo<void>() });
const Query = Schema.TaggedStruct("Query", {
  input: ContextQueryInput,
  cancelled: Schema.declare<Deferred.Deferred<void>>(Deferred.isDeferred),
  replyTo: ReplyTo<QueryReply>(),
});
type Query = typeof Query.Type;
const Command = Schema.Union([
  Ready,
  Query,
  Schema.TaggedStruct("Finished", { requestId: Schema.String, result: QueryReply }),
]);

class AppActor extends ContextActor.Service<AppActor, AppsSettings | OpenCli | ContextQueries>()(
  "apps/AppActor",
  {
    command: Command,
    context: defineContext({
      view: appView,
      state: AppState,
      message: Schema.Never,
    }),
  },
) {
  static readonly layer = Layer.effect(
    AppActor,
    Effect.gen(function* () {
      const registry = yield* ContextRegistry;
      const queries = yield* ContextQueries;
      const settings = yield* AppsSettings;
      const cli = yield* OpenCli;
      const scope = yield* Scope.Scope;
      let pending: Query | undefined;
      yield* Effect.addFinalizer(() =>
        pending
          ? pending.replyTo.tell({
              _tag: "Failure",
              error: new ContextQueryError({
                kind: "unavailable",
                message: "Context query owner stopped or restarted",
              }),
            })
          : Effect.void,
      );
      return AppActor.of({
        started: (actor) =>
          Effect.gen(function* () {
            const path = contextPath(actor);
            const app = settings.apps.find((app) => path === `/apps/${app.name}`)!;
            const previous = registry.get(path);
            const restored = previous
              ? yield* Schema.decodeUnknownEffect(AppState)(previous.state).pipe(Effect.orDie)
              : undefined;
            yield* registry
              .commit(
                {
                  path,
                  description: app.description,
                  messages: [],
                  state: {
                    app: app.name,
                    mode: "query-only",
                    commands: commandCatalogue(app.name),
                    ...(restored?.lastResult ? { lastResult: restored.lastResult } : {}),
                  },
                },
                { expectedRevision: previous?.revision ?? 0, mode: "bootstrap" },
              )
              .pipe(Effect.orDie);
            yield* queries
              .register(
                path,
                Effect.fn("Apps.query")(function* (input) {
                  const cancelled = yield* Deferred.make<void>();
                  const reply = yield* actor.self
                    .ask<QueryReply>(
                      (replyTo) => ({ _tag: "Query", input, cancelled, replyTo }),
                      "100 seconds",
                    )
                    .pipe(
                      Effect.ensuring(Deferred.succeed(cancelled, undefined)),
                      Effect.catchTag("AskTimeoutError", () =>
                        Effect.fail(
                          new ContextQueryError({
                            kind: "timeout",
                            message: "Context query acknowledgement timed out",
                          }),
                        ),
                      ),
                    );
                  if (reply._tag === "Failure") return yield* reply.error;
                  return reply.value;
                }),
              )
              .pipe(Effect.provideService(Scope.Scope, scope))
              .pipe(Effect.orDie);
          }),
        receive: (command, actor) =>
          Match.value(command).pipe(
            Match.tag("Ready", ({ replyTo }) => replyTo.tell(undefined)),
            Match.tag("Query", (request) =>
              Effect.gen(function* () {
                if (yield* Deferred.isDone(request.cancelled)) return;
                if (pending)
                  return yield* request.replyTo.tell({
                    _tag: "Failure",
                    error: new ContextQueryError({
                      kind: "busy",
                      message: "This Context already has a query in progress",
                    }),
                  });
                pending = request;
                const path = contextPath(actor);
                const app = settings.apps.find((app) => path === `/apps/${app.name}`)!;
                const query = Effect.gen(function* () {
                  const argv = yield* queryArgv(
                    app.name,
                    request.input.command,
                    request.input.args,
                  );
                  yield* Effect.logInfo({
                    event: "apps.query.started",
                    path,
                    command: request.input.command,
                  });
                  const data = yield* cli.run(argv);
                  return {
                    path,
                    command: request.input.command,
                    queriedAt: DateTime.formatIso(yield* DateTime.now),
                    data,
                  };
                });
                // Ask cancellation alone only closes the reply ref. Explicit cancellation releases the process too.
                yield* actor.pipeToSelf(
                  Effect.raceFirst(
                    query,
                    Deferred.await(request.cancelled).pipe(
                      Effect.andThen(
                        Effect.fail(
                          new ContextQueryError({
                            kind: "cancelled",
                            message: "Context query cancelled",
                          }),
                        ),
                      ),
                    ),
                  ),
                  (result) => ({ _tag: "Finished", requestId: request.replyTo.path, result }),
                );
              }),
            ),
            Match.tag("Finished", ({ requestId, result }) =>
              Effect.gen(function* () {
                const request = pending;
                // An already queued result from a retired Behavior cannot settle a new query.
                if (!request || request.replyTo.path !== requestId) return;
                const path = contextPath(actor);
                if (result._tag === "Success") {
                  const previous = registry.get(path)!;
                  const state = yield* Schema.decodeUnknownEffect(AppState)(previous.state).pipe(
                    Effect.orDie,
                  );
                  // Commit evidence before replying. Queries do not produce source events or wake unrelated Goals.
                  yield* registry
                    .commit(
                      { ...previous, state: { ...state, lastResult: result.value } },
                      { expectedRevision: previous.revision ?? 0, mode: "bootstrap" },
                    )
                    .pipe(Effect.orDie);
                  yield* Effect.logInfo({
                    event: "apps.query.completed",
                    path,
                    command: result.value.command,
                  });
                } else {
                  yield* Effect.logWarning({
                    event: "apps.query.failed",
                    path,
                    command: request.input.command,
                    kind: result.error.kind,
                    message: result.error.message,
                  });
                }
                yield* request.replyTo.tell(result);
                pending = undefined;
              }),
            ),
            Match.exhaustive,
          ),
      });
    }),
  );
}

export class AppsRootActor extends ContextActor.Service<
  AppsRootActor,
  AppsSettings | OpenCli | ContextQueries
>()("apps/RootActor", {
  command: Ready,
  context: defineContext({
    view: appsView,
    state: AppsState,
    message: Schema.Never,
  }),
}) {
  static readonly layer = Layer.effect(
    AppsRootActor,
    Effect.gen(function* () {
      const registry = yield* ContextRegistry;
      const settings = yield* AppsSettings;
      const children: ActorRef<typeof Command.Type>[] = [];
      return AppsRootActor.of({
        started: (actor) =>
          Effect.gen(function* () {
            yield* registry
              .commit(
                {
                  path: "/apps",
                  description: settings.description,
                  state: { apps: settings.apps.map((app) => `/apps/${app.name}`) },
                  messages: [],
                },
                { expectedRevision: registry.get("/apps")?.revision ?? 0, mode: "bootstrap" },
              )
              .pipe(Effect.orDie);
            for (const app of settings.apps) {
              const existing = yield* actor.child(app.name);
              children.push(
                (existing as ActorRef<typeof Command.Type> | undefined) ??
                  (yield* spawnContextChild(actor, app.name, AppActor)),
              );
            }
          }),
        receive: ({ replyTo }) =>
          Effect.gen(function* () {
            yield* Effect.forEach(
              children,
              (child) => child.ask<void>((replyTo) => ({ _tag: "Ready", replyTo })),
              { discard: true },
            ).pipe(Effect.orDie);
            yield* replyTo.tell(undefined);
          }),
      });
    }),
  );
}
