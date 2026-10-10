import { CurrentContextPath } from "@aster/core";
import { CommandProcessor } from "@aster/actor";
import { ContextActor, contextPath, DurableContext, spawnContextChild } from "@aster/core";
import { Effect, Match, Schema } from "effect";
import { OpenCli } from "./client.js";
import { appCommands, queryArgv } from "./commands.js";
import { AppsSettings } from "./settings.js";
import { AppsState, AppState } from "./contexts.js";

const AppActor = (appName: import("./commands.js").AppName) =>
  ContextActor.define("apps/AppActor", {
    commands: appCommands[appName],
  })(
    Effect.gen(function* () {
      const settings = yield* AppsSettings;
      const cli = yield* OpenCli;
      const processor = yield* CommandProcessor.make({ concurrency: 1 });
      const path = yield* CurrentContextPath;
      const app = settings.apps.find((app) => path === `/apps/${app.name}`)!;
      const session = yield* (yield* DurableContext)
        .open({
          path,
          state: AppState,
          message: Schema.Never,
          serialize: (state) => JSON.stringify(state),
          initial: { description: app.description, state: { app: app.name, mode: "query-only" } },
        })
        .pipe(Effect.orDie);
      yield* session.state
        .set(
          { app: app.name, mode: "query-only" },
          { ...{ mode: "bootstrap" }, description: app.description },
        )
        .pipe(Effect.orDie);
      return {
        receive: (command, actor) =>
          Match.value(command).pipe(
            Match.orElse((request) =>
              processor.submit(
                request,
                actor,
                Effect.gen(function* () {
                  const path = contextPath(actor);
                  const contract = appCommands[appName].find(
                    (candidate) => candidate._tag === request._tag,
                  )!;
                  const args = yield* Schema.encodeUnknownEffect(contract.payloadSchema)(
                    request,
                  ).pipe(Effect.orDie);
                  const argv = yield* queryArgv(appName, request._tag, args);
                  yield* Effect.logInfo({
                    event: "apps.query.started",
                    path,
                    command: request._tag,
                  });
                  const data = yield* cli.run(argv);
                  yield* Effect.logInfo({
                    event: "apps.query.completed",
                    path,
                    command: request._tag,
                  });
                  return data;
                }).pipe(
                  Effect.tapError((error) =>
                    Effect.logWarning({
                      event: "apps.query.failed",
                      path: contextPath(actor),
                      command: request._tag,
                      kind: error.kind,
                      message: error.message,
                    }),
                  ),
                ),
              ),
            ),
          ),
      };
    }),
  );

export const AppsRootActor = ContextActor.define("apps/RootActor", {
  commands: [],
})(
  Effect.gen(function* () {
    const settings = yield* AppsSettings;
    const session = yield* (yield* DurableContext)
      .open({
        path: yield* CurrentContextPath,
        state: AppsState,
        message: Schema.Never,
        serialize: (state) => JSON.stringify(state),
        initial: {
          description: settings.description,
          state: { apps: settings.apps.map((app) => `/apps/${app.name}`) },
        },
      })
      .pipe(Effect.orDie);
    yield* session.state
      .set(
        { apps: settings.apps.map((app) => `/apps/${app.name}`) },
        { ...{ mode: "bootstrap" }, description: settings.description },
      )
      .pipe(Effect.orDie);
    return {
      started: (actor) =>
        Effect.gen(function* () {
          for (const app of settings.apps) {
            const existing = yield* actor.child(app.name);
            const child =
              existing ?? (yield* spawnContextChild(actor, app.name, AppActor(app.name)));
            yield* child.awaitStarted;
          }
        }),
      receive: () => Effect.void,
    };
  }),
);
