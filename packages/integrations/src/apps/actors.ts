import { Command as ActorCommand, CommandProcessor, type ActorRef } from "@aster/actor";
import { ContextActor, contextPath, ContextSession, spawnContextChild } from "@aster/core";
import { Effect, Match, Schema } from "effect";
import { OpenCli } from "./client.js";
import { appCommands, queryArgv } from "./commands.js";
import { AppsSettings } from "./config.js";
import { AppsState, AppState, appsView, appView } from "./contexts.js";

export class Ready extends ActorCommand.Class<Ready>()("Ready", {
  payload: {},
  reply: Schema.Void,
}) {}

const AppActor = (appName: import("./commands.js").AppName) =>
  ContextActor.define("apps/AppActor", {
    commands: [Ready, ...appCommands[appName]],
  })((owner) =>
    Effect.gen(function* () {
      const settings = yield* AppsSettings;
      const cli = yield* OpenCli;
      const processor = yield* CommandProcessor.make({ concurrency: 1 });
      const path = contextPath(owner);
      const app = settings.apps.find((app) => path === `/apps/${app.name}`)!;
      const session = yield* ContextSession.make({
        path,
        state: AppState,
        message: Schema.Never,
        view: appView,
        initial: { description: app.description, state: { app: app.name, mode: "query-only" } },
      }).pipe(Effect.orDie);
      yield* session
        .set(
          { description: app.description, state: { app: app.name, mode: "query-only" } },
          { mode: "bootstrap" },
        )
        .pipe(Effect.orDie);
      return {
        receive: (command, actor) =>
          Match.value(command).pipe(
            Match.tag("Ready", ({ replyTo }) => replyTo.tell(undefined)),
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
  commands: [Ready],
})(
  Effect.gen(function* () {
    const settings = yield* AppsSettings;
    const session = yield* ContextSession.make({
      path: "/apps",
      state: AppsState,
      message: Schema.Never,
      view: appsView,
      initial: {
        description: settings.description,
        state: { apps: settings.apps.map((app) => `/apps/${app.name}`) },
      },
    }).pipe(Effect.orDie);
    yield* session
      .set(
        {
          description: settings.description,
          state: { apps: settings.apps.map((app) => `/apps/${app.name}`) },
        },
        { mode: "bootstrap" },
      )
      .pipe(Effect.orDie);
    const children: ActorRef<typeof Ready.Type>[] = [];
    return {
      started: (actor) =>
        Effect.gen(function* () {
          for (const app of settings.apps) {
            const existing = yield* actor.child(app.name);
            children.push(
              (existing as ActorRef<typeof Ready.Type> | undefined) ??
                (yield* spawnContextChild(actor, app.name, AppActor(app.name))),
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
    };
  }),
);
