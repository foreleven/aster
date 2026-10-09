import { ContextSession } from "../context/session.js";
import { Command as ActorCommand, CommandProcessor, type ActorRef } from "@aster/actor";
import { Effect, Match, Schema } from "effect";
import { GoalSettings } from "../config/settings.js";
import { ContextActor } from "../context/actor.js";
import { ApplicationError } from "../operations.js";
import { GoalActor } from "./actor.js";
import { GoalCommand, type GoalMailbox } from "./protocol.js";
import { queryGoals, GoalsQueries } from "./queries.js";

export class GoalsRootCommand extends ActorCommand.Class<GoalsRootCommand>()("Route", {
  payload: {
    slug: Schema.String,
    command: GoalCommand,
  },
}) {}
export const GoalsRootActor = ContextActor.define("goals/RootActor", {
  commands: [GoalsRootCommand, ...GoalsQueries],
})(
  Effect.gen(function* () {
    const processor = yield* CommandProcessor.make({ concurrency: 2 });
    const settings = yield* GoalSettings;
    yield* ContextSession.make({
      path: "/goals",
      state: Schema.Struct({}),
      message: Schema.Never,
      initial: { state: {}, description: "Goals" },
    }).pipe(Effect.orDie);
    return {
      started: (context) =>
        Effect.gen(function* () {
          for (const goal of settings.definitions) {
            const child =
              (yield* context.child(goal.slug)) ?? (yield* context.spawn(goal.slug, GoalActor));
            yield* context.watch(child);
          }
        }),
      // Supervision owns retries. A terminal child failure must not stop sibling Goals.
      receiveSignal: (signal) =>
        Effect.logError({
          event: "goal.actor.terminated",
          actorPath: signal.ref.path,
          cause: signal.cause,
        }),
      receive: (command, context) =>
        Match.value(command).pipe(
          Match.tag("list", "read", (request) =>
            processor.submit(request, context, queryGoals(request, context)),
          ),
          Match.tag("Route", (command) =>
            Effect.gen(function* () {
              const child = settings.definitions.some((goal) => goal.slug === command.slug)
                ? yield* context.child(command.slug)
                : undefined;
              if (child) yield* (child as ActorRef<GoalMailbox>).tell(command.command);
              else
                yield* command.command.replyTo.tell({
                  _tag: "Rejected",
                  error: new ApplicationError({
                    kind: "not-found",
                    message: "Goal Actor unavailable",
                  }),
                });
            }),
          ),
          Match.exhaustive,
        ),
    };
  }),
);
