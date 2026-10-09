import { Command, type ActorContext, type ActorRef } from "@aster/actor";
import { Effect, Match, Schema } from "effect";
import { ContextCommand } from "../context/queries/protocol.js";
import { ContextQueryError } from "../context/contracts.js";
import { publicJson } from "../json.js";

export class ListGoals extends ContextCommand.Class<ListGoals>()("list", {
  success: Schema.Json,
  error: ContextQueryError,
  description: "List current Goal Actors, optionally filtering their public business data by text.",
  payload: { query: Schema.optional(Schema.String) },
}) {}
export class ReadGoal extends ContextCommand.Class<ReadGoal>()("read", {
  success: Schema.Json,
  error: ContextQueryError,
  description: "Read a Goal Actor's public business details by its full path.",
  payload: { path: Schema.NonEmptyString },
}) {}
export const GoalsQueries = [ListGoals, ReadGoal] as const;

export class GetGoalDetails extends Command.Class<GetGoalDetails>()("GetGoalDetails", {
  payload: { detail: Schema.Boolean },
  success: Schema.Json,
  error: ContextQueryError,
}) {}

export const queryGoals = Effect.fn("Goal.query")(function* (
  command: ListGoals | ReadGoal,
  actor: Pick<ActorContext<unknown>, "children" | "child">,
) {
  const read = (child: ActorRef<unknown>, detail: boolean) =>
    // This root owns children of this domain; assert the protocol only at lookup.
    (child as ActorRef<GetGoalDetails>)
      .ask<Command.Reply<typeof GetGoalDetails>>(
        (replyTo) => new GetGoalDetails({ detail, replyTo }),
      )
      .pipe(
        Effect.flatMap((reply) =>
          Match.value(reply).pipe(
            Match.tag("Success", (reply) => Effect.succeed(reply.value)),
            Match.tag("Failure", (reply) => Effect.fail(reply.error)),
            Match.exhaustive,
          ),
        ),
        Effect.catchTag("AskTimeoutError", () =>
          Effect.fail(
            new ContextQueryError({
              kind: "unavailable",
              message: "Goal Actor unavailable",
            }),
          ),
        ),
      );
  const data = yield* Match.value(command).pipe(
    Match.tag("read", (request) =>
      Effect.gen(function* () {
        if (!/^\/goals\/[^/]+$/.test(request.path))
          return yield* new ContextQueryError({
            kind: "invalid-input",
            message: "Invalid Goal path",
          });
        const child = yield* actor.child(request.path.slice("/goals/".length));
        if (!child)
          return yield* new ContextQueryError({
            kind: "unavailable",
            message: "Goal Actor unavailable",
          });
        return yield* read(child, true);
      }),
    ),
    Match.tag("list", (request) =>
      Effect.gen(function* () {
        const children = (yield* actor.children())
          .filter((child) => /^[a-z0-9][a-z0-9-]*$/.test(child.path.split("/").at(-1)!))
          .sort((a, b) => a.path.localeCompare(b.path));
        const items = yield* Effect.forEach(children, (child) => read(child, false), {
          concurrency: 4,
        });
        const query = request.query?.toLowerCase();
        const matching = query
          ? items.filter((item) => JSON.stringify(item).toLowerCase().includes(query))
          : items;
        return publicJson({ items: matching, total: matching.length });
      }),
    ),
    Match.exhaustive,
  );
  return data;
});
