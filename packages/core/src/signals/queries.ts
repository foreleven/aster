import { Command, type ActorContext, type ActorRef } from "@aster/actor";
import { Effect, Match, Schema } from "effect";
import { ContextQueryError, PublicContext } from "../context/contracts.js";
import { ContextCommand } from "../context/queries/protocol.js";
import { publicJson } from "../json.js";
import { SignalSnapshot } from "./state/snapshot.js";

export class ListSignals extends ContextCommand.Class<ListSignals>()("list", {
  success: Schema.Json,
  error: ContextQueryError,
  description:
    "List current Signal Actors, optionally filtering their public business data by text.",
  payload: { query: Schema.optional(Schema.String) },
}) {}
export class ReadSignal extends ContextCommand.Class<ReadSignal>()("read", {
  success: Schema.Json,
  error: ContextQueryError,
  description: "Read a Signal Actor's public business details by its full path.",
  payload: { path: Schema.NonEmptyString },
}) {}
export const SignalsQueries = [ListSignals, ReadSignal] as const;

export class GetSignal extends Command.Class<GetSignal>()("GetSignal", {
  payload: {},
  reply: Schema.optional(PublicContext),
}) {}
const readSignal = (child: ActorRef<unknown>) =>
  (child as ActorRef<GetSignal>)
    .ask<Command.Reply<typeof GetSignal>>((replyTo) => new GetSignal({ replyTo }))
    .pipe(
      Effect.catchTag("AskTimeoutError", () =>
        Effect.fail(
          new ContextQueryError({ kind: "unavailable", message: "Signal Actor unavailable" }),
        ),
      ),
    );
export const listSignals = Effect.fn("Signal.list")(function* (
  actor: Pick<ActorContext<unknown>, "children">,
) {
  const children = (yield* actor.children())
    .filter((child) => /^[a-z0-9][a-z0-9-]*$/.test(child.path.split("/").at(-1)!))
    .sort((a, b) => a.path.localeCompare(b.path));
  const records = yield* Effect.forEach(children, readSignal, { concurrency: 4 });
  return records.filter((record) => record !== undefined);
});
export const querySignals = Effect.fn("Signal.query")(function* (
  command: ListSignals | ReadSignal,
  actor: Pick<ActorContext<unknown>, "children" | "child">,
) {
  const select = (record: PublicContext, detail: boolean) => {
    const state = Schema.decodeUnknownSync(SignalSnapshot)(record.state);
    return publicJson({
      path: record.path,
      owner: state.owner,
      status: state.status,
      trigger: state.trigger,
      nextDue: state.nextDue,
      ...(detail ? { task: state.task, version: state.version } : {}),
    });
  };
  const data = yield* Match.value(command).pipe(
    Match.tag("read", (request) =>
      Effect.gen(function* () {
        if (!/^\/signals\/[^/]+$/.test(request.path))
          return yield* new ContextQueryError({
            kind: "invalid-input",
            message: "Invalid Signal path",
          });
        const child = yield* actor.child(request.path.slice("/signals/".length));
        const record = child ? yield* readSignal(child) : undefined;
        if (!record)
          return yield* new ContextQueryError({
            kind: "unavailable",
            message: "Signal Actor unavailable",
          });
        return select(record, true);
      }),
    ),
    Match.tag("list", (request) =>
      Effect.gen(function* () {
        const items = (yield* listSignals(actor)).map((record) => select(record, false));
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
