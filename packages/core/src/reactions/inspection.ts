import { Effect, Schema } from "effect";
import { ApplicationError } from "../operations.js";
import type { ContextRegistry } from "../context/registry.js";
import {
  ReactionSnapshot,
  deliveriesOf,
  matchesOf,
  workStatus,
  type ReactionWork,
} from "./state.js";
/** Public diagnostics expose decisions and receipts, never frozen model evidence. */
export const reactionWorkView = (work: ReactionWork) => {
  const matches = matchesOf(work);
  const errors = matches.flatMap((match) =>
    match._tag === "Failed" ? [`${match.target}: ${match.error}`] : [],
  );
  return {
    event: {
      id: work.event.id,
      createdAt: work.event.createdAt,
      record: { path: work.event.record.path, revision: work.event.record.revision },
    },
    status: workStatus(work),
    matches,
    error: errors.length ? errors.join("; ") : undefined,
    deliveries: deliveriesOf(work).map((item) => ({
      command: {
        _tag: item.command._tag,
        input: { requestId: item.command.input.requestId, target: item.command.input.target },
      },
      status: item.status,
      attempts: item.attempts,
      error: "error" in item ? item.error : undefined,
      receipt: item.status === "delivered" ? item.receipt : undefined,
    })),
  };
};

export const inspectReactions = Effect.fn("Reactions.inspect")(function* (
  registry: ContextRegistry["Service"],
) {
  const current = registry.get("/system-one");
  if (!current)
    return yield* new ApplicationError({
      kind: "not-found",
      message: "Processing owner not found",
    });

  const state = yield* Schema.decodeUnknownEffect(ReactionSnapshot)(current.state).pipe(
    Effect.mapError(
      () => new ApplicationError({ kind: "unavailable", message: "Reaction state unavailable" }),
    ),
  );
  return {
    owner: "system-one" as const,
    revision: current.revision ?? 0,
    entries: state.work.map(reactionWorkView).flatMap((work) => [
      {
        id: work.event.id,
        kind: "screening" as const,
        workId: work.event.id,
        source: work.event.record.path,
        target: "/system-one",
        status: work.status,
        matches: work.matches,
        error: "error" in work ? work.error : undefined,
      },
      ...work.deliveries.map((item) => ({
        id: item.command.input.requestId,
        kind: "reaction-delivery" as const,
        workId: work.event.id,
        source: work.event.record.path,
        target: item.command.input.target,
        status: item.status,
        attempts: item.attempts,
        error: "error" in item ? item.error : undefined,
      })),
    ]),
  };
});
