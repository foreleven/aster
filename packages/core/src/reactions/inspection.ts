import { Effect, Schema } from "effect";
import { ApplicationError, type ProcessingSnapshot } from "@aster/api-contracts";
import type { ContextSnapshot } from "../context/model.js";
import { ReactionState, deliveriesOf } from "./state.js";
export const inspectReactions = Effect.fn("Reactions.inspect")(function* (
  current: ContextSnapshot,
): Effect.fn.Return<ProcessingSnapshot, ApplicationError> {
  const owner = "system-one";

  const state = yield* Schema.decodeUnknownEffect(ReactionState)(current.state).pipe(
    Effect.mapError(
      () => new ApplicationError({ kind: "unavailable", message: "Reaction state unavailable" }),
    ),
  );
  return {
    owner,
    revision: current.revision ?? 0,
    entries: state.work.flatMap((work): ProcessingSnapshot["entries"] => [
      {
        id: work.event.id,
        kind: "screening",
        workId: work.event.id,
        source: work.event.record.path,
        target: "/system-one",
        status: work.status,
        attempts: work.attempts,
        error: "error" in work ? work.error : undefined,
      },
      ...deliveriesOf(work).map((item) => ({
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
