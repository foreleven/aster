import { Effect, Schema } from "effect";
import {
  ApplicationError,
  type ProcessingSnapshot,
  type ProcessingOwner,
} from "@aster/api-contracts";
import type { ContextRegistry } from "../context/registry.js";
import { ReactionSnapshot, deliveriesOf } from "./state.js";
export const inspectReactions = Effect.fn("Reactions.inspect")(function* (
  registry: ContextRegistry["Service"],
  owner: ProcessingOwner,
): Effect.fn.Return<ProcessingSnapshot, ApplicationError> {
  const current = registry.get(`/${owner}`);
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
