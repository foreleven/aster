import { Effect, Match } from "effect";
import { ApplicationError, type ProcessingOwner } from "@aster/api-contracts";
import type { ContextRegistry } from "../context/registry.js";
import { inspectReactions } from "../reactions/inspection.js";
import { inspectNotifications } from "../notifications/inspection.js";

/** Runtime routes operator queries; each owner defines its own safe diagnostic view. */
export const inspectProcessing = Effect.fn("Processing.inspect")(function* (
  registry: ContextRegistry["Service"],
  owner: ProcessingOwner,
) {
  const current = registry.get(`/${owner}`);
  if (!current)
    return yield* new ApplicationError({
      kind: "not-found",
      message: "Processing owner not found",
    });
  return yield* Match.value(owner).pipe(
    Match.when("notifications", () => inspectNotifications(current)),
    Match.when("system-one", () => inspectReactions(current)),
    Match.exhaustive,
  );
});
