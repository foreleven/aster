import { Effect } from "effect";
import { ApplicationError, type ProcessingOwner } from "@aster/api-contracts";
import type { ContextRegistry } from "../context/registry.js";
import { inspectReactions } from "../reactions/inspection.js";
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
  return yield* inspectReactions(current);
});
