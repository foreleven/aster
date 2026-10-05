import { ApplicationError, GoalTimelinePage } from "@aster/api-contracts";
import { Effect, Schema } from "effect";
import type { ContextRegistry } from "../context/registry.js";
import { GoalState } from "./state.js";

export const goalTimeline = Effect.fn("Goal.timeline")(function* (
  registry: ContextRegistry["Service"],
  slug: string,
  page: { before?: number; limit?: number } = {},
) {
  const record = registry.get(`/goals/${slug}`);
  if (!record) return yield* new ApplicationError({ kind: "not-found", message: "Goal not found" });
  const state = Schema.decodeUnknownSync(GoalState)(record.state);
  const before = page.before ?? state.inputs.length + 1;
  const limit = page.limit ?? 30;
  if (
    !Number.isInteger(before) ||
    before < 1 ||
    !Number.isInteger(limit) ||
    limit < 1 ||
    limit > 100
  )
    return yield* new ApplicationError({ kind: "invalid-input", message: "Invalid timeline page" });
  const end = Math.min(before - 1, state.inputs.length);
  const start = Math.max(0, end - limit);
  return Schema.decodeUnknownSync(GoalTimelinePage)({
    groups: state.inputs.slice(start, end).map((input) => ({
      requestId: input.inputId,
      ordinal: input.ordinal,
      status: input.status,
      retryOf: input.retryOf,
      input,
      response: input.response,
      error: input.error,
    })),
    total: state.inputs.length,
    nextBefore: start > 0 ? start + 1 : null,
  });
});
