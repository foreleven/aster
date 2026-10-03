import { Schema } from "effect";
import { PublicContext } from "@aster/api-contracts";
import { SignalDefinition } from "../config/schema.js";

/** Public evidence only. Native messages remain in the immutable GoalHistory prefix. */
export const FrozenGoalEvaluation = Schema.Struct({
  goal: Schema.Struct({
    slug: Schema.String,
    title: Schema.optional(Schema.String),
    description: Schema.String,
    completionCriteria: Schema.optional(Schema.String),
  }),
  current: PublicContext,
  contexts: Schema.Record(Schema.String, PublicContext),
  signals: Schema.Array(SignalDefinition),
  historyAfter: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  historyThrough: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
}).check(
  Schema.makeFilter(
    (input) =>
      input.historyAfter <= input.historyThrough &&
      input.current.path === `/goals/${input.goal.slug}`,
    { expected: "Frozen Goal input with a matching owner and ordered history range" },
  ),
);
export type FrozenGoalEvaluation = typeof FrozenGoalEvaluation.Type;
