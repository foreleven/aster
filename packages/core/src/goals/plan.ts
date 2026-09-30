import { Schema } from "effect";
import { SignalDefinition } from "../config/schema.js";

export const GoalPlan = Schema.Struct({
  progress: Schema.String,
  completed: Schema.Boolean,
  evidence: Schema.Array(Schema.String),
  signals: Schema.Array(SignalDefinition),
});
export type GoalPlan = typeof GoalPlan.Type;
