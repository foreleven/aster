import { Schema } from "effect";
import { GoalTask } from "./tasks.js";

export const GoalState = Schema.Struct({
  slug: Schema.String,
  status: Schema.Literals(["active", "completed"]),
  description: Schema.String,
  completionCriteria: Schema.optional(Schema.String),
  summary: Schema.String,
  progress: Schema.String,
  lastError: Schema.optional(Schema.String),
  tasks: Schema.Array(GoalTask),
  historyThrough: Schema.Number,
  historyCount: Schema.Number,
  pendingEvaluation: Schema.Boolean,
  receivedEvents: Schema.Array(Schema.String),
});
export type GoalState = typeof GoalState.Type;
