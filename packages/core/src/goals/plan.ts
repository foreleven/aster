import { Schema } from "effect";
import { SignalDefinition } from "../config/schema.js";
import { GoalTaskChange, GoalSignalChange } from "./tasks.js";

export const GoalPlan = Schema.Struct({
  disposition: Schema.optional(Schema.Literals(["advance", "no_change", "ignored"])),
  progress: Schema.String,
  completed: Schema.Boolean,
  evidence: Schema.Array(Schema.String),
  signals: Schema.Array(SignalDefinition),
  signalChanges: Schema.optional(Schema.Array(GoalSignalChange).check(Schema.isMaxLength(16))),
  taskChanges: Schema.optional(Schema.Array(GoalTaskChange).check(Schema.isMaxLength(32))),
}).check(
  Schema.makeFilter(
    (plan) =>
      !plan.disposition ||
      plan.disposition === "advance" ||
      (!plan.completed && !plan.taskChanges?.length && !plan.signalChanges?.length),
    { expected: "Ignored or unchanged evaluations cannot propose mutations or completion" },
  ),
);
export type GoalPlan = typeof GoalPlan.Type;
