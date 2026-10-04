import { GoalNextStep } from "@aster/api-contracts";
import { Schema } from "effect";
import { SignalDefinition } from "../config/schema.js";
import { GoalTaskChange, GoalSignalChange } from "./tasks.js";

export const LegacyGoalPlan = Schema.Struct({
  version: Schema.optional(Schema.Literal(1)),
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

/** The current result has one next step; legacy results are retained only in the journal. */
export const GoalPlan = Schema.Struct({
  version: Schema.Literal(2),
  turnId: Schema.NonEmptyString,
  resultId: Schema.NonEmptyString,
  disposition: Schema.Literals(["advance", "no_change", "ignored"]),
  progress: Schema.String,
  evidence: Schema.Array(Schema.String),
  nextStep: GoalNextStep,
  signalChanges: Schema.optional(Schema.Array(GoalSignalChange).check(Schema.isMaxLength(16))),
  taskChanges: Schema.optional(Schema.Array(GoalTaskChange).check(Schema.isMaxLength(32))),
}).check(
  Schema.makeFilter(
    (plan) =>
      plan.turnId === plan.resultId &&
      (plan.disposition === "advance" ||
        (!plan.taskChanges?.length &&
          !plan.signalChanges?.length &&
          plan.nextStep._tag !== "Continue" &&
          plan.nextStep._tag !== "Complete")),
    {
      expected:
        "Matching result identity; ignored/unchanged results cannot mutate, continue or complete",
    },
  ),
);
export type GoalPlan = typeof GoalPlan.Type;
export const StoredGoalPlan = Schema.Union([GoalPlan, LegacyGoalPlan]);
export type StoredGoalPlan = typeof StoredGoalPlan.Type;
