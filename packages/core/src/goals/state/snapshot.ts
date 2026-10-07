import { GoalReceipt } from "../protocol.js";
import { GoalDefinition } from "../../config/schema.js";
import { RemainingAgentTurns, TaskPath } from "../../tasks/contracts.js";
import { GoalInputPayload } from "../contracts.js";

import { Schema } from "effect";

export const StoredGoalInput = Schema.Struct({
  inputId: Schema.String,
  entryId: Schema.Int,
  kind: Schema.Union(GoalInputPayload.members.map((member) => member.fields._tag)),
  status: Schema.Literals(["pending", "running", "completed", "failed", "unknown", "ignored"]),
  relevant: Schema.optional(Schema.Boolean),
  error: Schema.optional(Schema.String),
  retryOf: Schema.optional(Schema.String),
  remainingAgentTurns: RemainingAgentTurns,
});
export type StoredGoalInput = typeof StoredGoalInput.Type;

/** Goal owns business state and delivery to Pi. Pi owns conversation execution and recovery. */
export const GoalSnapshot = Schema.Struct({
  definition: GoalDefinition,
  status: Schema.Literals(["active", "completed"]),
  summary: Schema.String,
  tasks: Schema.Array(TaskPath),
  inputs: Schema.Array(StoredGoalInput),
  receipts: Schema.Array(GoalReceipt),
}).check(
  Schema.makeFilter(
    (state) =>
      new Set(state.tasks).size === state.tasks.length &&
      new Set(state.inputs.map((input) => input.inputId)).size === state.inputs.length &&
      state.inputs.filter((input) => input.status === "running" || input.status === "unknown")
        .length <= 1 &&
      new Set(state.receipts.map((item) => item.requestId)).size === state.receipts.length &&
      state.receipts.every((item) => item.requestId === item.receipt.requestId),
    { expected: "Unique Goal tasks, inputs and receipts, and at most one unfinished Pi delivery" },
  ),
);
export type GoalSnapshot = typeof GoalSnapshot.Type;
