import { Schema } from "effect";
import { GoalDefinition } from "../config/schema.js";
import { GoalReceipt } from "./protocol.js";
import { StoredGoalInput } from "./inputs.js";

/** Goal owns business state and delivery to Pi. Pi owns conversation execution and recovery. */
export const GoalState = Schema.Struct({
  definition: GoalDefinition,
  status: Schema.Literals(["active", "completed"]),
  summary: Schema.String,
  inputs: Schema.Array(StoredGoalInput),
  receipts: Schema.Array(GoalReceipt),
}).check(
  Schema.makeFilter(
    (state) =>
      new Set(state.inputs.map((input) => input.inputId)).size === state.inputs.length &&
      state.inputs.filter((input) => input.status === "running" || input.status === "unknown")
        .length <= 1 &&
      state.inputs.every(
        (input, index) =>
          input.goalSlug === state.definition.slug &&
          (index === 0 || state.inputs[index - 1]!.ordinal < input.ordinal),
      ) &&
      new Set(state.receipts.map((item) => item.requestId)).size === state.receipts.length &&
      state.receipts.every((item) => item.requestId === item.receipt.requestId),
    { expected: "Unique Goal inputs and at most one unfinished Pi delivery" },
  ),
);
export type GoalState = typeof GoalState.Type;
