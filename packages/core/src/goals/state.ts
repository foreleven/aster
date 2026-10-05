import { BusinessNotification, CausalChain } from "@aster/api-contracts";
import { Schema } from "effect";
import { GoalTitle } from "../config/schema.js";
import { GoalRequestRecord } from "./protocol.js";
import { StoredGoalInput } from "./inputs.js";

/** Goal owns business state and delivery to Pi. Pi owns conversation execution and recovery. */
export const GoalState = Schema.Struct({
  slug: Schema.String,
  title: Schema.optional(GoalTitle),
  description: Schema.String,
  completionCriteria: Schema.optional(Schema.String),
  status: Schema.Literals(["active", "completed"]),
  completionOrigin: Schema.optional(Schema.Literals(["user", "criteria"])),
  summary: Schema.String,
  progress: Schema.String,
  lastError: Schema.optional(Schema.String),
  inputs: Schema.Array(StoredGoalInput),
  requests: Schema.optional(Schema.Array(GoalRequestRecord)),
  businessOutbox: Schema.optional(Schema.Array(BusinessNotification)),
  causal: Schema.optional(CausalChain),
  historyCount: Schema.Number,
}).check(
  Schema.makeFilter(
    (state) =>
      new Set(state.inputs.map((input) => input.inputId)).size === state.inputs.length &&
      state.inputs.filter((input) => input.status === "running" || input.status === "unknown")
        .length <= 1 &&
      state.inputs.every(
        (input, index) =>
          input.goalSlug === state.slug &&
          (index === 0 || state.inputs[index - 1]!.ordinal < input.ordinal),
      ) &&
      new Set(state.requests?.map((item) => item.request.requestId)).size ===
        (state.requests?.length ?? 0),
    { expected: "Unique Goal inputs and at most one unfinished Pi delivery" },
  ),
);
export type GoalState = typeof GoalState.Type;
