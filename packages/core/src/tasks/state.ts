import {
  CausalChain,
  CommandReceipt,
  TaskAction,
  WritebackOperation,
  ExecutionResumption,
} from "@aster/api-contracts";
import { Schema } from "effect";
import { ExecutionSession } from "./model.js";

export const TaskInputRef = Schema.Struct({
  requestId: Schema.String,
  entryId: Schema.Int,
  receipt: CommandReceipt,
  status: Schema.Literals(["pending", "sending", "accepted", "completed", "unknown", "rejected"]),
});
export type TaskInputRef = typeof TaskInputRef.Type;
/** Business state only. Instructions, inputs and results are Pi entries. */
export const TaskState = Schema.Struct({
  admission: Schema.Struct({
    source: Schema.String,
    replyTo: Schema.String,
    agent: Schema.String,
    causal: CausalChain,
    action: Schema.optional(TaskAction),
  }),
  executorPrompt: Schema.String,
  resumptions: Schema.optional(Schema.Array(ExecutionResumption)),
  status: Schema.Literals([
    "awaiting-confirmation",
    "ready",
    "submitting",
    "running",
    "waiting_input",
    "completed",
    "failed",
    "cancelled",
    "rejected",
    "uncertain",
  ]),
  inputs: Schema.Array(TaskInputRef),
  responses: Schema.optional(
    Schema.Array(
      Schema.Struct({
        requestId: Schema.String,
        status: Schema.Literals(["sending", "sent", "unknown"]),
      }),
    ),
  ),
  session: Schema.optional(ExecutionSession),
  outcomeEntryId: Schema.optional(Schema.Int),
  writeback: Schema.optional(WritebackOperation),
}).check(
  Schema.makeFilter(
    (state) =>
      state.inputs.length > 0 &&
      new Set(state.inputs.map((input) => input.requestId)).size === state.inputs.length &&
      (state.admission.agent === "internal" ||
        !["running", "waiting_input"].includes(state.status) ||
        state.session !== undefined) &&
      (state.status !== "completed" || state.outcomeEntryId !== undefined),
    {
      expected:
        "Unique admitted inputs, an execution handle for running external work, and a result reference for completion",
    },
  ),
);
export type TaskState = typeof TaskState.Type;
