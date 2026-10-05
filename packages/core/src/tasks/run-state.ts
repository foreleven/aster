import { TaskAdmission, RunResumption, WritebackOperation } from "@aster/api-contracts";
import { Schema } from "effect";
/** The Run owns admission, confirmation and reply delivery; Delegation owns external execution. */
export const RunState = Schema.Struct({
  admission: TaskAdmission,
  executorPrompt: Schema.String,
  status: Schema.Literals([
    "awaiting-confirmation",
    "ready",
    "rejected",
    "submitting",
    "running",
    "waiting_input",
    "completed",
    "failed",
    "cancelled",
    "uncertain",
  ]),
  approvals: Schema.optional(Schema.Array(Schema.String)),
  resumptions: Schema.optional(Schema.Array(RunResumption)),
  outcomeText: Schema.optional(Schema.String),
  writeback: Schema.optional(WritebackOperation),
});
export type RunState = typeof RunState.Type;
export const terminalRunText = (state: RunState, includeUncertain = false) =>
  [
    "completed",
    "failed",
    "cancelled",
    "rejected",
    ...(includeUncertain ? ["uncertain"] : []),
  ].includes(state.status)
    ? state.outcomeText
    : undefined;
