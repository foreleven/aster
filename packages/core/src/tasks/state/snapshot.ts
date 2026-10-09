import {
  RemainingAgentTurns,
  TaskDeliveryInput,
  FollowupTaskInput,
  TaskRecoveryInput,
} from "../contracts.js";
import { CommandReceipt } from "../../operations.js";
import { InputRequest, ApprovalResponse } from "../../approvals/contracts.js";

import { Schema } from "effect";

export const TaskInput = Schema.TaggedUnion({
  Initial: { input: TaskDeliveryInput },
  Message: { input: FollowupTaskInput },
  Answer: { requestId: Schema.String, response: ApprovalResponse },
  Check: { input: TaskRecoveryInput },
  Retry: { input: TaskRecoveryInput },
});
export type TaskInput = typeof TaskInput.Type;
export const StoredTaskInput = Schema.Struct({ input: TaskInput, receipt: CommandReceipt });
export const TaskInputRef = Schema.Struct({
  requestId: Schema.String,
  entryId: Schema.Int,
  receipt: CommandReceipt,
  status: Schema.Literals(["pending", "completed"]),
});
export type TaskInputRef = typeof TaskInputRef.Type;
export const TaskRequest = Schema.Struct({
  id: Schema.String,
  kind: Schema.Literals(["confirmation", "approval", "input"]),
  request: InputRequest,
});
export const TaskOutcome = Schema.Struct({
  roundId: Schema.String,
  status: Schema.Literals(["completed", "failed", "waiting_input", "cancelled", "uncertain"]),
  text: Schema.String,
  covered: Schema.Array(Schema.String),
  requests: Schema.optional(Schema.Array(TaskRequest)),
});
export type TaskOutcome = typeof TaskOutcome.Type;
/** Business state only. Messages and executor checkpoints belong to Pi. */
export const TaskSnapshot = Schema.Struct({
  admission: Schema.Struct({
    source: Schema.String,
    replyTo: Schema.String,
    agent: Schema.String,
    remainingAgentTurns: RemainingAgentTurns,
  }),
  status: Schema.Literals([
    "ready",
    "running",
    "waiting_input",
    "completed",
    "failed",
    "cancelled",
    "uncertain",
  ]),
  inputs: Schema.Array(TaskInputRef),
  roundId: Schema.optional(Schema.String),
  outcomeEntryId: Schema.optional(Schema.Int),
  waiting: Schema.optional(Schema.Array(TaskRequest)),
}).check(
  Schema.makeFilter(
    (state) =>
      state.inputs.length > 0 &&
      new Set(state.inputs.map((input) => input.requestId)).size === state.inputs.length &&
      (state.status !== "completed" || state.outcomeEntryId !== undefined),
    { expected: "Unique admitted inputs and a result reference for completion" },
  ),
);
export type TaskSnapshot = typeof TaskSnapshot.Type;
export interface TaskWork {
  readonly roundId: string;
  readonly admissionEntryId: number;
  readonly inputs: readonly TaskInputRef[];
}
