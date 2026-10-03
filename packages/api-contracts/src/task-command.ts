import { CausalChain } from "./notification.js";
import { Schema } from "effect";
import { CommandIdentifier, CommandReceipt, ContextRevision } from "./command.js";

export const PreparedTask = Schema.Struct({
  instructions: Schema.String,
  input: Schema.Array(
    Schema.Struct({ content: Schema.String, sources: Schema.Array(Schema.String) }),
  ),
});
export type PreparedTask = typeof PreparedTask.Type;
export const PersonalTaskProposal = Schema.Struct({
  agent: Schema.NonEmptyString,
  task: Schema.Struct({
    ...PreparedTask.fields,
    instructions: Schema.String.check(Schema.isPattern(/\S/)),
  }),
});
export type PersonalTaskProposal = typeof PersonalTaskProposal.Type;
export const PersonalStartTaskInput = Schema.Struct({
  ...PersonalTaskProposal.fields,
  requestId: CommandIdentifier,
  causationId: CommandIdentifier,
  expectedRevision: ContextRevision,
});
export type PersonalStartTaskInput = typeof PersonalStartTaskInput.Type;
export const TaskDeliveryInput = Schema.Struct({
  operation: Schema.Literal("startTask"),
  requestId: CommandIdentifier,
  causationId: CommandIdentifier,
  source: Schema.Literal("/personal"),
  causal: Schema.optional(CausalChain),
  target: Schema.String.check(Schema.isPattern(/^\/runs\/personal--[a-f0-9]{64}$/)),
  expectedRevision: Schema.Literal(0),
  createdAt: Schema.NonEmptyString,
  ...PersonalTaskProposal.fields,
});
export type TaskDeliveryInput = typeof TaskDeliveryInput.Type;
export const TaskAdmission = Schema.Struct({ input: TaskDeliveryInput, receipt: CommandReceipt });
