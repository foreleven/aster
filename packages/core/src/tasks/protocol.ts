import { ReplyTo } from "@aster/actor";
import { ApplicationError, CommandReceipt } from "../operations.js";
import { TaskDeliveryInput, TaskRecoveryInput, FollowupTaskInput } from "./contracts.js";
import { Schema } from "effect";
import { ApprovalResolved } from "../approvals/actor.js";
import { TaskOutcome } from "./state/snapshot.js";
export const TaskAdmissionReply = Schema.TaggedUnion({
  Accepted: { receipt: CommandReceipt },
  Rejected: { error: ApplicationError },
});
export type TaskAdmissionReply = typeof TaskAdmissionReply.Type;
export const StartTask = Schema.TaggedStruct("StartTask", {
  input: TaskDeliveryInput,
  replyTo: ReplyTo<TaskAdmissionReply>(),
});
export const Input = Schema.TaggedStruct("Input", {
  input: FollowupTaskInput,
  replyTo: ReplyTo<TaskAdmissionReply>(),
});
export const CheckTask = Schema.TaggedStruct("CheckTask", {
  input: TaskRecoveryInput,
  replyTo: ReplyTo<TaskAdmissionReply>(),
});
export const RetryTask = Schema.TaggedStruct("RetryTask", {
  input: TaskRecoveryInput,
  replyTo: ReplyTo<TaskAdmissionReply>(),
});
export const TaskCommand = Schema.TaggedUnion({
  StartTask: StartTask.fields,
  Input: Input.fields,
  CheckTask: CheckTask.fields,
  RetryTask: RetryTask.fields,
  ApprovalResolved: ApprovalResolved.fields,
  ExecutionSettled: { generation: Schema.String, outcome: TaskOutcome },
  DeliverySettled: { error: Schema.optional(Schema.String) },
  CancellationChecked: {
    generation: Schema.String,
    reason: Schema.String,
    confirmed: Schema.Boolean,
    replyTo: Schema.optional(ReplyTo<void>()),
  },
  Cancel: {
    reason: Schema.String,
    replyTo: Schema.optional(ReplyTo<void>()),
  },
});
export type TaskCommand = typeof TaskCommand.Type;
