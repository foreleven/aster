import { ReplyTo } from "@aster/actor";
import {
  ApplicationError,
  CommandReceipt,
  TaskDeliveryInput,
  TaskRecoveryInput,
  FollowupTaskInput,
} from "@aster/api-contracts";
import { Schema } from "effect";
import { ApprovalResolved } from "../approvals/actor.js";
import { TaskOutcome } from "./state/snapshot.js";
export const TaskAdmissionReply = Schema.Union([
  Schema.TaggedStruct("Accepted", { receipt: CommandReceipt }),
  Schema.TaggedStruct("Rejected", { error: ApplicationError }),
]);
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
export const TaskCommand = Schema.Union([
  StartTask,
  Input,
  CheckTask,
  RetryTask,
  ApprovalResolved,
  Schema.TaggedStruct("ExecutionSettled", { generation: Schema.String, outcome: TaskOutcome }),
  Schema.TaggedStruct("DeliverySettled", { error: Schema.optional(Schema.String) }),
  Schema.TaggedStruct("CancellationChecked", {
    generation: Schema.String,
    reason: Schema.String,
    confirmed: Schema.Boolean,
    replyTo: Schema.optional(ReplyTo<void>()),
  }),
  Schema.TaggedStruct("Cancel", {
    reason: Schema.String,
    replyTo: Schema.optional(ReplyTo<void>()),
  }),
]);
export type TaskCommand = typeof TaskCommand.Type;
