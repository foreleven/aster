import type { MailboxOf } from "@aster/actor";
import { Command as ActorCommand, ReplyTo } from "@aster/actor";
import { Schema } from "effect";
import { ApprovalResolved } from "../approvals/actor.js";
import { ApplicationError, CommandReceipt } from "../operations.js";
import { FollowupTaskInput, TaskDeliveryInput, TaskRecoveryInput } from "./contracts.js";
import { TaskOutcome } from "./state/snapshot.js";
export const TaskAdmissionReply = Schema.TaggedUnion({
  Accepted: { receipt: CommandReceipt },
  Rejected: { error: ApplicationError },
});
export type TaskAdmissionReply = typeof TaskAdmissionReply.Type;
export class StartTask extends ActorCommand.Class<StartTask>()("StartTask", {
  payload: { input: TaskDeliveryInput },
  reply: TaskAdmissionReply,
}) {}
export class Input extends ActorCommand.Class<Input>()("Input", {
  payload: { input: FollowupTaskInput },
  reply: TaskAdmissionReply,
}) {}
export class CheckTask extends ActorCommand.Class<CheckTask>()("CheckTask", {
  payload: { input: TaskRecoveryInput },
  reply: TaskAdmissionReply,
}) {}
export class RetryTask extends ActorCommand.Class<RetryTask>()("RetryTask", {
  payload: { input: TaskRecoveryInput },
  reply: TaskAdmissionReply,
}) {}
export class Cancel extends ActorCommand.Class<Cancel>()("Cancel", {
  payload: {
    reason: Schema.String,
    replyTo: Schema.optional(ReplyTo<void>()),
  },
}) {}
export const TaskCommands = [
  StartTask,
  Input,
  CheckTask,
  RetryTask,
  ApprovalResolved,
  Cancel,
] as const;
export const TaskInternal = Schema.TaggedUnion({
  ExecutionSettled: { generation: Schema.String, outcome: TaskOutcome },
  DeliverySettled: { error: Schema.optional(Schema.String) },
  CancellationChecked: {
    generation: Schema.String,
    reason: Schema.String,
    confirmed: Schema.Boolean,
    replyTo: Schema.optional(ReplyTo<void>()),
  },
});
export type TaskCommand = MailboxOf<typeof TaskCommands, typeof TaskInternal>;
