import { ReplyTo } from "@aster/actor";
import {
  ApplicationError,
  CommandReceipt,
  TaskDeliveryInput,
  ResumeTaskDeliveryInput,
  FollowupTaskInput,
} from "@aster/api-contracts";
import { Schema } from "effect";
import { ApprovalResolved } from "../approvals/actor.js";
import { ExecutionSession, ExecutionStatus } from "./model.js";
import { WritebackFinished } from "./writeback.js";

export const TaskAdmissionReply = Schema.Union([
  Schema.TaggedStruct("Accepted", { receipt: CommandReceipt }),
  Schema.TaggedStruct("Rejected", { error: ApplicationError }),
]);
export type TaskAdmissionReply = typeof TaskAdmissionReply.Type;
export const StartTask = Schema.TaggedStruct("StartTask", {
  input: TaskDeliveryInput,
  replyTo: ReplyTo<TaskAdmissionReply>(),
});
export const FollowupTask = Schema.TaggedStruct("FollowupTask", {
  input: FollowupTaskInput,
  replyTo: ReplyTo<TaskAdmissionReply>(),
});
export const ResumeTask = Schema.TaggedStruct("ResumeTask", {
  input: ResumeTaskDeliveryInput,
  replyTo: ReplyTo<TaskAdmissionReply>(),
});
export const TaskReady = Schema.TaggedStruct("Ready", { replyTo: ReplyTo<void>() });
const result = <A extends Schema.Constraint>(value: A) =>
  Schema.Union([
    Schema.TaggedStruct("Success", { value }),
    Schema.TaggedStruct("Failure", { error: Schema.instanceOf(Error) }),
  ]);
export const TaskCommand = Schema.Union([
  StartTask,
  FollowupTask,
  ResumeTask,
  TaskReady,
  ApprovalResolved,
  WritebackFinished,
  Schema.TaggedStruct("Resume", {}),
  Schema.TaggedStruct("ResumeObserved", {
    requestId: Schema.String,
    result: result(ExecutionStatus),
  }),
  Schema.TaggedStruct("Resumed", { requestId: Schema.String, result: result(ExecutionSession) }),
  Schema.TaggedStruct("SubmissionLocated", {
    requestId: Schema.String,
    result: result(Schema.Option(ExecutionSession)),
  }),
  Schema.TaggedStruct("InternalSettled", {
    generation: Schema.String,
    inputId: Schema.String,
    result: result(Schema.String),
  }),
  Schema.TaggedStruct("ExternalSubmitted", {
    generation: Schema.String,
    inputId: Schema.String,
    result: result(ExecutionSession),
  }),
  Schema.TaggedStruct("ExternalStatus", {
    generation: Schema.String,
    result: result(ExecutionStatus),
  }),
  Schema.TaggedStruct("Responded", {
    generation: Schema.String,
    requestId: Schema.String,
    result: result(Schema.Void),
  }),
  Schema.TaggedStruct("FeedbackDelivered", { result: result(Schema.Void) }),
  Schema.TaggedStruct("Cancel", {
    reason: Schema.String,
    replyTo: Schema.optional(ReplyTo<void>()),
  }),
]);
export type TaskCommand = typeof TaskCommand.Type;
