import { ReplyTo } from "@aster/actor";
import { Schema } from "effect";
import {
  ApplicationError,
  CausalChain,
  CommandReceipt,
  GoalDeliveryInput,
} from "@aster/api-contracts";
import { GoalIntentInput } from "./intent.js";

/** Producer-specific envelopes preserve provenance; only UserInput crosses public ingress. */
export const GoalSubmission = Schema.Union([
  Schema.TaggedStruct("UserInput", { text: Schema.NonEmptyString }),
  Schema.TaggedStruct("PersonalMessage", { delivery: GoalDeliveryInput }),
  Schema.TaggedStruct("GoalIntent", { delivery: GoalIntentInput }),
  Schema.TaggedStruct("SignalOccurrence", {
    id: Schema.NonEmptyString,
    signalPath: Schema.String,
    text: Schema.String,
    causal: Schema.optional(CausalChain),
  }),
  Schema.TaggedStruct("ExecutionFeedback", {
    runPath: Schema.String,
    text: Schema.String,
    terminal: Schema.Boolean,
    status: Schema.optional(Schema.String),
    causal: Schema.optional(CausalChain),
  }),
]);
export type GoalSubmission = typeof GoalSubmission.Type;
const submit = { requestId: Schema.NonEmptyString, input: GoalSubmission };
const end = { requestId: Schema.NonEmptyString };
const retry = { requestId: Schema.NonEmptyString, turnId: Schema.NonEmptyString };
export const GoalRequestData = Schema.Union([
  Schema.TaggedStruct("SubmitInput", submit),
  Schema.TaggedStruct("End", end),
  Schema.TaggedStruct("RetryTurn", retry),
]);
export type GoalRequestData = typeof GoalRequestData.Type;
export const GoalCommandReply = Schema.Union([
  Schema.TaggedStruct("Accepted", { receipt: CommandReceipt }),
  Schema.TaggedStruct("Rejected", { error: ApplicationError }),
]);
export type GoalCommandReply = typeof GoalCommandReply.Type;
export const GoalDeliveryReply = GoalCommandReply;
export type GoalDeliveryReply = GoalCommandReply;
const replyTo = ReplyTo<GoalCommandReply>();
export const GoalCommand = Schema.Union([
  Schema.TaggedStruct("SubmitInput", { ...submit, replyTo }),
  Schema.TaggedStruct("End", { ...end, replyTo }),
  Schema.TaggedStruct("RetryTurn", { ...retry, replyTo }),
]);
export type GoalCommand = typeof GoalCommand.Type;
export const GoalRequestRecord = Schema.Struct({
  request: GoalRequestData,
  receipt: CommandReceipt,
});
export type GoalRequestRecord = typeof GoalRequestRecord.Type;
export const GoalReadyReply = Schema.Union([
  Schema.TaggedStruct("Ready", {}),
  Schema.TaggedStruct("Failed", { error: ApplicationError }),
]);
export type GoalReadyReply = typeof GoalReadyReply.Type;
export const GoalControl = Schema.Union([
  Schema.TaggedStruct("Activate", {}),
  Schema.TaggedStruct("AwaitReady", {
    stage: Schema.optional(Schema.Literals(["restored", "activated"])),
    replyTo: ReplyTo<GoalReadyReply>(),
  }),
]);
