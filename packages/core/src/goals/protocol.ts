import { GoalIntentInput } from "./screening/intent.js";
import {
  ApplicationError,
  GoalExecutionFeedback,
  TaskMessage,
  CommandReceipt,
  TaskPath,
} from "@aster/api-contracts";
import { Predicate, Schema } from "effect";
import { ReplyTo } from "@aster/actor";
import { publicJson } from "../context/json.js";
import { createHash } from "node:crypto";
import { AgentError } from "@aster/agent";

/** Producer-specific envelopes preserve provenance; only UserInput crosses public ingress. */
export const GoalSubmission = Schema.Union([
  Schema.TaggedStruct("TaskMessage", { delivery: TaskMessage }),
  Schema.TaggedStruct("UserInput", { text: Schema.NonEmptyString }),
  Schema.TaggedStruct("GoalIntent", { delivery: GoalIntentInput }),
  GoalExecutionFeedback,
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
const replyTo = ReplyTo<GoalCommandReply>();
export const GoalCommand = Schema.Union([
  Schema.TaggedStruct("SubmitInput", { ...submit, replyTo }),
  Schema.TaggedStruct("End", { ...end, replyTo }),
  Schema.TaggedStruct("RetryTurn", { ...retry, replyTo }),
]);
export type GoalCommand = typeof GoalCommand.Type;
/** Receipts retain identity and content equality without duplicating the accepted input. */
export const GoalReceipt = Schema.Struct({
  requestId: Schema.String,
  payloadFingerprint: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
  receipt: CommandReceipt,
});
export type GoalReceipt = typeof GoalReceipt.Type;
/** Decode before hashing. Sort JSON object keys, preserve array order and omit absent fields. */
export const goalRequestFingerprint = (request: GoalRequestData): string =>
  createHash("sha256")
    .update(
      JSON.stringify(
        publicJson(Schema.decodeUnknownSync(GoalRequestData)(request)),
        (_key, value: unknown) =>
          Predicate.isObject(value)
            ? Object.fromEntries(
                Object.keys(value)
                  .sort()
                  .map((key) => [key, value[key]]),
              )
            : value,
      ),
    )
    .digest("hex");
export const GoalTaskReply = Schema.Union([
  Schema.TaggedStruct("Attached", {}),
  Schema.TaggedStruct("Rejected", { error: ApplicationError }),
]);
export type GoalTaskReply = typeof GoalTaskReply.Type;

export const GoalMailbox = Schema.Union([
  GoalCommand,
  Schema.TaggedStruct("AttachTask", {
    taskPath: TaskPath,
    replyTo: ReplyTo<GoalTaskReply>(),
  }),
  Schema.TaggedStruct("RunNext", {}),
  Schema.TaggedStruct("GateSettled", {
    generation: Schema.String,
    inputId: Schema.String,
    result: Schema.Union([
      Schema.TaggedStruct("Success", {
        value: Schema.Struct({ relevant: Schema.Boolean, reason: Schema.String }),
      }),
      Schema.TaggedStruct("Failure", { error: Schema.instanceOf(AgentError) }),
    ]),
  }),
  Schema.TaggedStruct("ConversationSettled", {
    generation: Schema.String,
    inputId: Schema.String,
    result: Schema.Union([
      Schema.TaggedStruct("Success", { value: Schema.String }),
      Schema.TaggedStruct("Failure", { error: Schema.instanceOf(AgentError) }),
    ]),
  }),
  Schema.TaggedStruct("PeersEnded", {
    generation: Schema.String,
    result: Schema.Union([
      Schema.TaggedStruct("Success", { value: Schema.Void }),
      Schema.TaggedStruct("Failure", { error: Schema.instanceOf(Error) }),
    ]),
  }),
]);
export type GoalMailbox = typeof GoalMailbox.Type;
