import { createHash } from "node:crypto";
import { publicJson } from "../context/json.js";
import { ReplyTo } from "@aster/actor";
import { Predicate, Schema } from "effect";
import { ApplicationError, CausalChain, TaskMessage, CommandReceipt } from "@aster/api-contracts";
import { GoalIntentInput } from "./intent.js";

/** Producer-specific envelopes preserve provenance; only UserInput crosses public ingress. */
export const GoalSubmission = Schema.Union([
  Schema.TaggedStruct("TaskMessage", { delivery: TaskMessage }),
  Schema.TaggedStruct("UserInput", { text: Schema.NonEmptyString }),
  Schema.TaggedStruct("GoalIntent", { delivery: GoalIntentInput }),
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
