import { GoalIntentInput } from "./screening/intent.js";
import { ApplicationError, CommandReceipt } from "../operations.js";
import { GoalExecutionFeedback } from "./contracts.js";
import { TaskMessage, TaskPath } from "../tasks/contracts.js";
import { Predicate, Schema } from "effect";
import { ReplyTo } from "@aster/actor";
import { publicJson } from "../json.js";
import { createHash } from "node:crypto";
import { AgentError } from "@aster/agent";

/** Producer-specific envelopes preserve provenance; only UserInput crosses public ingress. */
export const GoalSubmission = Schema.TaggedUnion({
  TaskMessage: { delivery: TaskMessage },
  UserInput: { text: Schema.NonEmptyString },
  GoalIntent: { delivery: GoalIntentInput },
  ExecutionFeedback: GoalExecutionFeedback.fields,
});
export type GoalSubmission = typeof GoalSubmission.Type;
const submit = { requestId: Schema.NonEmptyString, input: GoalSubmission };
const end = { requestId: Schema.NonEmptyString };
const retry = { requestId: Schema.NonEmptyString, turnId: Schema.NonEmptyString };
export const GoalRequestData = Schema.TaggedUnion({
  SubmitInput: submit,
  End: end,
  RetryTurn: retry,
});
export type GoalRequestData = typeof GoalRequestData.Type;
export const GoalCommandReply = Schema.TaggedUnion({
  Accepted: { receipt: CommandReceipt },
  Rejected: { error: ApplicationError },
});
export type GoalCommandReply = typeof GoalCommandReply.Type;
const replyTo = ReplyTo<GoalCommandReply>();
export const GoalCommand = Schema.TaggedUnion({
  SubmitInput: { ...submit, replyTo },
  End: { ...end, replyTo },
  RetryTurn: { ...retry, replyTo },
});
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
export const GoalTaskReply = Schema.TaggedUnion({
  Attached: {},
  Rejected: { error: ApplicationError },
});
export type GoalTaskReply = typeof GoalTaskReply.Type;

export const GoalMailbox = Schema.TaggedUnion({
  SubmitInput: GoalCommand.cases.SubmitInput.fields,
  End: GoalCommand.cases.End.fields,
  RetryTurn: GoalCommand.cases.RetryTurn.fields,
  AttachTask: {
    taskPath: TaskPath,
    replyTo: ReplyTo<GoalTaskReply>(),
  },
  RunNext: {},
  GateSettled: {
    generation: Schema.String,
    inputId: Schema.String,
    result: Schema.TaggedUnion({
      Success: {
        value: Schema.Struct({ relevant: Schema.Boolean, reason: Schema.String }),
      },
      Failure: { error: Schema.instanceOf(AgentError) },
    }),
  },
  ConversationSettled: {
    generation: Schema.String,
    inputId: Schema.String,
    result: Schema.TaggedUnion({
      Success: { value: Schema.String },
      Failure: { error: Schema.instanceOf(AgentError) },
    }),
  },
  PeersEnded: {
    generation: Schema.String,
    result: Schema.TaggedUnion({
      Success: { value: Schema.Void },
      Failure: { error: Schema.instanceOf(Error) },
    }),
  },
});
export type GoalMailbox = typeof GoalMailbox.Type;
