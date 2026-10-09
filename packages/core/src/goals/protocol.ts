import type { MailboxOf } from "@aster/actor";
import { Command as ActorCommand } from "@aster/actor";
import { AgentError } from "@aster/agent";
import { Predicate, Schema } from "effect";
import { createHash } from "node:crypto";
import { publicJson } from "../json.js";
import { ApplicationError, CommandReceipt } from "../operations.js";
import { TaskMessage, TaskPath } from "../tasks/contracts.js";
import { GoalExecutionFeedback } from "./contracts.js";
import { GoalIntentInput } from "./screening/intent.js";

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
export class SubmitInput extends ActorCommand.Class<SubmitInput>()("SubmitInput", {
  payload: { ...submit },
  reply: GoalCommandReply,
}) {}
export class End extends ActorCommand.Class<End>()("End", {
  payload: { ...end },
  reply: GoalCommandReply,
}) {}
export class RetryTurn extends ActorCommand.Class<RetryTurn>()("RetryTurn", {
  payload: { ...retry },
  reply: GoalCommandReply,
}) {}
export const GoalCommand = Schema.TaggedUnion({
  SubmitInput: SubmitInput.fields,
  End: End.fields,
  RetryTurn: RetryTurn.fields,
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

export class AttachTask extends ActorCommand.Class<AttachTask>()("AttachTask", {
  payload: { taskPath: TaskPath },
  reply: GoalTaskReply,
}) {}
export const GoalCommands = [SubmitInput, End, RetryTurn, AttachTask] as const;
export const GoalInternal = Schema.TaggedUnion({
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
export type GoalMailbox = MailboxOf<typeof GoalCommands, typeof GoalInternal>;
