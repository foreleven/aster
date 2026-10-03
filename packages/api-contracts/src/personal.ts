import { CausalChain, BusinessNotification } from "./notification.js";
import { ResumeRunDeliveryInput } from "./run-command.js";
import { TaskDeliveryInput, PersonalTaskProposal } from "./task-command.js";
import {
  ApprovalDeliveryInput,
  ApprovalRequestDeliveryInput,
  PersonalApprovalRequestProposal,
} from "./approval-command.js";
import { Schema } from "effect";
import { SignalDeliveryInput, PersonalSignalProposal } from "./signal-command.js";
import { GoalDeliveryInput } from "./delivery.js";
import { CommandReceipt } from "./command.js";

const Identifier = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(256));
const Revision = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));

/** Client retry identity is independent of an RPC transport's request sequence. */
export const PersonalInput = Schema.Struct({
  requestId: Identifier,
  causationId: Identifier,
  expectedRevision: Revision,
  text: Schema.String.check(Schema.isMinLength(1)),
});
export type PersonalInput = typeof PersonalInput.Type;

export const PersonalRetryInput = Schema.Struct({
  requestId: Identifier,
  inputRequestId: Identifier,
  expectedRevision: Revision,
});
export type PersonalRetryInput = typeof PersonalRetryInput.Type;

export const PersonalGoalMessageInput = Schema.Struct({
  requestId: Identifier,
  causationId: Identifier,
  expectedRevision: Revision,
  goalSlug: Schema.String.check(Schema.isPattern(/^[a-z0-9][a-z0-9-]*$/)),
  goalRevision: Revision,
  text: Schema.NonEmptyString,
});
export type PersonalGoalMessageInput = typeof PersonalGoalMessageInput.Type;

export const PersonalOutboxItem = Schema.Struct({
  input: Schema.Union([
    GoalDeliveryInput,
    SignalDeliveryInput,
    ApprovalDeliveryInput,
    ApprovalRequestDeliveryInput,
    TaskDeliveryInput,
    ResumeRunDeliveryInput,
  ]),
  acceptedRevision: Revision,
  attempts: Schema.optional(Revision),
  lastAttemptAt: Schema.optional(Schema.String),
  status: Schema.Literals(["pending", "delivered", "unknown", "rejected"]),
  receipt: Schema.optional(CommandReceipt),
  error: Schema.optional(Schema.String),
});
export type PersonalOutboxItem = typeof PersonalOutboxItem.Type;

export const PersonalResult = Schema.Struct({
  approvalRequests: Schema.optional(
    Schema.Array(PersonalApprovalRequestProposal).check(Schema.isMaxLength(10)),
  ),
  tasks: Schema.optional(Schema.Array(PersonalTaskProposal).check(Schema.isMaxLength(10))),
  signalCommands: Schema.optional(
    Schema.Array(PersonalSignalProposal).check(Schema.isMaxLength(10)),
  ),
  text: Schema.NonEmptyString,
  goalMessages: Schema.optional(
    Schema.Array(
      Schema.Struct({
        goalSlug: Schema.String.check(Schema.isPattern(/^[a-z0-9][a-z0-9-]*$/)),
        goalRevision: Revision,
        text: Schema.String.check(Schema.isPattern(/\S/)),
      }),
    ).check(Schema.isMaxLength(10)),
  ),
});
export type PersonalResult = typeof PersonalResult.Type;

/** Aster business input; this is not a native model transcript message. */
export const PersonalMessage = Schema.Struct({
  requestId: Identifier,
  causationId: Identifier,
  source: Schema.String,
  causal: Schema.optional(CausalChain),
  target: Schema.Literals(["/personal", "user"]),
  revision: Revision,
  sequence: Schema.Int.check(Schema.isGreaterThanOrEqualTo(1)),
  createdAt: Schema.String,
  payload: Schema.Union([
    Schema.TaggedStruct("UserInput", { text: Schema.String }),
    Schema.TaggedStruct("ProgressEvent", {
      text: Schema.String,
      notification: BusinessNotification,
      processing: Schema.Literals(["queued", "display-only"]),
    }),
    Schema.TaggedStruct("AgentReply", { text: Schema.String, inputRequestId: Identifier }),
  ]),
});
export type PersonalMessage = typeof PersonalMessage.Type;

export const PersonalReceipt = Schema.Struct({
  requestId: Identifier,
  revision: Revision,
  sequence: Schema.Int.check(Schema.isGreaterThanOrEqualTo(1)),
});
export type PersonalReceipt = typeof PersonalReceipt.Type;

export const PersonalRun = Schema.Struct({
  requestId: Identifier,
  executionId: Schema.optional(Schema.String),
  revision: Schema.optional(Revision),
  inputSequence: Revision,
  status: Schema.Literals(["running", "completed", "failed"]),
  startedAt: Schema.String,
  error: Schema.optional(Schema.String),
});
export type PersonalRun = typeof PersonalRun.Type;

export const PersonalState = Schema.Struct({
  owner: Schema.Struct({ kind: Schema.Literal("ownerless"), id: Schema.Literal("personal") }),
  pendingRequestIds: Schema.Array(Identifier),
  processedThrough: Revision,
  runs: Schema.optional(Schema.Array(PersonalRun)),
  outbox: Schema.optional(Schema.Array(PersonalOutboxItem)),
});
export type PersonalState = typeof PersonalState.Type;
