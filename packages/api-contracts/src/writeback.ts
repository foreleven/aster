import { Schema } from "effect";
import { CommandIdentifier } from "./command.js";
import { CausalChain } from "./causal.js";

/** Publish the committed result verbatim. No credentials, templates or arbitrary
 * tool names can be supplied as part of a Task action. */
export const TaskAction = Schema.TaggedStruct("PublishResult", {
  channelPath: Schema.String.check(Schema.isPattern(/^\/[a-zA-Z0-9_/-]+$/)),
  identity: Schema.Literals(["user", "bot"]),
});
export type TaskAction = typeof TaskAction.Type;
const Timestamp = Schema.String.check(
  Schema.makeFilter(
    (value) => /(Z|[+-]\d{2}:\d{2})$/.test(value) && Number.isFinite(Date.parse(value)),
    { expected: "An absolute timestamp" },
  ),
);
export const WritebackRequest = Schema.Struct({
  requestId: CommandIdentifier,
  source: Schema.String,
  taskSource: Schema.String,
  causationId: CommandIdentifier,
  createdAt: Timestamp,
  action: TaskAction,
  content: Schema.NonEmptyString,
  causal: CausalChain,
});
export type WritebackRequest = typeof WritebackRequest.Type;
export const WritebackAuthorization = Schema.Struct({
  approvalId: CommandIdentifier,
  approvalsRevision: Schema.Int.check(Schema.isGreaterThan(0)),
  approvedAt: Timestamp,
});
export const WritebackOperation = Schema.Struct({
  request: WritebackRequest,
  status: Schema.Literals([
    "waiting-approval",
    "authorized",
    "sending",
    "published",
    "rejected",
    "unknown",
  ]),
  authorization: Schema.optional(WritebackAuthorization),
  submittedAt: Schema.optional(Timestamp),
  externalId: Schema.optional(Schema.NonEmptyString),
  error: Schema.optional(Schema.String),
}).check(
  Schema.makeFilter(
    (operation) => {
      const { status, authorization, submittedAt, externalId } = operation;
      if (authorization && authorization.approvalId !== writebackApprovalId(operation.request))
        return false;
      if (operation.request.causal.remainingAgentTurns !== 0) return false;
      if (submittedAt && !authorization) return false;
      if (status === "authorized" && submittedAt !== undefined) return false;
      if (["authorized", "sending", "published", "unknown"].includes(status) && !authorization)
        return false;
      if (["sending", "published", "unknown"].includes(status) && !submittedAt) return false;
      if (status === "published" && !externalId) return false;
      if (status !== "published" && externalId !== undefined) return false;
      return status !== "waiting-approval" || (!authorization && !submittedAt);
    },
    { expected: "Writeback phase with its committed authorization, submission intent and outcome" },
  ),
);
export type WritebackOperation = typeof WritebackOperation.Type;
export const writebackApprovalId = (request: WritebackRequest): string =>
  `${request.source}:writeback:${request.requestId}`;
export const writebackPrompt = (request: WritebackRequest): string =>
  `Publish this exact result to ${request.action.channelPath} as ${request.action.identity}?\n\n${request.content}\n\nSource: ${request.source}\nOperation: ${request.requestId}\nThis approval applies only to this destination, identity and content. Task approval does not authorize publication.`;
