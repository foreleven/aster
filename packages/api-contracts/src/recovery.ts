import { Schema } from "effect";
import { CommandIdentifier, CommandReceipt, ContextRevision } from "./command.js";
import { ApplicationError } from "./models.js";

const identity = { requestId: CommandIdentifier, expectedRevision: ContextRevision };
export const RecoveryInput = Schema.Union([
  Schema.TaggedStruct("RetryScreening", { ...identity, workId: CommandIdentifier }),
  Schema.TaggedStruct("RetryReactionDelivery", {
    ...identity,
    workId: CommandIdentifier,
    deliveryId: CommandIdentifier,
  }),
]);
export type RecoveryInput = typeof RecoveryInput.Type;
export const RecoveryReply = Schema.Union([
  Schema.TaggedStruct("Accepted", { receipt: CommandReceipt }),
  Schema.TaggedStruct("Rejected", { error: ApplicationError }),
]);
export type RecoveryReply = typeof RecoveryReply.Type;
export const RecoveryReceipt = Schema.Struct({
  input: RecoveryInput,
  receipt: CommandReceipt,
}).check(
  Schema.makeFilter(
    ({ input, receipt }) =>
      input.requestId === receipt.requestId && receipt.revision === input.expectedRevision + 1,
    { expected: "Receipt for this recovery authorization" },
  ),
);
export type RecoveryReceipt = typeof RecoveryReceipt.Type;

/** Per-target routing decisions; independent of work and delivery lifecycle. */
export const ReactionMatch = Schema.Union([
  Schema.TaggedStruct("Matched", { target: Schema.String, reason: Schema.String }),
  Schema.TaggedStruct("NotMatched", { target: Schema.String, reason: Schema.String }),
  Schema.TaggedStruct("Failed", { target: Schema.String, error: Schema.String }),
]);
export type ReactionMatch = typeof ReactionMatch.Type;

export const ProcessingOwner = Schema.Literal("system-one");
export type ProcessingOwner = typeof ProcessingOwner.Type;
export const ProcessingSnapshot = Schema.Struct({
  owner: ProcessingOwner,
  revision: ContextRevision,
  entries: Schema.Array(
    Schema.Struct({
      id: CommandIdentifier,
      kind: Schema.Literals(["screening", "reaction-delivery"]),
      workId: Schema.optional(CommandIdentifier),
      source: Schema.String,
      target: Schema.String,
      status: Schema.String,
      attempts: Schema.optional(Schema.Int),
      error: Schema.optional(Schema.String),
      matches: Schema.optional(Schema.Array(ReactionMatch)),
    }),
  ),
});
export type ProcessingSnapshot = typeof ProcessingSnapshot.Type;
