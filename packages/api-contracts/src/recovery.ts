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
  Schema.TaggedStruct("RetryNotification", { ...identity, deliveryId: CommandIdentifier }),
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

export const ProcessingOwner = Schema.Literals(["system-one", "notifications"]);
export type ProcessingOwner = typeof ProcessingOwner.Type;
export const ProcessingSnapshot = Schema.Struct({
  owner: ProcessingOwner,
  revision: ContextRevision,
  entries: Schema.Array(
    Schema.Struct({
      id: CommandIdentifier,
      kind: Schema.Literals(["screening", "reaction-delivery", "notification"]),
      workId: Schema.optional(CommandIdentifier),
      source: Schema.String,
      target: Schema.String,
      status: Schema.String,
      attempts: Schema.Int,
      error: Schema.optional(Schema.String),
    }),
  ),
});
export type ProcessingSnapshot = typeof ProcessingSnapshot.Type;
