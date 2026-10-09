import { Schema } from "effect";
import {
  CommandIdentifier,
  CommandReceipt,
  ContextRevision,
  ApplicationError,
} from "../operations.js";

const identity = { requestId: CommandIdentifier, expectedRevision: ContextRevision };
export const RecoveryInput = Schema.TaggedUnion({
  RetryScreening: { ...identity, workId: CommandIdentifier },
  RetryReactionDelivery: {
    ...identity,
    workId: CommandIdentifier,
    deliveryId: CommandIdentifier,
  },
});
export type RecoveryInput = typeof RecoveryInput.Type;
export const RecoveryReply = Schema.TaggedUnion({
  Accepted: { receipt: CommandReceipt },
  Rejected: { error: ApplicationError },
});
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
