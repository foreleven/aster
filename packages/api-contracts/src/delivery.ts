import { CausalChain } from "./notification.js";
import { Schema } from "effect";
import { CommandReceipt } from "./command.js";

const Identifier = Schema.NonEmptyString;
const Revision = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));

/** A committed Personal operation addressed to one Goal owner. */
export const GoalDeliveryInput = Schema.Struct({
  operation: Schema.optional(Schema.Literal("sendGoalMessage")),
  requestId: Identifier,
  causationId: Identifier,
  source: Schema.Literal("/personal"),
  causal: Schema.optional(CausalChain),
  target: Schema.String.check(Schema.isPattern(/^\/goals\/[a-z0-9][a-z0-9-]*$/)),
  expectedRevision: Revision,
  createdAt: Schema.NonEmptyString,
  text: Schema.NonEmptyString,
});
export type GoalDeliveryInput = typeof GoalDeliveryInput.Type;

export const GoalDeliveryReceipt = CommandReceipt;
export type GoalDeliveryReceipt = typeof GoalDeliveryReceipt.Type;

export const GoalDelivery = Schema.Struct({
  input: GoalDeliveryInput,
  receipt: GoalDeliveryReceipt,
  /** Projection to the compatibility history is recoverable and idempotent. */
  historySequence: Schema.optional(Revision),
});
export type GoalDelivery = typeof GoalDelivery.Type;
