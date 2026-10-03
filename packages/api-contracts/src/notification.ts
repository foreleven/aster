import { Schema } from "effect";
import { CommandIdentifier, ContextRevision } from "./command.js";

/** The host carries this budget; models cannot author or replenish it. */
export const CausalChain = Schema.Struct({
  rootRequestId: CommandIdentifier,
  remainingAgentTurns: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 4 })),
});
export type CausalChain = typeof CausalChain.Type;

export const BusinessNotification = Schema.Struct({
  requestId: CommandIdentifier,
  causationId: CommandIdentifier,
  source: Schema.String.check(Schema.isPattern(/^\//)),
  target: Schema.Literal("/personal"),
  revision: ContextRevision,
  createdAt: Schema.String,
  causal: CausalChain,
  kind: Schema.Literals(["RunResult", "NeedsAttention", "GoalProgress", "SignalMatched"]),
  text: Schema.NonEmptyString,
});
export type BusinessNotification = typeof BusinessNotification.Type;
