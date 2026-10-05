import { Schema } from "effect";
import { CommandIdentifier } from "./command.js";

/** The host carries this budget; models cannot author or replenish it. */
export const CausalChain = Schema.Struct({
  rootRequestId: CommandIdentifier,
  remainingAgentTurns: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 4 })),
});
export type CausalChain = typeof CausalChain.Type;
