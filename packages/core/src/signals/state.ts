import { CausalChain } from "@aster/api-contracts";
import { Schema } from "effect";
import { SignalDefinition } from "../config/schema.js";
import { SignalReactionReceipt, SignalOccurrence } from "./reaction.js";
import { GoalSignalReceipt } from "./goal-command.js";
export const SignalState = Schema.Struct({
  ...SignalDefinition.fields,
  goal: Schema.optional(Schema.String),
  causal: Schema.optional(CausalChain),
  active: Schema.Boolean,
  deleted: Schema.optional(Schema.Boolean),
  revision: Schema.Int,
  nextDue: Schema.optional(Schema.Number.check(Schema.isFinite())),
  timerDone: Schema.optional(Schema.Boolean),
  occurrences: Schema.Array(SignalOccurrence),
  reactionReceipts: Schema.optional(Schema.Array(SignalReactionReceipt)),
  goalCommandReceipts: Schema.optional(Schema.Array(GoalSignalReceipt)),
});
export type SignalState = typeof SignalState.Type;
