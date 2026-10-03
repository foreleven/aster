import { BusinessNotification, CausalChain } from "@aster/api-contracts";
import { SignalReactionReceipt } from "./reaction.js";
import { GoalSignalReceipt } from "./goal-command.js";
import { Schema } from "effect";
import { SignalDefinition } from "../config/schema.js";
import { ContextRecord } from "../context/model.js";

const Occurrence = Schema.Struct({
  causal: Schema.optional(CausalChain),
  id: Schema.String,
  text: Schema.String,
  delivered: Schema.Boolean,
  source: ContextRecord,
});

/** Delivery flags and timer revisions are executable recovery state, not arbitrary metadata. */
export const SignalState = Schema.Struct({
  goalCommandReceipts: Schema.optional(Schema.Array(GoalSignalReceipt)),
  businessOutbox: Schema.optional(Schema.Array(BusinessNotification)),
  causal: Schema.optional(CausalChain),
  ...SignalDefinition.fields,
  goal: Schema.optional(Schema.String),
  active: Schema.optional(Schema.Boolean),
  deleted: Schema.optional(Schema.Boolean),
  revision: Schema.optional(Schema.Int),
  reactionReceipts: Schema.optional(Schema.Array(SignalReactionReceipt)),
  seenSources: Schema.optional(Schema.Array(Schema.String)),
  nextDue: Schema.optional(Schema.Number.check(Schema.isFinite())),
  timerDone: Schema.optional(Schema.Boolean),
  occurrences: Schema.optional(Schema.Array(Occurrence)),
}).check(
  Schema.makeFilter(
    (state) => {
      const receipts = state.goalCommandReceipts ?? [];
      return (
        !(state.goal !== undefined && state.action !== undefined) &&
        new Set(receipts.map((entry) => entry.input.requestId)).size === receipts.length &&
        receipts.every(
          (entry) =>
            entry.input.target === `/signals/${state.slug}` &&
            entry.input.source === `/goals/${state.goal}`,
        )
      );
    },
    { expected: "Unique Goal Signal receipts belonging to this Signal owner" },
  ),
);
export type SignalState = typeof SignalState.Type;
