import {
  ApplicationError,
  CommandReceipt,
  PublicContext,
  RecoveryReceipt,
} from "@aster/api-contracts";
import { Schema } from "effect";
import { isDeepStrictEqual } from "node:util";
import { reactionEventId } from "./reaction-event.js";
import { ContextReactionEvent } from "./model.js";
import { GoalTitle } from "../config/schema.js";
import { GoalIntentInput } from "../goals/intent.js";
import { GoalScreeningRecord } from "../goals/screening.js";
import { SignalReactionInput } from "../signals/reaction.js";

const GoalDefinition = Schema.Struct({
  slug: Schema.String,
  title: Schema.optional(GoalTitle),
  description: Schema.String,
  completionCriteria: Schema.optional(Schema.String),
});

export const ReactionDeliveryInput = Schema.Union([
  Schema.TaggedStruct("Goal", { input: GoalIntentInput }),
  Schema.TaggedStruct("Signal", { input: SignalReactionInput }),
]);
export type ReactionDeliveryInput = typeof ReactionDeliveryInput.Type;
export const ReactionDelivery = Schema.Struct({
  command: ReactionDeliveryInput,
  status: Schema.Literals(["pending", "sending", "unknown", "delivered", "rejected"]),
  attempts: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  receipt: Schema.optional(CommandReceipt),
  error: Schema.optional(Schema.String),
});
export const ReactionPlan = Schema.Struct({
  screenings: Schema.Array(GoalScreeningRecord),
  commands: Schema.Array(ReactionDeliveryInput),
});
export type ReactionPlan = typeof ReactionPlan.Type;
export const ReactionWork = Schema.Struct({
  event: ContextReactionEvent,
  snapshot: Schema.Record(Schema.String, PublicContext),
  goals: Schema.Array(GoalDefinition),
  admittedAt: Schema.String,
  status: Schema.Literals(["pending", "planning", "ready", "failed", "completed"]),
  attempts: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  error: Schema.optional(Schema.String),
  screenings: Schema.optional(Schema.Array(GoalScreeningRecord)),
  deliveries: Schema.optional(Schema.Array(ReactionDelivery)),
});
export type ReactionWork = typeof ReactionWork.Type;
export const ReactionState = Schema.Struct({
  work: Schema.Array(ReactionWork),
  recoveryReceipts: Schema.optional(Schema.Array(RecoveryReceipt)),
}).check(
  Schema.makeFilter(
    ({ work, recoveryReceipts = [] }) => {
      if (
        new Set(recoveryReceipts.map((entry) => entry.input.requestId)).size !==
          recoveryReceipts.length ||
        recoveryReceipts.some(
          ({ input }) =>
            input._tag === "RetryNotification" ||
            !work.some((item) => item.event.requestId === input.workId),
        )
      )
        return false;
      const sources = new Set<string>();
      const deliveries = new Set<string>();
      for (const item of work) {
        const event = item.event;
        if (
          !Number.isFinite(Date.parse(event.createdAt)) ||
          !Number.isFinite(Date.parse(item.admittedAt))
        )
          return false;
        if (sources.has(event.requestId)) return false;
        sources.add(event.requestId);
        if (event.requestId !== reactionEventId(event.source, event.revision)) return false;
        if (
          event.causationId !== event.requestId ||
          event.record.path !== event.source ||
          event.record.revision !== event.revision
        )
          return false;
        if (
          item.status !== "pending" &&
          !isDeepStrictEqual(item.snapshot[event.source], event.record)
        )
          return false;
        if (item.status === "ready" && !item.deliveries?.length) return false;
        if (
          item.status === "completed" &&
          (!item.deliveries ||
            item.deliveries.some((d) => d.status !== "delivered" && d.status !== "rejected"))
        )
          return false;
        for (const delivery of item.deliveries ?? []) {
          const input = delivery.command.input;
          if (deliveries.has(input.requestId) || input.causationId !== event.causationId)
            return false;
          deliveries.add(input.requestId);
          if (delivery.status === "delivered" && delivery.receipt?.requestId !== input.requestId)
            return false;
        }
      }
      return true;
    },
    { expected: "Unique reaction work with frozen source evidence and matching delivery receipts" },
  ),
);
export type ReactionState = typeof ReactionState.Type;
export const ReactionReply = Schema.Union([
  Schema.TaggedStruct("Accepted", { receipt: CommandReceipt }),
  Schema.TaggedStruct("Rejected", { error: ApplicationError }),
]);
export type ReactionReply = typeof ReactionReply.Type;
