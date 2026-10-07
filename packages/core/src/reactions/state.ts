import {
  ApplicationError,
  CommandReceipt,
  PublicContext,
  RecoveryReceipt,
} from "@aster/api-contracts";
import { Schema } from "effect";
import { ContextEvent } from "../context/model.js";
import { contextEventId } from "../context/model.js";
import { GoalTitle } from "../config/schema.js";
import { GoalIntentInput } from "../goals/screening/intent.js";
import { GoalScreeningRecord } from "../goals/screening/decision.js";
import { SignalReactionInput } from "../signals/protocol.js";

const Attempts = Schema.Int.check(Schema.isGreaterThanOrEqualTo(0));
export const ReactionDeliveryInput = Schema.Union([
  Schema.TaggedStruct("Goal", { input: GoalIntentInput }),
  Schema.TaggedStruct("Signal", { input: SignalReactionInput }),
]);
export type ReactionDeliveryInput = typeof ReactionDeliveryInput.Type;
const delivery = { command: ReactionDeliveryInput, attempts: Attempts };
export const ReactionDelivery = Schema.Union([
  Schema.Struct({ ...delivery, status: Schema.Literal("pending") }),
  Schema.Struct({ ...delivery, status: Schema.Literal("sending") }),
  Schema.Struct({ ...delivery, status: Schema.Literal("unknown"), error: Schema.String }),
  Schema.Struct({ ...delivery, status: Schema.Literal("rejected"), error: Schema.String }),
  Schema.Struct({ ...delivery, status: Schema.Literal("delivered"), receipt: CommandReceipt }),
]);
export type ReactionDelivery = typeof ReactionDelivery.Type;
export const ReactionPlan = Schema.Struct({
  screenings: Schema.Array(GoalScreeningRecord),
  commands: Schema.Array(ReactionDeliveryInput),
  failures: Schema.Array(Schema.Struct({ target: Schema.String, error: Schema.String })),
});
export type ReactionPlan = typeof ReactionPlan.Type;
const ScreeningInput = Schema.Struct({
  evidence: Schema.Record(Schema.String, PublicContext),
  goals: Schema.Array(
    Schema.Struct({
      slug: Schema.String,
      title: Schema.optional(GoalTitle),
      description: Schema.String,
      completionCriteria: Schema.optional(Schema.String),
    }),
  ),
  screeningAt: Schema.String,
  targets: Schema.optional(Schema.Array(Schema.String)),
});
const work = { event: ContextEvent, attempts: Attempts };
export const ReactionPlanning = Schema.Struct({
  ...work,
  status: Schema.Literal("planning"),
  input: ScreeningInput,
  retained: Schema.optional(
    Schema.Struct({
      screenings: Schema.Array(GoalScreeningRecord),
      deliveries: Schema.Array(ReactionDelivery),
    }),
  ),
});
export type ReactionPlanning = typeof ReactionPlanning.Type;
export const ReactionWork = Schema.Union([
  Schema.Struct({ ...work, status: Schema.Literal("pending") }),
  ReactionPlanning,
  Schema.Struct({
    ...work,
    status: Schema.Literal("failed"),
    input: ScreeningInput,
    error: Schema.String,
    failedTargets: Schema.Array(Schema.String),
    screenings: Schema.Array(GoalScreeningRecord),
    deliveries: Schema.Array(ReactionDelivery),
  }),
  Schema.Struct({
    ...work,
    status: Schema.Literal("ready"),
    screenings: Schema.Array(GoalScreeningRecord),
    deliveries: Schema.Array(ReactionDelivery).check(Schema.isMinLength(1)),
  }),
  Schema.Struct({
    ...work,
    status: Schema.Literal("completed"),
    screenings: Schema.Array(GoalScreeningRecord),
    deliveries: Schema.Array(ReactionDelivery),
  }),
]);
export type ReactionWork = typeof ReactionWork.Type;
export const deliveriesOf = (work: ReactionWork): readonly ReactionDelivery[] =>
  "deliveries" in work ? work.deliveries : [];
const CurrentState = Schema.Struct({
  work: Schema.Array(ReactionWork),
  recoveryReceipts: Schema.optional(Schema.Array(RecoveryReceipt)),
}).check(
  Schema.makeFilter(
    ({ work, recoveryReceipts = [] }) => {
      if (
        new Set(recoveryReceipts.map((entry) => entry.input.requestId)).size !==
          recoveryReceipts.length ||
        recoveryReceipts.some(({ input }) => !work.some((item) => item.event.id === input.workId))
      )
        return false;
      const sources = new Set<string>();
      const deliveries = new Set<string>();
      for (const item of work) {
        const event = item.event;
        if (
          !Number.isFinite(Date.parse(event.createdAt)) ||
          sources.has(event.id) ||
          event.id !== contextEventId(event.record.path, event.record.revision)
        )
          return false;
        sources.add(event.id);
        if (
          "input" in item &&
          (!Number.isFinite(Date.parse(item.input.screeningAt)) ||
            event.record.path in item.input.evidence)
        )
          return false;
        for (const delivery of deliveriesOf(item)) {
          const input = delivery.command.input;
          if (deliveries.has(input.requestId) || input.causationId !== event.id) return false;
          deliveries.add(input.requestId);
          if (delivery.status === "delivered" && delivery.receipt.requestId !== input.requestId)
            return false;
          if (
            item.status === "completed" &&
            delivery.status !== "delivered" &&
            delivery.status !== "rejected"
          )
            return false;
        }
      }
      return true;
    },
    { expected: "Unique source evidence, frozen screening input and matching terminal receipts" },
  ),
);

export const ReactionSnapshot = CurrentState;
export type ReactionSnapshot = typeof CurrentState.Type;
export const ReactionReply = Schema.Union([
  Schema.TaggedStruct("Accepted", { receipt: CommandReceipt }),
  Schema.TaggedStruct("Rejected", { error: ApplicationError }),
]);
export type ReactionReply = typeof ReactionReply.Type;
