import {
  ApplicationError,
  CommandReceipt,
  PublicContext,
  RecoveryReceipt,
} from "@aster/api-contracts";
import { Match, Schema, SchemaGetter } from "effect";
import { ContextEvent } from "../context/model.js";
import { contextEventId } from "../context/model.js";
import { GoalTitle } from "../config/schema.js";
import { GoalIntentInput } from "../goals/intent.js";
import { GoalScreeningRecord } from "../goals/screening.js";
import { SignalReactionInput } from "../signals/reaction.js";
import { LegacyReactionState } from "./legacy-state.js";

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
});
const work = { event: ContextEvent, attempts: Attempts };
export const ReactionPlanning = Schema.Struct({
  ...work,
  status: Schema.Literal("planning"),
  input: ScreeningInput,
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
        recoveryReceipts.some(
          ({ input }) =>
            input._tag === "RetryNotification" ||
            !work.some((item) => item.event.id === input.workId),
        )
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

/** Decode old work once; stable source and delivery identities never change during migration. */
const legacy = LegacyReactionState.pipe(
  Schema.decodeTo(CurrentState, {
    decode: SchemaGetter.transform((state) => ({
      recoveryReceipts: state.recoveryReceipts,
      work: state.work.map((item) => {
        const event = {
          id: item.event.requestId,
          record: { ...item.event.record, revision: item.event.revision },
          createdAt: item.event.createdAt,
        };
        const base = { event, attempts: item.attempts };
        const { [event.record.path]: _source, ...evidence } = item.snapshot;
        const input = { evidence, goals: item.goals, screeningAt: item.admittedAt };
        const deliveries: readonly ReactionDelivery[] = (item.deliveries ?? []).map((delivery) => {
          const base = { command: delivery.command, attempts: delivery.attempts };
          return Match.value(delivery.status).pipe(
            Match.when("pending", () => ({ ...base, status: "pending" as const })),
            Match.when("sending", () => ({ ...base, status: "sending" as const })),
            Match.when("unknown", () => ({
              ...base,
              status: "unknown" as const,
              error: delivery.error ?? "Delivery outcome unknown",
            })),
            Match.when("rejected", () => ({
              ...base,
              status: "rejected" as const,
              error: delivery.error ?? "Delivery rejected",
            })),
            // LegacyReactionState already verifies delivered receipt identity.
            Match.when("delivered", () => ({
              ...base,
              status: "delivered" as const,
              receipt: delivery.receipt!,
            })),
            Match.exhaustive,
          );
        });
        return Match.value(item.status).pipe(
          Match.when("pending", () => ({ ...base, status: "pending" as const })),
          Match.when("planning", () => ({ ...base, status: "planning" as const, input })),
          Match.when("failed", () => ({
            ...base,
            status: "failed" as const,
            input,
            error: item.error ?? "Screening failed",
          })),
          Match.when("ready", () => ({
            ...base,
            status: "ready" as const,
            screenings: item.screenings ?? [],
            deliveries,
          })),
          Match.when("completed", () => ({
            ...base,
            status: "completed" as const,
            screenings: item.screenings ?? [],
            deliveries,
          })),
          Match.exhaustive,
        );
      }),
    })),
    encode: SchemaGetter.forbiddenEncoding,
  }),
);
export const ReactionState = Schema.Union([CurrentState, legacy]);
export type ReactionState = typeof CurrentState.Type;
export const ReactionReply = Schema.Union([
  Schema.TaggedStruct("Accepted", { receipt: CommandReceipt }),
  Schema.TaggedStruct("Rejected", { error: ApplicationError }),
]);
export type ReactionReply = typeof ReactionReply.Type;
