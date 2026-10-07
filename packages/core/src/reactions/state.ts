import { ApplicationError, CommandReceipt } from "../operations.js";
import { RecoveryReceipt } from "./contracts.js";
import { Match, Schema } from "effect";
import { ContextEvent, contextEventId } from "../context/model.js";
import { GoalIntentInput } from "../goals/screening/intent.js";
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

/** Only the evidence required to repeat the same target decision. */
export const ReactionCandidate = Schema.Union([
  Schema.TaggedStruct("Signal", {
    slug: Schema.String,
    when: Schema.String,
    version: Schema.Int.check(Schema.isGreaterThan(0)),
  }),
  Schema.TaggedStruct("Goal", {
    slug: Schema.String,
    title: Schema.String,
    description: Schema.String,
    summary: Schema.String,
  }),
]);
export type ReactionCandidate = typeof ReactionCandidate.Type;
export const targetPath = (input: ReactionCandidate): string =>
  `/${input._tag === "Goal" ? "goals" : "signals"}/${input.slug}`;

export const ReactionDecision = Schema.Union([
  Schema.TaggedStruct("Failed", { error: Schema.String }),
  Schema.TaggedStruct("NotMatched", { reason: Schema.String }),
  Schema.TaggedStruct("Matched", { reason: Schema.String, delivery: ReactionDelivery }),
]);
export type ReactionDecision = typeof ReactionDecision.Type;
export const ReactionTarget = Schema.Struct({
  input: ReactionCandidate,
  result: Schema.Union([Schema.TaggedStruct("Pending", {}), ReactionDecision]),
});
export type ReactionTarget = typeof ReactionTarget.Type;
export const ReactionPlan = Schema.Array(
  Schema.Struct({ target: Schema.String, result: ReactionDecision }),
);
export type ReactionPlan = typeof ReactionPlan.Type;

export const FrozenReaction = Schema.Struct({
  status: Schema.Literal("frozen"),
  event: ContextEvent,
  targets: Schema.Array(ReactionTarget),
});
export type FrozenReaction = typeof FrozenReaction.Type;
export const ReactionWork = Schema.Union([
  Schema.Struct({ status: Schema.Literal("queued"), event: ContextEvent }),
  FrozenReaction,
]);
export type ReactionWork = typeof ReactionWork.Type;
export const deliveriesOf = (work: ReactionWork): readonly ReactionDelivery[] =>
  work.status === "queued"
    ? []
    : work.targets.flatMap(({ result }) => (result._tag === "Matched" ? [result.delivery] : []));
const matchOf = ({ input, result }: typeof ReactionTarget.Type) =>
  Match.value(result).pipe(
    Match.tag("Pending", () => []),
    Match.tag("Failed", ({ error }) => [
      { _tag: "Failed" as const, target: targetPath(input), error },
    ]),
    Match.tag("NotMatched", ({ reason }) => [
      { _tag: "NotMatched" as const, target: targetPath(input), reason },
    ]),
    Match.tag("Matched", ({ reason }) => [
      { _tag: "Matched" as const, target: targetPath(input), reason },
    ]),
    Match.exhaustive,
  );
export const matchesOf = (work: ReactionWork) =>
  work.status === "queued" ? [] : work.targets.flatMap<ReturnType<typeof matchOf>[number]>(matchOf);
/** The lifecycle shown in diagnostics is derived, never persisted alongside target state. */
export const workStatus = (work: ReactionWork) => {
  if (work.status === "queued") return "pending";
  if (work.targets.some(({ result }) => result._tag === "Pending")) return "planning";
  if (work.targets.some(({ result }) => result._tag === "Failed")) return "failed";
  if (deliveriesOf(work).some((item) => item.status !== "delivered" && item.status !== "rejected"))
    return "ready";
  return "completed";
};

export const ReactionSnapshot = Schema.Struct({
  work: Schema.Array(ReactionWork),
  sourceRevisions: Schema.Record(Schema.String, Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))),
  recoveryReceipts: Schema.Array(RecoveryReceipt),
}).check(
  Schema.makeFilter(
    ({ work, sourceRevisions, recoveryReceipts }) => {
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
          (sourceRevisions[event.record.path] ?? 0) < event.record.revision ||
          event.id !== contextEventId(event.record.path, event.record.revision)
        )
          return false;
        sources.add(event.id);
        if (item.status === "queued") continue;
        const targets = new Set<string>();
        for (const { input: candidate, result } of item.targets) {
          const target = targetPath(candidate);
          if (targets.has(target)) return false;
          targets.add(target);
          if (result._tag !== "Matched") continue;
          const delivery = result.delivery;
          const { input } = delivery.command;
          if (
            delivery.command._tag !== candidate._tag ||
            input.target !== target ||
            deliveries.has(input.requestId) ||
            input.causationId !== event.id
          )
            return false;
          deliveries.add(input.requestId);
          if (delivery.status === "delivered" && delivery.receipt.requestId !== input.requestId)
            return false;
        }
      }
      return true;
    },
    { expected: "Unique source events, target decisions and matching delivery receipts" },
  ),
);
export type ReactionSnapshot = typeof ReactionSnapshot.Type;
export const ReactionReply = Schema.Union([
  Schema.TaggedStruct("Accepted", { receipt: CommandReceipt }),
  Schema.TaggedStruct("Rejected", { error: ApplicationError }),
]);
export type ReactionReply = typeof ReactionReply.Type;
