import { createHash } from "node:crypto";
import type { ActorRef } from "@aster/actor";
import { ApplicationError } from "@aster/api-contracts";
import { Clock, Context, Effect, Match, Ref, Schema } from "effect";
import type { SystemOneClient } from "../decisions/system-one.js";
import { makeSystemOneGate } from "../signals/detect.js";
import { sourceSignals } from "../signals/policy.js";
import { relevantGoals } from "../goals/relevance.js";
import type { GoalScreeningStore, GoalScreeningRecord } from "../goals/screening.js";
import { makeGoalIntent } from "../goals/intent.js";
import type { GoalsRootCommand } from "../goals/actors.js";
import type { SignalRootCommand } from "../signals/actors.js";
import type {
  ReactionDeliveryInput,
  ReactionPlan,
  ReactionReply,
  ReactionPlanning,
} from "./state.js";

export class ReactionFailure extends Schema.TaggedError<ReactionFailure>()("ReactionFailure", {
  message: Schema.String,
  cause: Schema.optional(Schema.Defect()),
}) {}
export class ReactionPolicy extends Context.Service<
  ReactionPolicy,
  {
    readonly bind: (
      signals: ActorRef<SignalRootCommand>,
      goals?: ActorRef<GoalsRootCommand>,
    ) => Effect.Effect<void>;
    readonly plan: (work: ReactionPlanning) => Effect.Effect<ReactionPlan, ReactionFailure>;
    readonly deliver: (
      command: ReactionDeliveryInput,
    ) => Effect.Effect<ReactionReply, ReactionFailure>;
  }
>()("context/ReactionPolicy") {}

/** Planning only reads frozen evidence. No domain delivery occurs until the plan commits. */
export const makeReactionPolicy = (options: {
  client: SystemOneClient;
  screening?: GoalScreeningStore["Service"] | undefined;
}): Effect.Effect<ReactionPolicy["Service"]> =>
  Effect.gen(function* () {
    const targets = yield* Ref.make<
      | { signals: ActorRef<SignalRootCommand>; goals?: ActorRef<GoalsRootCommand> | undefined }
      | undefined
    >(undefined);
    return ReactionPolicy.of({
      bind: (signals, goals) => Ref.set(targets, { signals, goals }),
      plan: Effect.fn("SystemOne.plan")(
        function* (work) {
          const record = work.event.record;
          const snapshot = { ...work.input.evidence, [record.path]: record };
          const definitions = sourceSignals(snapshot);
          const candidates = yield* makeSystemOneGate(options.client)(record, definitions);
          const commands: ReactionDeliveryInput[] = [];
          const deliveryId = (kind: string, target: string) =>
            createHash("sha256")
              .update(JSON.stringify(["reaction-delivery-v1", work.event.id, kind, target]))
              .digest("hex");
          for (const signal of candidates) {
            const target = `/signals/${signal.slug}`;
            commands.push({
              _tag: "Signal",
              input: {
                requestId: deliveryId("signal", target),
                causationId: work.event.id,
                source: "/system-one",
                target,
                expectedRevision: snapshot[target]?.revision ?? 0,
                sourceContext: record,
              },
            });
          }
          const screenings: GoalScreeningRecord[] = [];
          const clock = yield* Clock.Clock;
          const relevant = yield* relevantGoals(
            options.client,
            record,
            work.input.goals.filter(
              (goal) =>
                snapshot[`/goals/${goal.slug}`]?.projection?.visibility !== "restricted" &&
                (snapshot[`/goals/${goal.slug}`]?.state as { status?: string } | undefined)
                  ?.status === "active",
            ),
            {
              goalRecords: snapshot,
              now: () => clock.currentTimeMillisUnsafe(),
              screening: {
                append: (item) =>
                  Effect.gen(function* () {
                    if (options.screening) yield* options.screening.append(item);
                    screenings.push(item);
                  }),
              },
            },
          );
          for (const goal of relevant) {
            const target = `/goals/${goal.slug}`;
            commands.push({
              _tag: "Goal",
              input: {
                requestId: deliveryId("goal", target),
                causationId: work.event.id,
                source: "/system-one",
                target,
                expectedRevision: snapshot[target]?.revision ?? 0,
                intent: makeGoalIntent(
                  record,
                  goal,
                  new Date(yield* Clock.currentTimeMillis).toISOString(),
                ),
              },
            });
          }
          return { commands, screenings };
        },
        Effect.mapError((error) => new ReactionFailure({ message: error.message, cause: error })),
      ),
      deliver: Effect.fn("SystemOne.deliver")(function* (command) {
        const roots = yield* Ref.get(targets);
        if (!roots) return yield* new ReactionFailure({ message: "Reaction targets not bound" });
        return yield* Match.value(command).pipe(
          Match.tag("Goal", ({ input }) =>
            roots.goals
              ? roots.goals.ask<ReactionReply>((replyTo) => ({
                  _tag: "Route",
                  slug: input.intent.goalSlug,
                  command: {
                    _tag: "SubmitInput",
                    requestId: input.requestId,
                    input: { _tag: "GoalIntent", delivery: input },
                    replyTo,
                  },
                }))
              : Effect.succeed({
                  _tag: "Rejected" as const,
                  error: new ApplicationError({ kind: "not-found", message: "Goals unavailable" }),
                }),
          ),
          Match.tag("Signal", ({ input }) =>
            roots.signals.ask<ReactionReply>((replyTo) => ({ _tag: "React", input, replyTo })),
          ),
          Match.exhaustive,
          Effect.mapError((error) => new ReactionFailure({ message: error.message, cause: error })),
        );
      }),
    });
  });
