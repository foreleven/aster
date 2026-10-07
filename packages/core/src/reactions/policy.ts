import { CurrentActors } from "../tools/actors.js";
import { createHash } from "node:crypto";
import type { ActorRef } from "@aster/actor";
import { type PublicContext } from "@aster/api-contracts";
import { Clock, Context, Effect, Match, Schema } from "effect";
import { choice, type SystemOneClient } from "../decisions/system-one.js";
import { SignalSnapshot, signalEnabled } from "../signals/state/snapshot.js";
import type { SignalDefinition } from "../config/schema.js";
import { relevantGoals } from "../goals/screening/decision.js";
import type { GoalScreeningStore, GoalScreeningRecord } from "../goals/screening/decision.js";
import { makeGoalIntent } from "../goals/screening/intent.js";
import type { GoalsRootCommand } from "../goals/root.js";
import type { SignalRootCommand } from "../signals/protocol.js";
import type {
  ReactionDeliveryInput,
  ReactionPlan,
  ReactionReply,
  ReactionPlanning,
} from "./state.js";

export const sourceSignals = (snapshot: Readonly<Record<string, PublicContext>>) =>
  Object.values(snapshot)
    .filter(
      (record) =>
        /^\/signals\/[^/]+$/.test(record.path) && record.projection?.visibility !== "restricted",
    )
    .flatMap((record) => {
      const state = Schema.decodeUnknownSync(SignalSnapshot)(record.state);
      return state.trigger._tag === "Context" && signalEnabled(state, (path) => snapshot[path])
        ? [
            {
              slug: record.path.slice("/signals/".length),
              trigger: state.trigger,
              task: state.task,
            },
          ]
        : [];
    });

/** One decision-model pass matches every active Context Signal. Timers never enter this path. */
export const makeSystemOneGate =
  (client: SystemOneClient) => (record: PublicContext, signals: readonly SignalDefinition[]) =>
    Effect.gen(function* () {
      if (!signals.length) return [];
      const questions = Object.fromEntries(
        signals.map((signal, index) => [
          `signal_${index}`,
          choice(
            `Does this Context satisfy the Signal condition? ${signal.trigger._tag === "Context" ? signal.trigger.when : "Not a Context Signal"}`,
            {
              yes: "Evidence satisfies the condition; execute its Task.",
              no: "The condition is not satisfied.",
            },
          ),
        ]),
      );
      const response = yield* client.systemOne({
        state: JSON.stringify({ context: record }),
        questions,
      });
      return signals.filter((_, index) => {
        const answer = response.answers[`signal_${index}`];
        return answer?.type === "choice" && answer.choice === "yes";
      });
    });

export class ReactionFailure extends Schema.TaggedError<ReactionFailure>()("ReactionFailure", {
  message: Schema.String,
  cause: Schema.optional(Schema.Defect()),
}) {}
export class ReactionPolicy extends Context.Service<
  ReactionPolicy,
  {
    readonly plan: (work: ReactionPlanning) => Effect.Effect<ReactionPlan, ReactionFailure>;
    readonly deliver: (
      command: ReactionDeliveryInput,
    ) => Effect.Effect<ReactionReply, ReactionFailure, CurrentActors>;
  }
>()("context/ReactionPolicy") {}

/** Planning only reads frozen evidence. No domain delivery occurs until the plan commits. */
export const makeReactionPolicy = (options: {
  client: SystemOneClient;
  screening?: GoalScreeningStore["Service"] | undefined;
}): Effect.Effect<ReactionPolicy["Service"]> =>
  Effect.sync(() => {
    return ReactionPolicy.of({
      plan: Effect.fn("SystemOne.plan")(function* (work) {
        const record = work.event.record;
        const snapshot = { ...work.input.evidence, [record.path]: record };
        const selected = (target: string) =>
          !work.input.targets || work.input.targets.includes(target);
        const failures: ReactionPlan["failures"][number][] = [];
        const definitions = sourceSignals(snapshot).filter((signal) =>
          selected(`/signals/${signal.slug}`),
        );
        const matched = yield* makeSystemOneGate(options.client)(record, definitions).pipe(
          Effect.result,
        );
        const candidates = matched._tag === "Success" ? matched.success : [];
        if (matched._tag === "Failure")
          for (const signal of definitions)
            failures.push({ target: `/signals/${signal.slug}`, error: matched.failure.message });
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
        const goals = work.input.goals.filter((goal) => {
          const target = `/goals/${goal.slug}`;
          const current = snapshot[target];
          return (
            selected(target) &&
            current?.projection?.visibility !== "restricted" &&
            (current?.state as { status?: string } | undefined)?.status === "active"
          );
        });
        const matches = yield* Effect.forEach(
          goals,
          (goal) =>
            relevantGoals(options.client, record, [goal], {
              goalRecords: snapshot,
              now: () => clock.currentTimeMillisUnsafe(),
              screening: {
                append: (item) =>
                  Effect.gen(function* () {
                    if (options.screening) yield* options.screening.append(item);
                    screenings.push(item);
                  }),
              },
            }).pipe(Effect.result),
          { concurrency: 4 },
        );
        const relevant = matches.flatMap((result, index) => {
          if (result._tag === "Success") return result.success;
          failures.push({ target: `/goals/${goals[index]!.slug}`, error: result.failure.message });
          return [];
        });
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
        return { commands, screenings, failures };
      }),
      deliver: Effect.fn("SystemOne.deliver")(function* (command) {
        const actors = yield* CurrentActors;
        const path = command._tag === "Goal" ? "/user/goals" : "/user/signals";
        const root = yield* actors
          .select(path)
          .resolve()
          .pipe(
            Effect.mapError(
              (error) => new ReactionFailure({ message: error.message, cause: error }),
            ),
          );
        return yield* Match.value(command).pipe(
          Match.tag("Goal", ({ input }) =>
            (root as ActorRef<GoalsRootCommand>).ask<ReactionReply>((replyTo) => ({
              _tag: "Route",
              slug: input.intent.goalSlug,
              command: {
                _tag: "SubmitInput",
                requestId: input.requestId,
                input: { _tag: "GoalIntent", delivery: input },
                replyTo,
              },
            })),
          ),
          Match.tag("Signal", ({ input }) =>
            (root as ActorRef<SignalRootCommand>).ask<ReactionReply>((replyTo) => ({
              _tag: "React",
              input,
              replyTo,
            })),
          ),
          Match.exhaustive,
          Effect.mapError((error) => new ReactionFailure({ message: error.message, cause: error })),
        );
      }),
    });
  });
