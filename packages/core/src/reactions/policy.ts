import { CurrentActors } from "../services/actors.js";
import { createHash } from "node:crypto";
import type { DurableContext } from "../context/store.js";

import { loadGoalState } from "../goals/queries.js";
import { SignalRecord } from "../signals/queries.js";
import type { ActorRef } from "@aster/actor";
import { type ContextText } from "../context/contracts.js";
import { Clock, Context, Effect, Match, Schema } from "effect";
import { choice, type SystemOneClient } from "../services/system-one.js";
import { SignalSnapshot, signalEnabled } from "../signals/state/snapshot.js";
import type { GoalDefinition } from "../config/schema.js";
import { matchGoal, goalTitleText, type GoalScreeningStore } from "../goals/screening/decision.js";

import { makeGoalIntent } from "../goals/screening/intent.js";
import type { GoalsRootCommand } from "../goals/root.js";
import type { SignalRootCommand } from "../signals/protocol.js";
import {
  targetPath,
  type ReactionDeliveryInput,
  type ReactionPlan,
  type ReactionDecision,
  type ReactionReply,
  type FrozenReaction,
  type ReactionCandidate,
} from "./state.js";

export const sourceSignals = (
  signals: readonly SignalRecord[],
  goals: ReadonlyMap<string, { status: string }>,
) =>
  signals.flatMap((record) => {
    const state = record.state;
    return state.trigger._tag === "Context" && signalEnabled(state, (path) => goals.get(path))
      ? [
          {
            _tag: "Signal" as const,
            slug: record.path.slice("/signals/".length),
            when: state.trigger.when,
            version: state.version,
          },
        ]
      : [];
  });

/** Freeze domain rules from schema-decoded committed state; serialized Context text is evidence only. */
export const reactionTargets = Effect.fn("Reactions.targets")(function* (
  durable: DurableContext["Service"],
  definitions: readonly GoalDefinition[],
) {
  const goals = new Map<string, Awaited<Effect.Success<ReturnType<typeof loadGoalState>>>>();
  for (const definition of definitions) {
    const path = `/goals/${definition.slug}`;
    if (!durable.get(path)) continue;
    const state = yield* loadGoalState(durable, path).pipe(Effect.option);
    if (state._tag === "Some") goals.set(path, state.value);
  }
  const signals = yield* Effect.forEach(
    durable.directory().filter(({ path }) => /^\/signals\/[^/]+$/.test(path)),
    ({ path }) =>
      durable
        .readState(path, SignalSnapshot)
        .pipe(Effect.map((state) => ({ ...durable.get(path)!, state })))
        .pipe(Effect.option),
    { concurrency: 4 },
  );
  return [
    ...sourceSignals(
      signals.flatMap((value) => (value._tag === "Some" ? [value.value] : [])),
      goals,
    ),
    ...definitions.flatMap((definition): ReactionCandidate[] => {
      const state = goals.get(`/goals/${definition.slug}`);
      if (!state || state.status !== "active") return [];
      return [
        {
          _tag: "Goal",
          slug: definition.slug,
          description: definition.description,
          title: goalTitleText(definition, state),
          summary: state.summary,
        },
      ];
    }),
  ];
});

/** One request per Signal; a missing or malformed answer is a failure, not a negative match. */
export const matchSignal = Effect.fn("Signal.match")(function* (
  client: SystemOneClient,
  record: ContextText,
  signal: Extract<ReactionCandidate, { _tag: "Signal" }>,
) {
  const response = yield* client.systemOne({
    state: JSON.stringify({ context: record }),
    questions: {
      matches: choice(`Does this Context satisfy the Signal condition? ${signal.when}`, {
        yes: "Evidence satisfies the condition; execute its Task.",
        no: "The condition is not satisfied.",
      }),
    },
  });
  const answer = response.answers.matches;
  if (answer?.type !== "choice" || (answer.choice !== "yes" && answer.choice !== "no"))
    return yield* new ReactionFailure({ message: "System One returned no valid Signal decision" });
  const legend = answer.legend?.[answer.choice];
  const reason =
    typeof legend === "string"
      ? legend
      : `Signal condition ${answer.choice === "yes" ? "satisfied" : "not satisfied"}: ${signal.when}`;
  return answer.choice === "yes"
    ? { _tag: "Matched" as const, reason }
    : { _tag: "NotMatched" as const, reason };
});

export class ReactionFailure extends Schema.TaggedError<ReactionFailure>()("ReactionFailure", {
  message: Schema.String,
  cause: Schema.optional(Schema.Defect()),
}) {}
export class ReactionPolicy extends Context.Service<
  ReactionPolicy,
  {
    readonly plan: (
      work: FrozenReaction,
      concurrency: number,
    ) => Effect.Effect<ReactionPlan, ReactionFailure>;
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
      plan: Effect.fn("SystemOne.plan")(function* (work, concurrency) {
        const record = work.event.record;
        const deliveryId = (target: string) =>
          createHash("sha256")
            .update(JSON.stringify(["reaction-delivery-v1", work.event.id, target]))
            .digest("hex");
        const clock = yield* Clock.Clock;
        return yield* Effect.forEach(
          work.targets.filter(({ result }) => result._tag === "Pending"),
          ({ input }) =>
            Effect.gen(function* () {
              const target = targetPath(input);
              const request = {
                requestId: deliveryId(target),
                causationId: work.event.id,
                source: "/system-one" as const,
                target,
              };
              const result = yield* Match.value(input).pipe(
                Match.tag("Signal", (signal) =>
                  matchSignal(options.client, record, signal).pipe(
                    Effect.map((match): ReactionDecision =>
                      match._tag === "NotMatched"
                        ? match
                        : {
                            ...match,
                            delivery: {
                              status: "pending",
                              attempts: 0,
                              command: {
                                _tag: "Signal",
                                input: {
                                  ...request,
                                  version: signal.version,
                                  sourceContext: record,
                                },
                              },
                            },
                          },
                    ),
                  ),
                ),
                Match.tag("Goal", (goal) =>
                  matchGoal(
                    options.client,
                    record,
                    {
                      definition: { slug: goal.slug, description: goal.description },
                      title: goal.title,
                      summary: goal.summary,
                    },
                    options.screening,
                  ).pipe(
                    Effect.map((match): ReactionDecision =>
                      match._tag === "NotMatched"
                        ? match
                        : {
                            _tag: "Matched",
                            reason: match.reason,
                            delivery: {
                              status: "pending",
                              attempts: 0,
                              command: {
                                _tag: "Goal",
                                input: {
                                  ...request,
                                  intent: makeGoalIntent(
                                    record,
                                    match.relevance,
                                    new Date(clock.currentTimeMillisUnsafe()).toISOString(),
                                  ),
                                },
                              },
                            },
                          },
                    ),
                  ),
                ),
                Match.exhaustive,
                Effect.catchTag(
                  ["DecisionError", "ReactionFailure", "GoalScreeningStoreError"],
                  (error) => Effect.succeed({ _tag: "Failed" as const, error: error.message }),
                ),
              );
              return { target, result };
            }),
          { concurrency },
        );
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
              slug: input.target.slice("/goals/".length),
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
