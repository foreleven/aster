import { CurrentActors } from "../services/actors.js";
import { createHash } from "node:crypto";
import type { ContextReader } from "../context/registry.js";
import type { ActorRef } from "@aster/actor";
import { type PublicContext } from "../context/contracts.js";
import { Clock, Context, Effect, Match, Schema } from "effect";
import { choice, type SystemOneClient } from "../services/system-one.js";
import { SignalSnapshot, signalEnabled } from "../signals/state/snapshot.js";
import type { GoalDefinition } from "../config/schema.js";
import {
  matchGoal,
  goalTitleText,
  goalSummaryText,
  type GoalScreeningStore,
} from "../goals/screening/decision.js";

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
              _tag: "Signal" as const,
              slug: record.path.slice("/signals/".length),
              when: state.trigger.when,
              version: state.version,
            },
          ]
        : [];
    });

/** Freeze only candidate rules and Goal context, never the entire Context tree. */
export const reactionTargets = (
  reader: ContextReader,
  goals: readonly GoalDefinition[],
): readonly ReactionCandidate[] => {
  const records = Object.fromEntries(
    reader
      .directory()
      .filter(({ path }) => /^\/(signals|goals)\/[^/]+$/.test(path))
      .flatMap(({ path }) => {
        const record = reader.get(path);
        return record ? [[path, record]] : [];
      }),
  );
  return [
    ...sourceSignals(records),
    ...goals.flatMap((definition): ReactionCandidate[] => {
      const record = records[`/goals/${definition.slug}`];
      if (
        !record ||
        record.projection?.visibility === "restricted" ||
        !Schema.is(Schema.Struct({ status: Schema.Literal("active") }))(record.state)
      )
        return [];
      return [
        {
          _tag: "Goal",
          slug: definition.slug,
          description: definition.description,
          title: goalTitleText(definition, record),
          summary: goalSummaryText(record),
        },
      ];
    }),
  ];
};

/** One request per Signal; a missing or malformed answer is a failure, not a negative match. */
export const matchSignal = Effect.fn("Signal.match")(function* (
  client: SystemOneClient,
  record: PublicContext,
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
