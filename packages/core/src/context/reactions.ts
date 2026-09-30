import { GoalScreeningError } from "./errors.js";
import type { ActorRef } from "@aster/actor";
import { Clock, Effect, Stream } from "effect";
import { ContextRegistry } from "./registry.js";
import { ContextCaptureSink } from "./memory.js";
import { makeContextProcessor, isolateContextChange } from "./processing.js";
import { InternalAgent } from "../tasks/services.js";
import { SystemOneClient } from "../decisions/system-one.js";
import { GoalSettings } from "../config/settings.js";
import { makeSystemOneGate, detectSignals } from "../signals/detect.js";
import { sourceSignals } from "../signals/policy.js";
import { relevantGoals } from "../goals/runtime.js";
import type { GoalsRootCommand } from "../goals/actors.js";
import type { SignalRootCommand } from "../signals/actors.js";

/** Domain coordination is independent of any source integration or transport. */
export const startContextReactions = (roots: {
  readonly signals: ActorRef<SignalRootCommand>;
  readonly goals?: ActorRef<GoalsRootCommand>;
}) =>
  Effect.gen(function* () {
    const registry = yield* ContextRegistry;
    const capture = yield* ContextCaptureSink;
    const internal = yield* InternalAgent;
    const client = yield* SystemOneClient;
    const settings = yield* GoalSettings;
    const gate = makeSystemOneGate(client);
    const process = makeContextProcessor(
      registry,
      capture.capture,
      (record, snapshot) =>
        Effect.gen(function* () {
          const definitions = sourceSignals(snapshot, yield* Clock.currentTimeMillis);
          const matchedGoals = new Set<string>();
          yield* detectSignals(
            record.path,
            snapshot,
            definitions,
            gate,
            roots.signals,
            internal.extract,
            (slugs) => {
              for (const slug of slugs) {
                const goal = (snapshot[`/signals/${slug}`]?.state as { goal?: string } | undefined)
                  ?.goal;
                if (goal) matchedGoals.add(goal);
              }
            },
          );
          if (!roots.goals) return;
          const relevant = yield* relevantGoals(
            client,
            record,
            settings.definitions.filter(
              (goal) =>
                !matchedGoals.has(goal.slug) &&
                (registry.get(`/goals/${goal.slug}`)?.state as { status?: string })?.status !==
                  "completed",
            ),
          ).pipe(
            Effect.mapError(
              (cause) =>
                new GoalScreeningError({ path: record.path, cause, message: cause.message }),
            ),
          );
          for (const goal of relevant)
            yield* roots.goals.tell({
              _tag: "Route",
              slug: goal.slug,
              command: { _tag: "Evaluate", reason: `Context changed: ${record.path}` },
            });
        }),
      internal.describe,
    );
    const changes = yield* registry.subscribe;
    return yield* Stream.runForEach(changes, isolateContextChange(process)).pipe(Effect.forkScoped);
  });
