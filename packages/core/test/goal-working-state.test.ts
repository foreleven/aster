import assert from "node:assert/strict";
import { test } from "node:test";
import { Deferred, Effect, Fiber } from "effect";
import {
  GoalActor,
  makeContextRegistry,
  makeMemoryGoalHistory,
  type ContextRecord,
} from "../src/index.js";
import { goalWorkingState } from "../src/goals/working-state.js";

for (const phase of ["count", "read"] as const)
  test(`Goal projection rejects a snapshot changed while history ${phase} is pending`, async () => {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const writes: ContextRecord[] = [];
          const registry = yield* makeContextRegistry({
            loadAll: () => [],
            save: (record) => {
              writes.push(record);
            },
          });
          const path = "/goals/demo";
          yield* registry.register(path, GoalActor.context);
          const initial = yield* registry.commit(
            {
              path,
              description: "Goal",
              state: {
                slug: "demo",
                description: "Goal",
                status: "active",
                summary: "Original",
                progress: "Original",
                tasks: [],
                historyThrough: 0,
                agentThrough: 0,
                historyCount: 0,
                pendingEvaluation: false,
                receivedEvents: [],
                receivedIntents: [],
              },
              messages: [],
            },
            { expectedRevision: 0 },
          );
          const entered = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          const gate = Deferred.succeed(entered, undefined).pipe(
            Effect.andThen(Deferred.await(release)),
          );
          const history = makeMemoryGoalHistory();
          const working = goalWorkingState(
            registry,
            {
              ...history,
              count: (goal) =>
                phase === "count"
                  ? gate.pipe(Effect.andThen(history.count(goal)))
                  : history.count(goal),
              read: (goal, options) =>
                phase === "read"
                  ? gate.pipe(Effect.andThen(history.read(goal, options)))
                  : history.read(goal, options),
            },
            () => ({ slug: "demo", description: "Goal" }),
            () => path,
            10000,
          );
          const pending = yield* working
            .save({ summary: "Stale computed answer" })
            .pipe(Effect.result, Effect.forkScoped);
          yield* Deferred.await(entered);
          const newer = yield* registry.commit(
            {
              ...initial,
              state: { ...initial.state, status: "completed", progress: "Newer progress" },
            },
            { expectedRevision: 1 },
          );
          yield* Deferred.succeed(release, undefined);
          const result = yield* Fiber.join(pending);
          assert.equal(result._tag, "Failure");
          if (result._tag === "Failure") {
            assert.equal(result.failure._tag, "ContextConflict");
            if (result.failure._tag === "ContextConflict") {
              assert.equal(result.failure.expectedRevision, 1);
              assert.equal(result.failure.actualRevision, 2);
            }
          }
          assert.deepEqual(registry.get(path), newer);
          assert.equal(writes.length, 2);
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
  });
