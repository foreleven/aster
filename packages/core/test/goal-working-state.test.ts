import assert from "node:assert/strict";
import { test } from "node:test";
import { Deferred, Effect, Fiber } from "effect";
import { GoalActor, makeMemoryGoalHistory, type ContextRecord } from "../src/index.js";
import { makeContextRegistry } from "../src/testing/context.js";
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
                inputs: [],
                historyThrough: 0,
                historyCount: 0,
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

test("Goal public messages show the latest business inputs without a compaction cursor", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const registry = yield* makeContextRegistry();
      const path = "/goals/demo";
      yield* registry.register(path, GoalActor.context);
      yield* registry.commit(
        {
          path,
          description: "Goal",
          messages: [],
          state: {
            slug: "demo",
            description: "Goal",
            status: "active",
            summary: "",
            progress: "",
            inputs: [],
            historyCount: 0,
          },
        },
        { expectedRevision: 0 },
      );
      const history = makeMemoryGoalHistory();
      for (let index = 1; index <= 105; index++) {
        yield* history.append(
          "demo",
          { role: "user", content: `Input ${index}`, timestamp: index },
          `input-${index}`,
        );
      }
      const working = goalWorkingState(
        registry,
        history,
        () => ({ slug: "demo", description: "Goal" }),
        () => path,
      );
      yield* working.save();
      const record = registry.get(path)!;
      assert.equal(record.messages.length, 100);
      assert.deepEqual(record.messages[0], { role: "user", content: "Input 6", timestamp: 6 });
      assert.deepEqual(record.messages.at(-1), {
        role: "user",
        content: "Input 105",
        timestamp: 105,
      });
      assert.equal(working.state().historyCount, 105);
      assert.equal("historyThrough" in record.state, false);
    }),
  );
});
