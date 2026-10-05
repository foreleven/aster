import assert from "node:assert/strict";
import { test } from "node:test";
import { Deferred, Effect, Fiber } from "effect";
import {
  GoalActor,
  makeApplicationApi,
  makeMemoryGoalHistory,
  type ContextRecord,
} from "../src/index.js";
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
                definition: { slug: "demo", description: "Goal" },
                status: "active",
                summary: "Original",
                inputs: [],
                receipts: [],
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
            () => path,
          );
          const pending = yield* working
            .save({ summary: "Stale computed answer" })
            .pipe(Effect.result, Effect.forkScoped);
          yield* Deferred.await(entered);
          const newer = yield* registry.commit(
            {
              ...initial,
              state: { ...initial.state, status: "completed", summary: "Newer progress" },
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
            definition: { slug: "demo", description: "Goal" },
            status: "active",
            summary: "",
            inputs: [],
            receipts: [],
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
      const working = goalWorkingState(registry, history, () => path);
      yield* working.save();
      const record = registry.get(path)!;
      assert.equal(record.messages.length, 100);
      assert.deepEqual(record.messages[0], { role: "user", content: "Input 6", timestamp: 6 });
      assert.deepEqual(record.messages.at(-1), {
        role: "user",
        content: "Input 105",
        timestamp: 105,
      });
      assert.equal(yield* history.count("demo"), 105);
      assert.equal("historyCount" in record.state, false);
      assert.equal("historyThrough" in record.state, false);
    }),
  );
});

test("Goal public error reflects the latest settled input without storing an error cache", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const registry = yield* makeContextRegistry();
      const path = "/goals/errors";
      yield* registry.register(path, GoalActor.context);
      const api = makeApplicationApi({ registry, inspect: Effect.succeed(null) });
      const definition = { slug: "errors", description: "Observe errors" };
      const first = {
        inputId: "first",
        goalSlug: "errors",
        ordinal: 1,
        receivedAt: "2026-10-05T00:00:00Z",
        payload: { _tag: "UserInput", text: "First" },
        status: "failed",
        error: "Model failed",
      };
      for (const status of ["failed", "running", "completed"]) {
        const current = registry.get(path);
        yield* registry.commit(
          {
            path,
            description: definition.description,
            messages: [],
            state: {
              definition,
              status: "active",
              summary: "",
              receipts: [],
              inputs:
                status === "failed"
                  ? [first]
                  : [
                      first,
                      {
                        ...first,
                        inputId: "second",
                        ordinal: 2,
                        status,
                        error: "Old uncertain outcome",
                      },
                    ],
            },
          },
          { expectedRevision: current?.revision ?? 0 },
        );
        const view = yield* api.context(path);
        const projected = view.state as { lastError?: string };
        assert.equal(projected.lastError, status === "completed" ? undefined : "Model failed");
        assert.equal("lastError" in registry.get(path)!.state, false);
        assert.equal("historyCount" in view.state, false);
        assert.equal("receipts" in view.state, false);
      }
    }),
  );
});
