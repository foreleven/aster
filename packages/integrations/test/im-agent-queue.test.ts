import assert from "node:assert/strict";
import { test } from "node:test";
import { Cause, Clock, Context, Effect, Exit, Fiber } from "effect";
import { TestClock } from "effect/testing";
import { makeImAgentQueue } from "../src/lark/im/summary/agent-queue.js";
import { ChatSummaryError } from "../src/lark/shared/errors.js";

class Caller extends Context.Service<Caller, string>()("test/Caller") {}

test("shared workers survive admission/model failures and preserve the caller Context", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const clock = yield* TestClock.make();
        yield* Effect.gen(function* () {
          const error = new ChatSummaryError({ message: "expected failure" });
          const defect = new Error("unexpected defect");
          let admissions = 0;
          const queue = yield* makeImAgentQueue(
            { startIntervalMs: 10, concurrency: 1 },
            {
              load: () => Effect.succeed(undefined),
              save: () =>
                Effect.gen(function* () {
                  admissions++;
                  if (admissions === 1) return yield* error;
                  if (admissions === 2) return yield* Effect.die(defect);
                }),
            },
          );
          for (const scenario of [
            "admission-error",
            "admission-defect",
            "model-error",
            "model-defect",
            "success",
          ]) {
            const fiber = yield* queue
              .run(
                scenario,
                Effect.gen(function* () {
                  const caller = yield* Caller;
                  assert.equal(caller, scenario);
                  assert.ok(scenario.startsWith("model") || scenario === "success");
                  if (scenario === "model-error") return yield* error;
                  if (scenario === "model-defect") return yield* Effect.die(defect);
                  return caller;
                }),
              )
              .pipe(Effect.provideService(Caller, scenario), Effect.forkScoped);
            yield* clock.adjust(10);
            const result = yield* Fiber.await(fiber);
            if (scenario === "success") {
              assert.deepEqual(result, Exit.succeed(scenario));
            } else {
              assert.ok(Exit.isFailure(result));
              assert.equal(
                Cause.squash(result.cause),
                scenario.endsWith("defect") ? defect : error,
              );
            }
          }
          assert.equal(admissions, 5);
        }).pipe(Effect.provideService(Clock.Clock, clock));
      }),
    ),
  );
});

test("cancelling during shared start spacing does not postpone the next chat", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const clock = yield* TestClock.make();
        yield* Effect.gen(function* () {
          const saved: number[] = [];
          const queue = yield* makeImAgentQueue(
            { startIntervalMs: 10, concurrency: 1 },
            {
              load: () => Effect.succeed(0),
              save: (at) =>
                Effect.sync(() => {
                  saved.push(at);
                }),
            },
          );
          const cancelled = yield* queue
            .run("cancelled", Effect.die("Cancelled model ran"))
            .pipe(Effect.forkScoped);
          yield* clock.adjust(5);
          yield* Fiber.interrupt(cancelled);
          const next = yield* queue.run("next", Clock.currentTimeMillis).pipe(Effect.forkScoped);
          yield* clock.adjust(5);
          assert.equal(yield* Fiber.join(next), 10);
          assert.deepEqual(saved, [10]);
        }).pipe(Effect.provideService(Clock.Clock, clock));
      }),
    ),
  );
});
