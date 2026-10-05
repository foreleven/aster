import assert from "node:assert/strict";
import { test } from "node:test";
import { Clock, Deferred, Effect, Fiber, Layer } from "effect";
import { TestClock } from "effect/testing";
import { GoalSignals, SignalCommands } from "../src/index.js";

test("Goal coordination retains the caller's Clock and cancellation instead of starting a detached runtime", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        let cancelled = false;
        let observedTime: number | undefined;
        const runtime = yield* GoalSignals.pipe(
          Effect.provide(
            GoalSignals.layer.pipe(
              Layer.provide(
                Layer.succeed(SignalCommands, {
                  bind: () => Effect.succeed(true),
                  ask: () =>
                    Effect.gen(function* () {
                      observedTime = yield* Clock.currentTimeMillis;
                      yield* Deferred.succeed(entered, undefined);
                      return yield* Effect.never;
                    }).pipe(
                      Effect.ensuring(
                        Effect.sync(() => {
                          cancelled = true;
                        }),
                      ),
                    ),
                }),
              ),
            ),
          ),
        );
        const operation = runtime.deactivate("project");
        assert.equal(observedTime, undefined, "constructing an Effect must not send a command");
        const clock = yield* TestClock.make();
        yield* clock.adjust(12345);
        const fiber = yield* operation.pipe(
          Effect.provideService(Clock.Clock, clock),
          Effect.forkScoped,
        );
        yield* Deferred.await(entered);
        assert.equal(observedTime, 12345);
        yield* Fiber.interrupt(fiber);
        assert.equal(cancelled, true);
      }),
    ),
  );
});
