import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorTestKit } from "@aster/actor";
import { Clock, Deferred, Effect, Fiber } from "effect";
import { TestClock } from "effect/testing";
import {
  SignalActor,
  makeContextRegistry,
  makeGoalRuntime,
  type GoalCommand,
} from "../src/index.js";

test("Goal coordination retains the caller's Clock and cancellation instead of starting a detached runtime", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        const path = "/signals/project--watch";
        yield* registry.register(path, SignalActor.context);
        yield* registry.commit(
          {
            path,
            description: "watch",
            messages: [],
            state: {
              slug: "project--watch",
              goal: "project",
              when: "changed",
              task: "read",
              agent: "test",
              mode: "confirm",
            },
          },
          { expectedRevision: registry.get(path)?.revision ?? 0 },
        );
        const subscriber = yield* ActorTestKit.probe<GoalCommand>();
        const entered = yield* Deferred.make<void>();
        let cancelled = false;
        let observedTime: number | undefined;
        const runtime = makeGoalRuntime(
          { definitions: [] },
          registry,
          {
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
          },
          {
            plan: () => Effect.die(new Error("No reasoning expected")),
          },
        );
        const operation = runtime.reconcile("project", subscriber.ref);
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
