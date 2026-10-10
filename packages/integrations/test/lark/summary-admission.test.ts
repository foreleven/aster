import assert from "node:assert/strict";
import { test } from "node:test";
import { Cause, Clock, Context, Deferred, Effect, Exit, Fiber, Queue, Scope } from "effect";
import { TestClock } from "effect/testing";
import { makeSummaryAdmission } from "../../src/lark/im/summary/admission.js";
import { ChatSummaryError } from "../../src/lark/im/summary/errors.js";
class Caller extends Context.Service<Caller, string>()("test/Caller") {}

test("admission preserves FIFO, exact spacing, cancellation, scope ownership and fresh restart timing", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const clock = yield* TestClock.make();
        yield* Effect.gen(function* () {
          const owner = yield* Scope.make();
          const queue = yield* makeSummaryAdmission({ startIntervalMs: 10, concurrency: 2 }).pipe(
            Effect.provideService(Scope.Scope, owner),
          );
          const started = yield* Queue.unbounded<readonly [string, number]>();
          const release = yield* Deferred.make<void>();
          const work = (id: string) =>
            Effect.gen(function* () {
              yield* Queue.offer(started, [id, yield* Clock.currentTimeMillis]);
              yield* Deferred.await(release);
            });
          const a = yield* queue.run("a", work("a")).pipe(Effect.forkScoped);
          assert.deepEqual(yield* Queue.take(started), ["a", 0]);
          const b = yield* queue.run("b", work("b")).pipe(Effect.forkScoped);
          const cancelled = yield* queue
            .run("cancelled", work("cancelled"))
            .pipe(Effect.forkScoped);
          const c = yield* queue.run("c", work("c")).pipe(Effect.forkScoped);
          yield* clock.adjust(10);
          assert.deepEqual(yield* Queue.take(started), ["b", 10]);
          yield* Fiber.interrupt(cancelled);
          yield* Fiber.interrupt(a);
          yield* clock.adjust(10);
          assert.deepEqual(yield* Queue.take(started), ["c", 20]);
          yield* Scope.close(owner, Exit.void);
          assert.ok(Exit.isFailure(yield* Fiber.await(b)));
          assert.ok(Exit.isFailure(yield* Fiber.await(c)));
          const restarted = yield* makeSummaryAdmission({ startIntervalMs: 10, concurrency: 1 });
          assert.equal(yield* restarted.run("next", Clock.currentTimeMillis), 20);
        }).pipe(Effect.provideService(Clock.Clock, clock));
      }),
    ),
  );
});

test("model errors and defects release shared capacity and preserve the caller Context", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const clock = yield* TestClock.make();
        yield* Effect.gen(function* () {
          const queue = yield* makeSummaryAdmission({ startIntervalMs: 10, concurrency: 1 });
          const error = new ChatSummaryError({ message: "offline" });
          const defect = new Error("defect");
          for (const name of ["error", "defect", "success"]) {
            const fiber = yield* queue
              .run(
                name,
                Effect.gen(function* () {
                  assert.equal(yield* Caller, name);
                  if (name === "error") return yield* error;
                  if (name === "defect") return yield* Effect.die(defect);
                  return name;
                }),
              )
              .pipe(Effect.provideService(Caller, name), Effect.forkScoped);
            yield* clock.adjust(10);
            const exit = yield* Fiber.await(fiber);
            if (name === "success") assert.deepEqual(exit, Exit.succeed(name));
            else {
              assert.ok(Exit.isFailure(exit));
              assert.equal(Cause.squash(exit.cause), name === "error" ? error : defect);
            }
          }
        }).pipe(Effect.provideService(Clock.Clock, clock));
      }),
    ),
  );
});

test("cancelling an admission wait does not consume a later start slot", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const clock = yield* TestClock.make();
        yield* Effect.gen(function* () {
          const queue = yield* makeSummaryAdmission({ startIntervalMs: 10, concurrency: 1 });
          assert.equal(yield* queue.run("first", Clock.currentTimeMillis), 0);
          const cancelled = yield* queue
            .run("cancelled", Effect.die("unexpected execution"))
            .pipe(Effect.forkScoped);
          yield* clock.adjust(5);
          yield* Fiber.interrupt(cancelled);
          const next = yield* queue.run("next", Clock.currentTimeMillis).pipe(Effect.forkScoped);
          yield* clock.adjust(5);
          assert.equal(yield* Fiber.join(next), 10);
        }).pipe(Effect.provideService(Clock.Clock, clock));
      }),
    ),
  );
});
