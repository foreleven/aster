import assert from "node:assert/strict";
import { test } from "node:test";
import { Context, Deferred, Effect, Fiber, Layer, Scope } from "effect";
import { MemoryBackend, MemoryRecallError } from "../src/memory/contracts.js";
import { MemoryRecall } from "../src/memory/recall.js";

const run = <A, E>(work: Effect.Effect<A, E, Scope.Scope>) =>
  Effect.runPromise(Effect.scoped(work).pipe(Effect.timeout("8 seconds")));
test("Scoped Memory recall bounds capacity and caller interruption releases backend work", async () => {
  await run(
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const released = yield* Deferred.make<void>();
      let count = 0;
      const recall = yield* Layer.build(MemoryRecall.layer).pipe(
        Effect.map(Context.get(MemoryRecall)),
        Effect.provideService(MemoryBackend, {
          description: "Test",
          retrieval: "bm25",
          capture: () => Effect.void,
          save: () => Effect.succeed({ memoryId: "test-memory" }),
          drain: Effect.void,
          recall: {
            search: () =>
              Effect.gen(function* () {
                count++;
                if (count === 4) yield* Deferred.succeed(entered, undefined);
                return yield* Effect.never;
              }).pipe(Effect.ensuring(Deferred.succeed(released, undefined))),
            expand: () => Effect.fail(new MemoryRecallError({ message: "Backend unavailable" })),
          },
        }),
      );
      assert.equal((yield* Effect.flip(recall.expand([]))).message, "Backend unavailable");
      const workers = yield* Effect.forEach([1, 2, 3, 4], () =>
        recall.search("blocked").pipe(Effect.forkScoped),
      );
      yield* Deferred.await(entered);
      assert.match((yield* Effect.flip(recall.search("overflow"))).message, /capacity/);
      assert.equal(count, 4);
      yield* Fiber.interrupt(workers[0]!);
      yield* Deferred.await(released);
      assert.equal((yield* Effect.flip(recall.expand([]))).message, "Backend unavailable");
      yield* Effect.forEach(workers.slice(1), Fiber.interrupt);
    }),
  );
});
