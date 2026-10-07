import assert from "node:assert/strict";
import { test } from "node:test";
import { Deferred, Effect, Fiber } from "effect";
import { managedMemory } from "../src/agentmemory/runtime.js";
import { parseMemoryConfig } from "../src/agentmemory/config.js";

const config = parseMemoryConfig(undefined, "/unused");

test("interrupted memory startup joins launch and cleanup before releasing its outer owner", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const starting = yield* Deferred.make<void>();
        const aborted = yield* Deferred.make<void>();
        const stopping = yield* Deferred.make<void>();
        const finishStart = yield* Deferred.make<void>();
        const finishStop = yield* Deferred.make<void>();
        const run = Effect.runPromiseWith(yield* Effect.context<never>());
        const events: string[] = [];
        const acquire = Effect.scoped(
          Effect.gen(function* () {
            yield* Effect.addFinalizer(() =>
              Effect.sync(() => {
                events.push("owner-released");
              }),
            );
            yield* managedMemory(config, "/unused", {}, () => ({
              start: async (signal) => {
                signal.addEventListener("abort", () => Deferred.doneUnsafe(aborted, Effect.void), {
                  once: true,
                });
                Deferred.doneUnsafe(starting, Effect.void);
                await run(Deferred.await(finishStart));
                events.push("launch-joined");
                signal.throwIfAborted();
                throw new Error("Expected cancellation");
              },
              stop: async () => {
                Deferred.doneUnsafe(stopping, Effect.void);
                await run(Deferred.await(finishStop));
                events.push("stopped");
              },
            }));
          }),
        );
        const fiber = yield* acquire.pipe(Effect.forkScoped);
        yield* Deferred.await(starting);
        const interruption = yield* Fiber.interrupt(fiber).pipe(Effect.forkScoped);
        yield* Deferred.await(aborted);
        assert.deepEqual(events, []);
        yield* Deferred.succeed(finishStart, undefined);
        yield* Deferred.await(stopping);
        assert.deepEqual(events, ["launch-joined"]);
        assert.equal(interruption.pollUnsafe(), undefined);
        yield* Deferred.succeed(finishStop, undefined);
        yield* Fiber.join(interruption);
        assert.deepEqual(events, ["launch-joined", "stopped", "owner-released"]);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("memory startup failure retains typed error and runs cleanup exactly once", async () => {
  let stops = 0;
  const failure = new Error("health failed");
  const result = await Effect.runPromise(
    Effect.scoped(
      managedMemory(config, "/unused", {}, () => ({
        start: async () => {
          throw failure;
        },
        stop: async () => {
          stops++;
        },
      })),
    ).pipe(Effect.flip),
  );
  assert.equal(result._tag, "MemoryStartupError");
  assert.equal(result.cause, failure);
  assert.equal(stops, 1);
});

test("memory cleanup defects remain observable on interrupted startup", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const starting = yield* Deferred.make<void>();
        const cleanupError = new Error("stop failed");
        const fiber = yield* Effect.scoped(
          managedMemory(config, "/unused", {}, () => ({
            start: (signal) =>
              new Promise<never>((_resolve, reject) => {
                signal.addEventListener("abort", () => reject(signal.reason), { once: true });
                Deferred.doneUnsafe(starting, Effect.void);
              }),
            stop: async () => {
              throw cleanupError;
            },
          })),
        ).pipe(Effect.forkScoped);
        yield* Deferred.await(starting);
        yield* Fiber.interrupt(fiber);
        const exit = fiber.pollUnsafe();
        assert.equal(exit?._tag, "Failure");
        if (exit?._tag === "Failure")
          assert.ok(
            exit.cause.reasons.some(
              (reason) => reason._tag === "Die" && reason.defect === cleanupError,
            ),
          );
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("ready memory is stopped once when its Scope closes", async () => {
  let stops = 0;
  const connection = {
    url: "http://unused",
    secret: "test",
    dataDir: "/unused",
    project: "/unused",
    cwd: "/unused",
  };
  const client = {
    capture: async () => {},
    search: async () => {
      throw new Error("unused");
    },
    expand: async () => {
      throw new Error("unused");
    },
    drain: async () => {},
    close: () => {},
  };
  const ready = await Effect.runPromise(
    Effect.scoped(
      managedMemory(config, "/unused", {}, () => ({
        start: async () => ({ connection, client }),
        stop: async () => {
          stops++;
        },
      })),
    ),
  );
  assert.equal(ready.client, client);
  assert.equal(stops, 1);
});
