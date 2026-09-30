import assert from "node:assert/strict";
import { test } from "node:test";
import { Effect, Fiber } from "effect";
import { waitForShutdown } from "../src/shutdown.js";

test("repeated signals remain handled until application resource cleanup completes", async () => {
  const interruptListeners = process.listenerCount("SIGINT");
  const terminateListeners = process.listenerCount("SIGTERM");
  let cleaned = false;
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const shutdown = yield* waitForShutdown.pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        yield* Effect.acquireRelease(Effect.void, () =>
          Effect.sync(() => {
            assert.equal(process.listenerCount("SIGINT"), interruptListeners + 1);
            process.emit("SIGINT");
            process.emit("SIGTERM");
            cleaned = true;
          }),
        );
        process.emit("SIGINT");
        yield* Fiber.join(shutdown);
        assert.equal(process.listenerCount("SIGINT"), interruptListeners + 1);
      }),
    ),
  );
  assert.equal(cleaned, true);
  assert.equal(process.listenerCount("SIGINT"), interruptListeners);
  assert.equal(process.listenerCount("SIGTERM"), terminateListeners);
});
