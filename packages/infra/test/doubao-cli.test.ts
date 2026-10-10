import assert from "node:assert/strict";
import { test } from "node:test";
import { Deferred, Effect, Fiber, Sink, Stream } from "effect";
import { ChildProcessSpawner } from "effect/process";
import { makeDoubaoCli } from "../src/executors/doubao/cli.js";
test("queued CLI cancellation never spawns another command or interrupts unrelated work", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        let spawned = 0;
        const spawner = ChildProcessSpawner.make(() =>
          Effect.acquireRelease(
            Effect.gen(function* () {
              spawned++;
              yield* Deferred.succeed(entered, undefined);
              return ChildProcessSpawner.makeHandle({
                pid: ChildProcessSpawner.ProcessId(1),
                exitCode: Deferred.await(release).pipe(Effect.as(ChildProcessSpawner.ExitCode(0))),
                isRunning: Effect.succeed(true),
                kill: () => Effect.void,
                stdin: Sink.drain,
                stdout: Stream.make(new TextEncoder().encode('{"status":"running"}')),
                stderr: Stream.empty,
                all: Stream.empty,
                getInputFd: () => Sink.drain,
                getOutputFd: () => Stream.empty,
                unref: Effect.succeed(Effect.void),
              });
            }),
            () => Effect.void,
          ),
        );
        const run = yield* makeDoubaoCli({}).pipe(
          Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
        );
        const first = yield* run(["sessions", "status"]).pipe(Effect.forkScoped);
        yield* Deferred.await(entered);
        const queued = yield* run(["sessions", "status"]).pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        yield* Fiber.interrupt(queued);
        assert.equal(spawned, 1);
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(first);
      }),
    ),
  );
});

test("retired CLI capability rejects commands without spawning", async () => {
  const run = await Effect.runPromise(
    Effect.scoped(
      makeDoubaoCli({}).pipe(
        Effect.provideService(
          ChildProcessSpawner.ChildProcessSpawner,
          ChildProcessSpawner.make(() => Effect.die("Retired CLI spawned a process")),
        ),
      ),
    ),
  );
  const result = await Effect.runPromise(run(["sessions", "create"]).pipe(Effect.result));
  assert.equal(result._tag, "Failure");
  if (result._tag === "Failure") assert.match(result.failure.message, /closed/);
});
