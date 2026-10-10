import assert from "node:assert/strict";
import { test } from "node:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Deferred, Effect, Fiber, FileSystem, Layer, Path, Sink, Stream } from "effect";
import { TestClock } from "effect/testing";
import { ChildProcessSpawner } from "effect/process";
import { ConfigLocation } from "../src/config/location.js";
import { managedMemory } from "../src/memory/daemon.js";
import { http } from "./http-fixtures.js";

for (const phase of ["spawn", "health"] as const)
  test(
    `memory ${phase} interruption joins worker cleanup and engine stop before releasing its owner`,
    { timeout: 10000 },
    async (t) => {
      const root = await mkdtemp(join(tmpdir(), "aster-daemon-"));
      t.after(() => rm(root, { recursive: true, force: true }));
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            // Retire TestClock's live warning before cancellation: its initial warning
            // starts in an explicitly interruptible region even during finalization.
            yield* TestClock.adjust(0);
            const entered = yield* Deferred.make<void>();
            const spawned = yield* Deferred.make<void>();
            const stopping = yield* Deferred.make<void>();
            const finish = yield* Deferred.make<void>();
            const exited = yield* Deferred.make<void>();
            const events: string[] = [];
            const files = new Map<string, string>();
            const dataDir = join(root, "memory");
            const upstream = join(root, ".agentmemory");
            const fs = FileSystem.layerNoop({
              makeDirectory: () => Effect.void,
              readFileString: (path) =>
                Effect.sync(() => {
                  return files.get(path) ?? (path.endsWith("iii-config.yaml") ? "workers: []" : "");
                }),
              writeFileString: (path, text) =>
                Effect.sync(() => {
                  files.set(path, text);
                }),
              remove: (path) =>
                Effect.sync(() => {
                  files.delete(path);
                }),
            });
            const spawner = ChildProcessSpawner.make(() =>
              Effect.acquireRelease(
                Effect.gen(function* () {
                  files.set(join(upstream, "iii.pid"), "12345");
                  files.set(
                    join(upstream, "engine-state.json"),
                    JSON.stringify({
                      kind: "native",
                      configPath: join(dataDir, "signals-iii-template.yaml"),
                    }),
                  );
                  const handle = ChildProcessSpawner.makeHandle({
                    pid: ChildProcessSpawner.ProcessId(123),
                    exitCode: Deferred.await(exited).pipe(
                      Effect.as(ChildProcessSpawner.ExitCode(0)),
                    ),
                    isRunning: Effect.succeed(true),
                    kill: () =>
                      Deferred.succeed(stopping, undefined).pipe(
                        Effect.andThen(Deferred.await(finish)),
                        Effect.andThen(
                          Effect.sync(() => {
                            events.push("worker-stopped");
                          }),
                        ),
                        Effect.andThen(Deferred.succeed(exited, undefined)),
                      ),
                    stdin: Sink.drain,
                    stdout: Stream.empty,
                    stderr: Stream.empty,
                    all: Stream.empty,
                    getInputFd: () => Sink.drain,
                    getOutputFd: () => Stream.empty,
                    unref: Effect.succeed(Effect.void),
                  });
                  if (phase === "spawn") {
                    yield* Deferred.succeed(entered, undefined);
                    yield* Deferred.await(spawned);
                  }
                  return handle;
                }),
                () => Effect.void,
              ),
            );
            const layers = Layer.mergeAll(
              fs,
              Path.layer,
              Layer.succeed(ChildProcessSpawner.ChildProcessSpawner, spawner),
              Layer.succeed(ConfigLocation, {
                baseDir: root,
                projectRoot: root,
                homeDirectory: root,
                envPath: join(root, ".env"),
              }),
              http((_input, init) => {
                Deferred.doneUnsafe(entered, Effect.void);
                return new Promise<Response>((_, reject) =>
                  init?.signal?.addEventListener("abort", () => reject(init.signal?.reason)),
                );
              }),
            );
            const acquisition = Effect.scoped(
              Effect.gen(function* () {
                yield* Effect.addFinalizer(() =>
                  Effect.sync(() => {
                    events.push("owner-released");
                  }),
                );
                yield* managedMemory(
                  { description: "Memory", port: 3111, dataDir, autoCompress: false },
                  {},
                  {
                    alive: () => Effect.succeed(false),
                    stopPid: () =>
                      Effect.sync(() => {
                        events.push("engine-stopped");
                      }),
                  },
                );
              }),
            ).pipe(Effect.provide(layers));
            const fiber = yield* acquisition.pipe(Effect.forkScoped);
            yield* Deferred.await(entered);
            const interrupted = yield* Fiber.interrupt(fiber).pipe(Effect.forkScoped);
            yield* Effect.yieldNow;
            yield* Deferred.succeed(spawned, undefined);
            yield* Deferred.await(stopping);
            assert.deepEqual(events, []);
            yield* Deferred.succeed(finish, undefined);
            yield* Deferred.await(exited);
            yield* TestClock.adjust(1000);
            yield* Fiber.join(interrupted);
            const outcome = yield* Fiber.await(fiber);
            assert.equal(outcome._tag, "Failure");
            if (outcome._tag === "Failure")
              assert.equal(
                outcome.cause.reasons.every((reason) => reason._tag === "Interrupt"),
                true,
                JSON.stringify(outcome),
              );
            assert.deepEqual(events, ["worker-stopped", "engine-stopped", "owner-released"]);
          }).pipe(Effect.provide(TestClock.layer())),
        ),
      );
    },
  );
