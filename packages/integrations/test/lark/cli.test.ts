import { Redacted } from "effect";
import assert from "node:assert/strict";
import { test } from "node:test";
import { Clock, Deferred, Effect, Fiber, Sink, Stream } from "effect";
import { TestClock } from "effect/testing";
import { ChildProcessSpawner, type ChildProcess } from "effect/process";
import { ProcessEnvironment } from "@aster/core";
import { LarkCli, decodeCliOutput } from "../../src/lark/cli.js";
import { LarkSettings } from "../../src/lark/settings.js";
const fakeSpawner = (
  options: {
    stdout?: string;
    code?: number;
    wait?: Effect.Effect<void>;
    inspect?: (command: ChildProcess.Command) => void;
    released?: () => void;
    entered?: Deferred.Deferred<void>;
  } = {},
) =>
  ChildProcessSpawner.make((command) =>
    Effect.acquireRelease(
      Effect.gen(function* () {
        options.inspect?.(command);
        if (options.entered) yield* Deferred.succeed(options.entered, undefined);
        return ChildProcessSpawner.makeHandle({
          pid: ChildProcessSpawner.ProcessId(1),
          exitCode: (options.wait ?? Effect.void).pipe(
            Effect.as(ChildProcessSpawner.ExitCode(options.code ?? 0)),
          ),
          isRunning: Effect.succeed(true),
          kill: () => Effect.void,
          stdin: Sink.drain,
          stdout: Stream.make(new TextEncoder().encode(options.stdout ?? "{}")),
          stderr: Stream.empty,
          all: Stream.empty,
          getInputFd: () => Sink.drain,
          getOutputFd: () => Stream.empty,
          unref: Effect.succeed(Effect.void),
        });
      }),
      () => Effect.sync(() => options.released?.()),
    ),
  );
const run = (spawner: ChildProcessSpawner.ChildProcessSpawner["Service"]) =>
  LarkCli.use((cli) => cli.run(["im", "+messages-search", "--query", "x; $(secret)"])).pipe(
    Effect.provide(LarkCli.layer),
    Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner),
    Effect.provideService(ProcessEnvironment, {
      values: Redacted.make({ PATH: "/bin", PRIVATE: "secret" }),
      privateKeys: ["PRIVATE"],
    }),
    Effect.provideService(LarkSettings, { description: "Account", profile: "work" }),
  );

test("CLI uses direct argv and captured environment, scopes processes and decodes envelopes", async () => {
  let released = false;
  const stdout = await Effect.runPromise(
    Effect.scoped(
      run(
        fakeSpawner({
          stdout: '{"ok":true,"data":{"value":1}}',
          released: () => {
            released = true;
          },
          inspect: (command) => {
            assert.equal(command._tag, "StandardCommand");
            if (command._tag !== "StandardCommand") return;
            assert.equal(command.command, "lark-cli");
            assert.deepEqual(command.args, [
              "--profile",
              "work",
              "im",
              "+messages-search",
              "--query",
              "x; $(secret)",
            ]);
            assert.equal(command.options.shell, false);
            assert.equal(command.options.extendEnv, false);
            assert.equal(command.options.env?.PRIVATE, undefined);
            assert.equal(command.options.env?.PATH, "/bin");
          },
        }),
      ),
    ),
  );
  assert.ok(released);
  assert.deepEqual(await Effect.runPromise(decodeCliOutput(stdout)), { value: 1 });
  for (const body of ["[]", "null", "invalid", '{"ok":false,"error":{"message":"denied"}}'])
    assert.equal(
      (await Effect.runPromise(decodeCliOutput(body).pipe(Effect.flip)))._tag,
      "LarkCliError",
    );
  assert.equal(
    (await Effect.runPromise(Effect.scoped(run(fakeSpawner({ code: 1 })).pipe(Effect.flip))))._tag,
    "LarkCliError",
  );
  assert.equal(
    (
      await Effect.runPromise(
        Effect.scoped(
          run(fakeSpawner({ stdout: "x".repeat(32 * 1024 * 1024 + 1) })).pipe(Effect.flip),
        ),
      )
    )._tag,
    "LarkCliError",
  );
});

test("timeout and caller cancellation release the native process", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const clock = yield* TestClock.make();
        for (const timeout of [true, false]) {
          let released = false;
          const entered = yield* Deferred.make<void>();
          const fiber = yield* run(
            fakeSpawner({
              wait: Effect.never,
              entered,
              released: () => {
                released = true;
              },
            }),
          ).pipe(Effect.provideService(Clock.Clock, clock), Effect.forkScoped);
          yield* Deferred.await(entered);
          if (timeout) {
            yield* clock.adjust("60 seconds");
            assert.equal((yield* Fiber.join(fiber).pipe(Effect.flip))._tag, "LarkCliError");
          } else yield* Fiber.interrupt(fiber);
          assert.ok(released);
        }
      }),
    ),
  );
});
