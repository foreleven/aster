import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { withStorageLease } from "@aster/infra";
import { Deferred, Effect, Fiber } from "effect";

test("startup rejects a held actor-store lock without changing its owner marker", async () => {
  const home = await mkdtemp(join(tmpdir(), "aster-startup-"));
  const lock = join(home, ".aster", "actors.pid");
  let release: (() => Promise<unknown>) | undefined;
  try {
    const ready = Deferred.makeUnsafe<void>();
    const lease = Effect.runFork(
      withStorageLease(
        join(home, ".aster"),
        Deferred.succeed(ready, undefined).pipe(Effect.andThen(Effect.never)),
      ),
    );
    await Effect.runPromise(Deferred.await(ready));
    release = () => Effect.runPromise(Fiber.interrupt(lease));
    const config = join(home, "config.yaml");
    await writeFile(config, "{}");
    const result = await new Promise<{ code: number | string | null | undefined; stderr: string }>(
      (resolve) => {
        execFile(
          process.execPath,
          [
            "--conditions=aster-source",
            "--import=tsx",
            fileURLToPath(new URL("../src/cli.js", import.meta.url)),
            "start",
            "--config",
            config,
          ],
          { env: { ...process.env, HOME: home }, timeout: 10_000 },
          (error, _stdout, stderr) => resolve({ code: error?.code, stderr }),
        );
      },
    );
    assert.equal(result.code, 1, result.stderr);
    assert.ok(result.stderr.includes("Cannot acquire actor store"), result.stderr);
    assert.equal(await readFile(lock, "utf8"), String(process.pid));
  } finally {
    await release?.();
    await rm(home, { recursive: true, force: true });
  }
});

const cli = (args: string[], home: string) =>
  new Promise<{ code: number | string | null | undefined; stdout: string; stderr: string }>(
    (resolve) => {
      execFile(
        process.execPath,
        [
          "--conditions=aster-source",
          "--import=tsx",
          fileURLToPath(new URL("../src/cli.js", import.meta.url)),
          ...args,
        ],
        {
          env: { PATH: process.env.PATH, HOME: home },
          timeout: 10_000,
        },
        (error, stdout, stderr) => resolve({ code: error?.code, stdout, stderr }),
      );
    },
  );

test("startup locks the configured root before acquiring model and integration services", async () => {
  const home = await mkdtemp(join(tmpdir(), "aster-configured-startup-"));
  const root = join(home, "custom");
  const lock = join(root, "actors.pid");
  const config = join(home, "config.yaml");
  let release: (() => Promise<unknown>) | undefined;
  try {
    const ready = Deferred.makeUnsafe<void>();
    const lease = Effect.runFork(
      withStorageLease(root, Deferred.succeed(ready, undefined).pipe(Effect.andThen(Effect.never))),
    );
    await Effect.runPromise(Deferred.await(ready));
    release = () => Effect.runPromise(Fiber.interrupt(lease));
    await writeFile(
      config,
      JSON.stringify({ config: { durable: { root }, personal: { model: "missing" } } }),
    );
    const result = await cli(["start", "--config", config], home);
    assert.equal(result.code, 1, result.stderr);
    assert.match(result.stderr, /Cannot acquire actor store/);
    assert.equal(await readFile(lock, "utf8"), String(process.pid));
  } finally {
    await release?.();
    await rm(home, { recursive: true, force: true });
  }
});
