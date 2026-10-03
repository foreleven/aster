import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";

test("startup reports the live actor-store owner without changing its lock", async () => {
  const home = await mkdtemp(join(tmpdir(), "aster-startup-"));
  const lock = join(home, ".aster", "actors.pid");
  try {
    await mkdir(join(home, ".aster"));
    await writeFile(lock, String(process.pid));
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
    assert.ok(
      result.stderr.includes(`Another Aster process (${process.pid}) owns the actor store`),
      result.stderr,
    );
    assert.equal(await readFile(lock, "utf8"), String(process.pid));
  } finally {
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
  try {
    await mkdir(root);
    await writeFile(lock, String(process.pid));
    await writeFile(
      config,
      JSON.stringify({ config: { durable: { root }, personal: { model: "missing" } } }),
    );
    const result = await cli(["start", "--config", config], home);
    assert.equal(result.code, 1, result.stderr);
    assert.match(
      result.stderr,
      new RegExp(`Another Aster process \\(${process.pid}\\) owns the actor store`),
    );
    assert.equal(await readFile(lock, "utf8"), String(process.pid));
  } finally {
    await rm(home, { recursive: true, force: true });
  }
});

test("storage migrate CLI runs offline with no configured model and supports route rollback", async () => {
  const home = await mkdtemp(join(tmpdir(), "aster-migrate-cli-"));
  const root = join(home, "data");
  const config = join(home, "config.yaml");
  try {
    const durable = { root, pi: {}, routes: [{ prefix: "/personal", backend: "pi" }] };
    // Invalid execution model is intentional: migration must never resolve or start it.
    await writeFile(
      config,
      JSON.stringify({ config: { durable }, agents: { pi: { model: "missing" } } }),
    );
    const result = await cli(["storage", "migrate", "--config", config], home);
    assert.equal(result.code, undefined, result.stderr);
    assert.deepEqual(JSON.parse(result.stdout), { checked: 0, copied: 0, routes: durable.routes });
    await writeFile(config, JSON.stringify({ config: { durable: { ...durable, routes: [] } } }));
    const reverse = await cli(["storage", "migrate", "--config", config], home);
    assert.equal(reverse.code, undefined, reverse.stderr);
    assert.deepEqual(JSON.parse(reverse.stdout), { checked: 0, copied: 0, routes: [] });
  } finally {
    await rm(home, { recursive: true, force: true });
  }
});
