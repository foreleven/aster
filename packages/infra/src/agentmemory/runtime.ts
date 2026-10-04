import { MemoryStartupError } from "./errors.js";
import { spawn, type ChildProcess } from "node:child_process";
import { randomBytes } from "node:crypto";
import {
  existsSync,
  readFileSync,
  unlinkSync,
  mkdirSync,
  openSync,
  closeSync,
  writeFileSync,
} from "node:fs";
import { createRequire } from "node:module";
import { homedir } from "node:os";
import { dirname, join } from "node:path";
import { createServer } from "node:net";
import { parse, stringify } from "yaml";
import { Effect } from "effect";
import { memoryEnvironment, type MemoryConfig } from "./config.js";
import { makeMemoryClient, type MemoryClient, type MemoryConnection } from "./client.js";

export interface ManagedMemory {
  readonly connection: MemoryConnection;
  readonly client: MemoryClient;
}
const require = createRequire(import.meta.url);
const alive = (pid: number) => {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
};
const pidAt = (path: string): number | undefined => {
  try {
    const value = Number(readFileSync(path, "utf8").trim());
    return Number.isSafeInteger(value) && value > 0 ? value : undefined;
  } catch {
    return undefined;
  }
};
const delay = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));
const stopPid = async (pid: number) => {
  if (!alive(pid)) return;
  process.kill(pid, "SIGTERM");
  const deadline = Date.now() + 10_000;
  while (alive(pid) && Date.now() < deadline) await delay(50);
  if (alive(pid)) process.kill(pid, "SIGKILL");
};
const checkPort = (port: number) =>
  new Promise<void>((resolve, reject) => {
    const server = createServer();
    server.once("error", () => reject(new Error(`Memory port ${port} is already in use`)));
    server.listen(port, "127.0.0.1", () => server.close(() => resolve()));
  });

/** The published CLI has global PID files, so serialize ownership without adopting another daemon. */
export const launchMemory = async (
  config: MemoryConfig,
  cwd: string,
  signal?: AbortSignal,
  source: NodeJS.ProcessEnv = process.env,
) => {
  const upstreamDir = join(homedir(), ".agentmemory");
  mkdirSync(upstreamDir, { recursive: true });
  const lockPath = join(upstreamDir, "signals-managed.pid");
  const workerPath = join(upstreamDir, "worker.pid");
  const enginePath = join(upstreamDir, "iii.pid");
  const statePath = join(upstreamDir, "engine-state.json");
  const lockPid = pidAt(lockPath);
  if (lockPid && alive(lockPid))
    throw new Error("Another Aster process owns the managed memory runtime");
  if (existsSync(lockPath)) unlinkSync(lockPath);
  const lock = openSync(lockPath, "wx", 0o600);
  closeSync(lock);
  // Synchronous lock creation/write avoids an in-process acquisition race.
  writeFileSync(lockPath, `${process.pid}\n`);
  let child: ChildProcess | undefined;
  let enginePid: number | undefined;
  let client: MemoryClient | undefined;
  const previousEngine = pidAt(enginePath);
  const previousWorker = pidAt(workerPath);
  let startupError: Error | undefined;
  const ownsEngine = () => {
    const current = pidAt(enginePath);
    if (current && current !== previousEngine && child?.pid) {
      try {
        const state = JSON.parse(readFileSync(statePath, "utf8")) as {
          kind?: string;
          configPath?: string;
        };
        if (
          state.kind === "native" &&
          [
            join(config.dataDir, "iii-config.yaml"),
            join(config.dataDir, "signals-iii-template.yaml"),
          ].includes(state.configPath ?? "")
        )
          enginePid = current;
      } catch {
        /* engine may still be starting */
      }
    }
  };
  const stop = async () => {
    await client?.drain();
    ownsEngine();
    if (child?.pid && alive(child.pid)) await stopPid(child.pid);
    // iii 0.11.2 persists KV on a timer and has no shutdown flush. Keep it alive
    // for several configured ticks after the worker saves its search index.
    if (enginePid && alive(enginePid)) {
      await delay(500);
      await stopPid(enginePid);
    }
    client?.close();
    if (pidAt(workerPath) === child?.pid && child?.pid) unlinkSync(workerPath);
    if (pidAt(enginePath) === enginePid && enginePid) {
      unlinkSync(enginePath);
      if (existsSync(statePath)) unlinkSync(statePath);
    }
    if (pidAt(lockPath) === process.pid) unlinkSync(lockPath);
  };
  try {
    if ((previousEngine && alive(previousEngine)) || (previousWorker && alive(previousWorker))) {
      throw new Error("agentmemory is already running; stop that instance before starting Aster");
    }
    for (const port of [config.port, config.port + 1, config.port + 2, config.port + 46023])
      await checkPort(port);
    signal?.throwIfAborted();
    const environment = memoryEnvironment(config, source);
    const secret = randomBytes(32).toString("hex");
    const connection: MemoryConnection = {
      url: `http://127.0.0.1:${config.port}`,
      secret,
      dataDir: config.dataDir,
      project: cwd,
      cwd,
    };
    const packageDir = dirname(require.resolve("@agentmemory/agentmemory/package.json"));
    const cliPath = join(packageDir, "dist/cli.mjs");
    mkdirSync(config.dataDir, { recursive: true });
    const template = parse(readFileSync(join(packageDir, "iii-config.yaml"), "utf8")) as {
      workers: Array<{ name: string; config: Record<string, unknown> }>;
    };
    template.workers = template.workers.filter((worker) => worker.name !== "iii-exec");
    template.workers.push({
      name: "iii-worker-manager",
      config: { port: config.port + 46023, host: "127.0.0.1" },
    });
    for (const worker of template.workers) {
      if (worker.name === "iii-http" || worker.name === "iii-stream") {
        worker.config.port = config.port + (worker.name === "iii-stream" ? 1 : 0);
      }
      if (worker.name === "iii-state" || worker.name === "iii-stream") {
        const adapter = worker.config.adapter as { config: { file_path: string } };
        Object.assign(adapter.config, { save_interval_ms: 100 });
        adapter.config.file_path = join(
          config.dataDir,
          worker.name === "iii-state" ? "state_store.db" : "stream_store",
        );
      }
    }
    const templatePath = join(config.dataDir, "signals-iii-template.yaml");
    writeFileSync(templatePath, stringify(template));
    environment.AGENTMEMORY_III_CONFIG = templatePath;
    child = spawn(
      process.execPath,
      ["--use-env-proxy", cliPath, "--port", String(config.port), "--data-dir", config.dataDir],
      {
        cwd,
        detached: process.platform !== "win32",
        env: { ...environment, AGENTMEMORY_SECRET: secret },
        stdio: ["ignore", "pipe", "pipe"],
      },
    );
    // Keep upstream provider responses and credentials out of user-facing startup errors.
    child.stdout?.on("data", () => {});
    child.stderr?.on("data", () => {});
    child.once("error", (error) => {
      startupError = error;
    });
    const deadline = Date.now() + 120_000;
    let healthy = false;
    while (Date.now() < deadline) {
      signal?.throwIfAborted();
      ownsEngine();
      if (startupError) throw startupError;
      if (child.exitCode !== null || child.signalCode !== null)
        throw new Error(`agentmemory startup failed (exit ${child.exitCode ?? child.signalCode})`);
      try {
        const response = await fetch(`${connection.url}/agentmemory/health`, {
          headers: { Authorization: `Bearer ${secret}` },
          signal: AbortSignal.timeout(1_000),
        });
        if (response.ok) {
          const health = (await response.json()) as { status?: string };
          if (health.status === "healthy") {
            // Routes are registered before the index loads. Viewer readiness follows initialization.
            const live = await fetch(`${connection.url}/agentmemory/livez`, {
              signal: AbortSignal.timeout(1_000),
            });
            const ready = (await live.json()) as { viewerPort?: number; viewerSkipped?: boolean };
            if (ready.viewerPort || ready.viewerSkipped) {
              healthy = true;
              break;
            }
          }
        }
      } catch {
        /* engine and worker register asynchronously */
      }
      await delay(100);
    }
    if (!healthy) throw new Error("agentmemory did not become healthy within 120 seconds");
    ownsEngine();
    if (!enginePid) throw new Error("Cannot establish ownership of the agentmemory engine");
    signal?.throwIfAborted();
    client = makeMemoryClient(connection);
    return { connection, client, stop };
  } catch (error) {
    await stop();
    throw error;
  }
};

export const managedMemory = (
  config: MemoryConfig,
  cwd: string,
  environment: NodeJS.ProcessEnv = process.env,
) =>
  Effect.acquireRelease(
    Effect.tryPromise({
      try: (signal) => launchMemory(config, cwd, signal, environment),
      catch: (cause) =>
        new MemoryStartupError({
          cause,
          message: cause instanceof Error ? cause.message : String(cause),
        }),
    }),
    (runtime) => Effect.promise(runtime.stop),
    { interruptible: true },
  );
