import { createRequire } from "node:module";
import { randomBytes } from "node:crypto";
import { parse, stringify } from "yaml";
import { Effect, FileSystem, Path, Predicate, Redacted, Schedule, Schema, Stream } from "effect";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { HttpClient, HttpClientRequest } from "effect/http";
import { ConfigLocation } from "../config/location.js";
import { scopedStorageLease } from "../storage/lease.js";
import { MemoryStartupError } from "./errors.js";
import { Row, type MemoryConnection } from "./protocol.js";
import type { MemorySettingsValue } from "./settings.js";

const require = createRequire(import.meta.url);
const failure = (cause: unknown) =>
  new MemoryStartupError({
    message: "Managed memory lifecycle failed",
    cause: Redacted.make(cause),
  });
/** Native PID inspection/control is not exposed by the scoped child-process handle for upstream detached engines. */
const alive = (pid: number) =>
  Effect.try({
    try: () => {
      try {
        process.kill(pid, 0);
        return true;
      } catch (cause) {
        if (Predicate.hasProperty(cause, "code") && cause.code === "ESRCH") return false;
        throw cause;
      }
    },
    catch: failure,
  });
const stopPid = Effect.fn("Memory.stopEngine")(function* (pid: number) {
  if (!(yield* alive(pid))) return;
  yield* Effect.try({ try: () => process.kill(pid, "SIGTERM"), catch: failure });
  yield* alive(pid).pipe(
    Effect.repeat({ schedule: Schedule.spaced(50), until: (running) => !running }),
    Effect.timeout(10000),
    Effect.catchTag("TimeoutError", () =>
      Effect.try({ try: () => process.kill(pid, "SIGKILL"), catch: failure }),
    ),
  );
  yield* alive(pid).pipe(
    Effect.repeat({ schedule: Schedule.spaced(50), until: (running) => !running }),
    Effect.timeout(2000),
  );
});

export const managedMemory = Effect.fn("Memory.daemon")(function* (
  config: MemorySettingsValue,
  environment: Readonly<Record<string, string | undefined>>,
  native: { readonly alive: typeof alive; readonly stopPid: typeof stopPid } = { alive, stopPid },
) {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const http = yield* HttpClient.HttpClient;
  const { homeDirectory, projectRoot } = yield* ConfigLocation;
  const upstream = path.join(homeDirectory, ".agentmemory");
  yield* fs.makeDirectory(upstream, { recursive: true });
  const lease = yield* scopedStorageLease(upstream);
  const enginePath = path.join(upstream, "iii.pid");
  const workerPath = path.join(upstream, "worker.pid");
  const statePath = path.join(upstream, "engine-state.json");
  const pidAt = (file: string) =>
    fs.readFileString(file).pipe(
      Effect.catchTag("PlatformError", (error) =>
        error.reason._tag === "NotFound" ? Effect.succeed("") : Effect.fail(error),
      ),
      Effect.map((text) => {
        const pid = Number(text.trim());
        return Number.isSafeInteger(pid) && pid > 0 ? pid : undefined;
      }),
    );
  const previousEngine = yield* pidAt(enginePath);
  const previousWorker = yield* pidAt(workerPath);
  for (const pid of [previousEngine, previousWorker])
    if (pid && (yield* native.alive(pid)))
      return yield* new MemoryStartupError({
        message: "agentmemory is already running; stop that instance before starting Aster",
      });
  const packageDir = yield* Effect.try({
    try: () => path.dirname(require.resolve("@agentmemory/agentmemory/package.json")),
    catch: failure,
  });
  const raw = yield* fs.readFileString(path.join(packageDir, "iii-config.yaml")).pipe(
    Effect.flatMap((text) => Effect.try({ try: () => parse(text), catch: failure })),
    Effect.flatMap(Schema.decodeUnknownEffect(Row)),
  );
  const workers = yield* Schema.decodeUnknownEffect(
    Schema.Array(Schema.Struct({ name: Schema.String, config: Row })),
  )(raw.workers);
  const selected = workers
    .filter((worker) => worker.name !== "iii-exec")
    .map((worker) => ({ ...worker, config: { ...worker.config } }));
  selected.push({
    name: "iii-worker-manager",
    config: { port: config.port + 46023, host: "127.0.0.1" },
  });
  for (const worker of selected) {
    if (["iii-http", "iii-stream"].includes(worker.name))
      worker.config.port = config.port + (worker.name === "iii-stream" ? 1 : 0);
    if (["iii-state", "iii-stream"].includes(worker.name)) {
      const adapter = yield* Schema.decodeUnknownEffect(Row)(worker.config.adapter);
      const options = yield* Schema.decodeUnknownEffect(Row)(adapter.config);
      worker.config.adapter = {
        ...adapter,
        config: {
          ...options,
          save_interval_ms: 100,
          file_path: path.join(
            config.dataDir,
            worker.name === "iii-state" ? "state_store.db" : "stream_store",
          ),
        },
      };
    }
  }
  yield* fs.makeDirectory(config.dataDir, { recursive: true, mode: 0o700 });
  const templatePath = path.join(config.dataDir, "signals-iii-template.yaml");
  yield* fs.writeFileString(templatePath, stringify({ ...raw, workers: selected }), {
    mode: 0o600,
  });
  const secret = yield* Effect.sync(() => Redacted.make(randomBytes(32).toString("hex")));
  const connection: MemoryConnection = {
    url: `http://127.0.0.1:${config.port}`,
    secret,
    dataDir: config.dataDir,
    project: projectRoot,
    cwd: projectRoot,
  };
  const discoverEngine = Effect.gen(function* () {
    const pid = yield* pidAt(enginePath);
    if (!pid || pid === previousEngine) return undefined;
    const text = yield* fs
      .readFileString(statePath)
      .pipe(
        Effect.catchTag("PlatformError", (error) =>
          error.reason._tag === "NotFound" ? Effect.succeed("{}") : Effect.fail(error),
        ),
      );
    const state = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Row))(text);
    return state.kind === "native" &&
      [templatePath, path.join(config.dataDir, "iii-config.yaml")].includes(
        String(state.configPath),
      )
      ? pid
      : undefined;
  });
  const child = yield* Effect.acquireRelease(
    spawner.spawn(
      ChildProcess.make(
        process.execPath,
        [
          "--use-env-proxy",
          path.join(packageDir, "dist/cli.mjs"),
          "--port",
          String(config.port),
          "--data-dir",
          config.dataDir,
        ],
        {
          cwd: projectRoot,
          env: {
            ...environment,
            AGENTMEMORY_III_CONFIG: templatePath,
            AGENTMEMORY_SECRET: Redacted.value(secret),
          },
          extendEnv: false,
          stdin: "ignore",
          forceKillAfter: "2 seconds",
        },
      ),
    ),
    (child) =>
      Effect.gen(function* () {
        if (yield* child.isRunning) {
          yield* child.kill({ forceKillAfter: "2 seconds" });
          yield* child.exitCode;
        }
        const engine = yield* discoverEngine;
        if (engine) {
          yield* Effect.sleep(500);
          yield* native.stopPid(engine);
        } else {
          const pid = yield* pidAt(enginePath);
          if (pid && pid !== previousEngine && (yield* native.alive(pid)))
            return yield* new MemoryStartupError({
              message: "Cannot establish ownership of remaining memory engine",
            });
        }
        if ((yield* pidAt(workerPath)) === child.pid) yield* fs.remove(workerPath, { force: true });
        if (engine && (yield* pidAt(enginePath)) === engine) {
          yield* fs.remove(enginePath, { force: true });
          yield* fs.remove(statePath, { force: true });
        }
      }).pipe(
        Effect.orDie,
        Effect.onExit((exit) => (exit._tag === "Failure" ? lease.retain : Effect.void)),
      ),
  );
  yield* child.stdout.pipe(Stream.runDrain, Effect.forkScoped);
  yield* child.stderr.pipe(Stream.runDrain, Effect.forkScoped);
  const health = Effect.scoped(
    Effect.gen(function* () {
      if (!(yield* child.isRunning))
        return yield* new MemoryStartupError({ message: "agentmemory exited before readiness" });
      const request = (route: string) =>
        http
          .execute(
            HttpClientRequest.get(`${connection.url}/agentmemory/${route}`).pipe(
              HttpClientRequest.setHeader("Authorization", `Bearer ${Redacted.value(secret)}`),
            ),
          )
          .pipe(
            Effect.flatMap((response) =>
              response.status === 200 ? response.json : Effect.succeed({}),
            ),
            Effect.flatMap(Schema.decodeUnknownEffect(Row)),
            Effect.timeout(1000),
          );
      const status = yield* request("health");
      if (status.status !== "healthy") return false;
      const ready = yield* request("livez");
      return Boolean(ready.viewerPort || ready.viewerSkipped);
    }),
  ).pipe(Effect.catchTag(["HttpClientError", "TimeoutError"], () => Effect.succeed(false)));
  yield* health.pipe(
    Effect.repeat({ schedule: Schedule.spaced(100), until: (ready) => ready }),
    Effect.timeout(120000),
  );
  if (!(yield* discoverEngine))
    return yield* new MemoryStartupError({
      message: "Cannot establish ownership of agentmemory engine",
    });
  return connection;
});
