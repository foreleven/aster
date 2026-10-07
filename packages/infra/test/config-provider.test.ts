import assert from "node:assert/strict";
import { test } from "node:test";
import { mkdtemp, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Config, ConfigProvider, Effect, Layer, Redacted, Schema } from "effect";
import { ConfigLocation, GoalSettings, secretConfig, signalSettings } from "@aster/core";
import { LocalConfig, SystemOneClientLive } from "../src/index.js";
import { SystemOneClient } from "@aster/core";

test("local provider precedence, exact credentials and config-relative locations do not mutate process.env", async (t) => {
  const dir = await mkdtemp(join(tmpdir(), "aster-provider-"));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const configPath = join(dir, "app.yaml"),
    envPath = join(dir, ".env");
  await writeFile(
    configPath,
    'http: {port: 4317}\ncontexts:\n  /lark:\n    children:\n      /im: {config: {pollIntervalMs: 900000}}\nsignals:\n  test: {trigger: {_tag: Context, when: changed}, task: {_tag: Goal, target: /goals/personal, text: "Keep ${UNEXPANDED}"}}\n',
  );
  await writeFile(envPath, "ASTER_HTTP_PORT=4318\nTEST_SECRET=dotenv-secret\n");
  const before = { ...process.env };
  const port = Config.Port("port").pipe(Config.nested("http"));
  for (const [environment, overrides, expected] of [
    [{}, {}, 4318],
    [{ ASTER_HTTP_PORT: "4319" }, {}, 4319],
    [{ ASTER_HTTP_PORT: "4319" }, { http: { port: 4320 } }, 4320],
  ] as const) {
    const layer = LocalConfig.layer({
      configPath,
      envPath,
      projectRoot: dir,
      environment,
      overrides,
    });
    await Effect.runPromise(
      Effect.gen(function* () {
        assert.equal(yield* port, expected);
        assert.equal((yield* ConfigLocation).baseDir, dir);
        assert.deepEqual((yield* signalSettings)[0]!.task, {
          _tag: "Goal",
          target: "/goals/personal",
          text: "Keep ${UNEXPANDED}",
        });
        const provider = yield* ConfigProvider.ConfigProvider;
        const secret = yield* secretConfig("${TEST_SECRET}", provider);
        assert.equal(String(secret), "<redacted>");
        assert.equal(Redacted.value(secret), "dotenv-secret");
        assert.equal(
          yield* Config.schema(Schema.Int, [
            "contexts",
            "/lark",
            "children",
            "/im",
            "config",
            "pollIntervalMs",
          ]),
          900000,
        );
      }).pipe(Effect.provide(layer)),
    );
  }
  assert.equal(
    JSON.stringify({ ...process.env }) === JSON.stringify(before),
    true,
    "Configuration must not mutate process.env",
  );
  for (const invalid of ["bad", ""]) {
    await assert.rejects(
      Effect.runPromise(
        port.pipe(
          Effect.provide(
            LocalConfig.layer({
              configPath,
              envPath,
              projectRoot: dir,
              environment: { ASTER_HTTP_PORT: invalid },
            }),
          ),
        ),
      ),
    );
  }
  await rm(envPath);
  assert.equal(
    await Effect.runPromise(
      port.pipe(
        Effect.provide(
          LocalConfig.layer({ configPath, envPath, projectRoot: dir, environment: {} }),
        ),
      ),
    ),
    4317,
  );
  await writeFile(configPath, "invalid: [");
  await assert.rejects(
    Effect.runPromise(
      port.pipe(
        Effect.provide(
          LocalConfig.layer({ configPath, envPath, projectRoot: dir, environment: {} }),
        ),
      ),
    ),
    /Invalid YAML/,
  );
});

test("module settings read structured provider values and disabled decisions need no credentials", async () => {
  const config = ConfigProvider.layer(
    ConfigProvider.fromUnknown({
      config: { agent: { model: "test" } },
      contexts: { "/custom": { private: { count: 3 } } },
    }),
  );
  await Effect.runPromise(
    Effect.gen(function* () {
      assert.deepEqual(
        (yield* GoalSettings).definitions.map((goal) => goal.slug),
        ["personal"],
      );
      assert.equal((yield* SystemOneClient).configured, false);
    }).pipe(
      Effect.provide(
        Layer.mergeAll(GoalSettings.layer, SystemOneClientLive.layer).pipe(
          Layer.provide(
            Layer.mergeAll(
              config,
              Layer.succeed(ConfigLocation, {
                baseDir: "/tmp",
                projectRoot: "/tmp",
                envPath: "/tmp/.env",
              }),
            ),
          ),
        ),
      ),
    ),
  );
});

test("executor environments use captured private keys even after dotenv changes", async (t) => {
  const { ProcessEnvironment } = await import("@aster/core");
  const { agentEnvironment } = await import("../src/process/environment.js");
  const dir = await mkdtemp(join(tmpdir(), "aster-captured-env-"));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const configPath = join(dir, "config.yaml");
  const envPath = join(dir, ".env");
  await writeFile(configPath, "{}");
  await writeFile(envPath, "PROJECT_SECRET=private\n");
  const captured = await Effect.runPromise(
    ProcessEnvironment.pipe(
      Effect.provide(
        LocalConfig.layer({
          configPath,
          envPath,
          projectRoot: dir,
          environment: {
            PATH: "/captured/bin",
            DOUBAO_CDP_ENDPOINT: "http://captured:9226",
            ASTER_HTTP_PORT: "9000",
          },
        }),
      ),
    ),
  );
  await rm(envPath);
  assert.deepEqual(agentEnvironment(captured), {
    PATH: "/captured/bin",
    DOUBAO_CDP_ENDPOINT: "http://captured:9226",
  });
});

test("System One resolves credential references exactly once in its Layer", async () => {
  const sources = ConfigProvider.layer(
    ConfigProvider.fromUnknown({
      config: { "system-one": { url: "http://unused", model: "fake", apiKey: "${SERVICE_KEY}" } },
      secrets: { SERVICE_KEY: "${LITERAL_SECRET_VALUE}" },
    }),
  );
  const client = await Effect.runPromise(
    SystemOneClient.pipe(Effect.provide(SystemOneClientLive.layer.pipe(Layer.provide(sources)))),
  );
  assert.equal(client.configured, true);
});
