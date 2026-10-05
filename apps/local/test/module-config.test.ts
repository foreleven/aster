import assert from "node:assert/strict";
import { test } from "node:test";
import { mkdtemp, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Config, Effect } from "effect";
import { signalSettings } from "@aster/core";
import { Models } from "@aster/agent";
import { LarkConfig } from "@aster/integrations";
import { LocalConfig, memorySettings } from "@aster/infra";

test("environment overrides preserve dynamic keys, nested module settings and model array entries", async (t) => {
  const dir = await mkdtemp(join(tmpdir(), "aster-structured-config-"));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const configPath = join(dir, "app.yaml"),
    envPath = join(dir, ".env");
  await writeFile(
    configPath,
    `
contexts:
  /lark:
    children:
      /im: {config: {summary: {model: one}}}
  /memory: {config: {dataDir: ./memory}}
signals:
  my-signal: {trigger: {_tag: Context, when: changed}, task: {_tag: Goal, target: /goals/personal, text: original}}
  another: {trigger: {_tag: Context, when: changed}, task: {_tag: Goal, target: /goals/personal, text: keep}}
config:
  models:
    - {name: one, provider: openai, model: first, url: 'http://unused.invalid', apiKey: '\${SECRET}'}
    - {name: two, provider: openai, model: second, url: 'http://unused.invalid', apiKey: literal}
`,
  );
  const layer = LocalConfig.layer({
    configPath,
    envPath,
    projectRoot: dir,
    environment: {
      ASTER_SIGNALS_MY_SIGNAL_TASK_TEXT: "overridden",
      ASTER_CONTEXTS_LARK_CHILDREN_IM_CONFIG_POLL_INTERVAL_MS: "5000",
      ASTER_CONFIG_MODELS_0_MODEL: "replacement",
      SECRET: "captured-secret",
    },
    overrides: { signals: { another: { task: { text: "explicit" } } } },
  });
  const { Layer } = await import("effect");
  await Effect.runPromise(
    Effect.gen(function* () {
      assert.deepEqual(
        (yield* signalSettings)
          .map((s) => [s.slug, s.task._tag === "Goal" ? s.task.text : ""])
          .sort(),
        [
          ["another", "explicit"],
          ["my-signal", "overridden"],
        ],
      );
      assert.equal((yield* LarkConfig).im?.config?.pollIntervalMs, 5000);
      assert.equal((yield* memorySettings).dataDir, join(dir, "memory"));
      const models = yield* Models;
      const one = yield* models.resolve("one");
      assert.equal(one.model.id, "replacement");
      assert.equal(one.getApiKey(), "captured-secret");
      assert.equal((yield* models.resolve("two")).model.id, "second");
    }).pipe(
      Effect.provide(
        Layer.mergeAll(LarkConfig.layer, Models.configured).pipe(Layer.provideMerge(layer)),
      ),
    ),
  );
  for (const invalid of ["- item", "42"]) {
    await writeFile(configPath, invalid);
    await assert.rejects(
      Effect.runPromise(Config.String("value").pipe(Effect.provide(layer))),
      /Invalid YAML/,
    );
  }
});
