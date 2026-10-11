import { testLocalConfig } from "./configuration-fixtures.js";
import assert from "node:assert/strict";
import { test } from "node:test";
import { mkdtemp, writeFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Config, Effect } from "effect";
import { SignalDefinitions } from "@aster/core";
import { AgentModels } from "@aster/core/agent";
import { LarkSettings } from "@aster/integrations";
import { ConfiguredModels } from "@aster/infra";

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
  my-signal: {trigger: {_tag: Context, when: changed}, action: {_tag: SendGoalMessage, goalPath: /goals/personal, text: original}}
  another: {trigger: {_tag: Context, when: changed}, action: {_tag: SendGoalMessage, goalPath: /goals/personal, text: keep}}
config:
  models:
    - {name: one, provider: openai, model: gpt-4o-mini}
    - {name: two, provider: openai, model: gpt-4o}
`,
  );
  const layer = testLocalConfig({
    configPath,
    envPath,
    projectRoot: dir,
    environment: {
      ASTER_SIGNALS_MY_SIGNAL_ACTION_TEXT: "overridden",
      ASTER_CONTEXTS_LARK_CHILDREN_IM_CONFIG_POLL_INTERVAL_MS: "5000",
      ASTER_CONFIG_MODELS_0_MODEL: "gpt-4o",
      SECRET: "captured-secret",
    },
    overrides: { signals: { another: { action: { text: "explicit" } } } },
  });
  const { Layer } = await import("effect");
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        assert.deepEqual(
          (yield* SignalDefinitions.pipe(Effect.provide(SignalDefinitions.layer)))
            .map((s) => [s.slug, s.action._tag === "SendGoalMessage" ? s.action.text : ""])
            .sort(),
          [
            ["another", "explicit"],
            ["my-signal", "overridden"],
          ],
        );
        assert.equal((yield* LarkSettings).im?.config?.pollIntervalMs, 5000);
        const models = yield* AgentModels;
        const one = yield* models.resolve("one");
        assert.equal(one.id, "gpt-4o");
        assert.equal((yield* models.resolve("two")).id, "gpt-4o");
      }).pipe(
        Effect.provide(
          Layer.mergeAll(LarkSettings.layer, ConfiguredModels.layer).pipe(
            Layer.provideMerge(layer),
          ),
        ),
      ),
    ),
  );
  for (const invalid of ["- item", "42"]) {
    await writeFile(configPath, invalid);
    await assert.rejects(
      Effect.runPromise(Effect.scoped(Config.String("value").pipe(Effect.provide(layer)))),
      /Invalid YAML/,
    );
  }
});
