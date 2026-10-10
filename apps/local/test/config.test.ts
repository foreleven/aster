import { testLocalConfig } from "./configuration-fixtures.js";
import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { LarkSettings } from "@aster/integrations";
import { Config, Effect, Schema } from "effect";
import { SignalDefinitions } from "@aster/core";

const settings = (file: string) =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        return {
          lark: yield* LarkSettings.pipe(Effect.provide(LarkSettings.layer)),
          custom: yield* Config.schema(Schema.optional(Schema.String), [
            "contexts",
            "/custom",
            "children",
            "/anything",
            "private",
          ]),
          systemOne: yield* Config.schema(
            Schema.optional(
              Schema.Struct({ url: Schema.String, model: Schema.String, apiKey: Schema.String }),
            ),
            ["config", "system-one"],
          ),
          signals: yield* SignalDefinitions.pipe(Effect.provide(SignalDefinitions.layer)),
        };
      }).pipe(
        Effect.provide(
          testLocalConfig({
            configPath: file,
            envPath: `${file}.env`,
            projectRoot: "/tmp",
            environment: {},
          }),
        ),
      ),
    ),
  );

test("module settings own children, defaults and validation", async () => {
  const dir = await mkdtemp(join(tmpdir(), "signals-root-config-test-"));
  try {
    const file = join(dir, "signals.yaml");
    await writeFile(
      file,
      "contexts:\n  /custom:\n    children:\n      /anything: {private: value}\n",
    );
    const config = await settings(file);
    assert.equal(config.custom, "value");
    assert.deepEqual(config.signals, []);
    assert.equal(config.systemOne, undefined);
    assert.equal(config.lark.mail, undefined);
    // The Lark module validates its own child configuration through the provider.
    await writeFile(
      file,
      "contexts:\n  /lark:\n    children:\n      /mail:\n        config: {pollIntervalMs: bad}\n",
    );
    await assert.rejects(settings(file));
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});
