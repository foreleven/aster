import assert from "node:assert/strict";
import { test } from "node:test";
import { ConfigProvider, Effect, Layer, Path } from "effect";
import { MemorySettings } from "../src/agentmemory/settings.js";
import { ConfigLocation } from "../src/config/location.js";

const settings = (entry: unknown) =>
  MemorySettings.pipe(
    Effect.provide(
      MemorySettings.layer.pipe(
        Layer.provide(
          Layer.mergeAll(
            Path.layer,
            Layer.succeed(ConfigLocation, {
              baseDir: "/config",
              projectRoot: "/project",
              homeDirectory: "/home/test",
              envPath: "/project/.env",
            }),
            ConfigProvider.layer(
              ConfigProvider.fromUnknown(
                { contexts: { "/memory": entry } },
                { preserveEmptyStrings: true },
              ),
            ),
          ),
        ),
      ),
    ),
  );

test("memory defaults and relative/home paths use captured locations", async () => {
  for (const [entry, expected] of [
    [{}, "/home/test/.aster/memory"],
    [{ config: { dataDir: "./data" } }, "/config/data"],
    [{ config: { dataDir: "~/data" } }, "/home/test/data"],
  ] as const) {
    const result = await Effect.runPromise(Effect.scoped(settings(entry)));
    assert.equal(result.dataDir, expected);
    assert.equal(result.port, 3111);
    assert.equal(result.autoCompress, false);
  }
});

test("provided memory capabilities must be complete and satisfy backend constraints", async () => {
  for (const config of [
    { llm: {} },
    { llm: { provider: "openai" } },
    { llm: { provider: "openai", model: "", apiKeyEnv: "KEY" } },
    { embedding: {} },
    { embedding: { provider: "openai" } },
    { autoCompress: true },
    { port: 19513 },
    {
      llm: {
        provider: "gemini",
        model: "test",
        apiKeyEnv: "KEY",
        baseUrl: "https://unused.invalid",
      },
    },
  ]) {
    const result = await Effect.runPromise(Effect.scoped(settings({ config }).pipe(Effect.result)));
    assert.equal(result._tag, "Failure", JSON.stringify(config));
  }
});
