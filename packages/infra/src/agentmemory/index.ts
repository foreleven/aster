import { makeMemoryBackend } from "./backend.js";
import {
  ConfigLocation,
  ProcessEnvironment,
  MemoryBackend,
  secretConfig,
  validateConfig,
} from "@aster/core";
import { Config, ConfigProvider, Effect, Layer, Redacted } from "effect";
import { writeFile, rm } from "node:fs/promises";
import { join } from "node:path";
import { MemoryEntry, parseMemoryConfig } from "./config.js";
import { managedMemory } from "./runtime.js";
import { openMemoryReader } from "./reader.js";
export * from "./config.js";
export * from "./client.js";
export * from "./runtime.js";
export * from "./reader.js";

export const memorySettings = Effect.gen(function* () {
  const location = yield* ConfigLocation;
  const entry = yield* Config.schema(MemoryEntry, ["contexts", "/memory"]).pipe(
    Config.withDefault({}),
  );
  return yield* validateConfig("Memory", () => parseMemoryConfig(entry, location.baseDir));
});

export const configuredMemoryReader = memorySettings.pipe(
  Effect.flatMap((settings) => openMemoryReader(join(settings.dataDir, "connection.json"))),
);

/** Infrastructure Layer: owns the daemon and its connection file. */
export const AgentMemoryBackend = {
  layer: Layer.effect(
    MemoryBackend,
    Effect.gen(function* () {
      const config = yield* memorySettings;
      const location = yield* ConfigLocation;
      const environment = { ...(yield* ProcessEnvironment).values };
      const provider = yield* ConfigProvider.ConfigProvider;
      for (const name of [config.llm?.apiKeyEnv, config.embedding?.apiKeyEnv]) {
        if (name) environment[name] = Redacted.value(yield* secretConfig(`\${${name}}`, provider));
      }
      const runtime = yield* managedMemory(config, location.projectRoot, environment);
      const connectionPath = join(config.dataDir, "connection.json");
      yield* Effect.acquireRelease(
        Effect.tryPromise(() =>
          writeFile(connectionPath, JSON.stringify(runtime.connection), { mode: 0o600 }),
        ),
        () => Effect.promise(() => rm(connectionPath, { force: true })),
      );
      return makeMemoryBackend(runtime.client, {
        description: config.description,
        retrieval: config.embedding ? "hybrid" : "bm25",
        ...(config.llm ? { llm: { provider: config.llm.provider, model: config.llm.model } } : {}),
      });
    }),
  ),
};

export * from "./errors.js";
export * from "./recall.js";
export * from "./backend.js";
