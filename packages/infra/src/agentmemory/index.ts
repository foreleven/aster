import { makeMemoryBackend } from "./backend.js";
import { ProcessEnvironment, MemoryBackend } from "@aster/core";
import { secretConfig } from "../config/credentials.js";
import { ConfigLocation } from "../config/location.js";
import { ConfigProvider, Effect, Layer, Redacted } from "effect";
import { writeFile, rm } from "node:fs/promises";
import { join } from "node:path";
import { MemorySettings } from "./settings.js";
import { managedMemory } from "./runtime.js";
import { openMemoryReader } from "./reader.js";
export * from "./settings.js";
export * from "./client.js";
export * from "./runtime.js";
export * from "./reader.js";

export const configuredMemoryReader = MemorySettings.pipe(
  Effect.flatMap((settings) => openMemoryReader(join(settings.dataDir, "connection.json"))),
  Effect.provide(MemorySettings.layer),
);

/** Infrastructure Layer: owns the daemon and its connection file. */
export const AgentMemoryBackend = {
  layer: Layer.effect(
    MemoryBackend,
    Effect.gen(function* () {
      const config = yield* MemorySettings;
      const location = yield* ConfigLocation;
      const environment = { ...Redacted.value((yield* ProcessEnvironment).values) };
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
  ).pipe(Layer.provide(MemorySettings.layer)),
};

export * from "./errors.js";
export * from "./recall.js";
export * from "./backend.js";
