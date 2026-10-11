import { ProcessEnvironment, MemoryBackend, RuntimeConfigurationError } from "@aster/core";
import { secretConfig } from "../config/credentials.js";
import { ConfigProvider, Effect, FileSystem, Layer, Path, Redacted } from "effect";
import { MemorySettings } from "./settings.js";
import { managedMemory } from "./daemon.js";
import { memoryEnvironment } from "./environment.js";
import { openProvenance } from "./provenance.js";
import { makeMemoryClient } from "./client.js";
import { makeCapture } from "./capture.js";

export const AgentMemoryBackend = {
  layer: Layer.effect(
    MemoryBackend,
    Effect.gen(function* () {
      const config = yield* MemorySettings;
      const provider = yield* ConfigProvider.ConfigProvider;
      const source = { ...Redacted.value((yield* ProcessEnvironment).values) };
      for (const name of [config.llm?.apiKeyEnv, config.embedding?.apiKeyEnv])
        if (name) source[name] = Redacted.value(yield* secretConfig(`\${${name}}`, provider));
      const connection = yield* managedMemory(config, yield* memoryEnvironment(config, source));
      const provenance = yield* openProvenance(config.dataDir);
      const client = yield* makeMemoryClient(connection, provenance);
      const capture = yield* makeCapture(client, provenance, connection);
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const connectionPath = path.join(config.dataDir, "connection.json");
      yield* Effect.acquireRelease(
        fs.writeFileString(
          connectionPath,
          JSON.stringify({ ...connection, secret: Redacted.value(connection.secret) }),
          { mode: 0o600 },
        ),
        () => fs.remove(connectionPath, { force: true }).pipe(Effect.orDie),
      );
      return {
        description: config.description,
        retrieval: config.embedding ? ("hybrid" as const) : ("bm25" as const),
        ...(config.llm ? { llm: { provider: config.llm.provider, model: config.llm.model } } : {}),
        recall: { search: (query: string) => client.search(query), expand: client.expand },
        save: client.save,
        ...capture,
      };
    }).pipe(
      Effect.mapError(
        (cause) =>
          new RuntimeConfigurationError({
            message: "Cannot initialize managed memory",
            cause: Redacted.make(cause),
          }),
      ),
    ),
  ).pipe(Layer.provide(MemorySettings.layer)),
};
