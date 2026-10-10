import { AgentError, ModelConfig, Models } from "@aster/agent";
import { Config, ConfigProvider, Effect, Layer, Redacted, Schema } from "effect";
import { secretConfig } from "./credentials.js";

const ModelEntry = Schema.Struct({ ...ModelConfig.fields, apiKey: Schema.String });
const modelSettings = Config.schema(Schema.Array(ModelEntry), ["config", "models"]).pipe(
  Config.withDefault([]),
);

/** Resolve application configuration before supplying model registrations to the SDK adapter. */
export const ConfiguredModels = {
  layer: Layer.unwrap(
    Effect.gen(function* () {
      const configs = yield* modelSettings;
      const provider = yield* ConfigProvider.ConfigProvider;
      const resolved = yield* Effect.forEach(configs, (config) =>
        secretConfig(config.apiKey, provider).pipe(
          Effect.map((apiKey) => ({ ...config, apiKey })),
          Effect.mapError(
            (cause) =>
              new AgentError("Model credential unavailable", [], { cause: Redacted.make(cause) }),
          ),
        ),
      );
      return Models.layer(resolved);
    }),
  ),
};
