import { AgentError, AgentModels } from "@aster/core";
import { anthropicProvider } from "@earendil-works/pi-ai/providers/anthropic";
import { minimaxProvider } from "@earendil-works/pi-ai/providers/minimax";
import { openaiProvider } from "@earendil-works/pi-ai/providers/openai";
import { Config, Effect, Layer, Schema } from "effect";
import { createModels } from "@earendil-works/pi-ai";

const ModelEntry = Schema.Struct({
  name: Schema.NonEmptyString,
  provider: Schema.Literals(["anthropic", "minimax", "openai"]),
  model: Schema.NonEmptyString,
});
const modelSettings = Config.schema(Schema.Array(ModelEntry), ["config", "models"]).pipe(
  Config.withDefault([]),
);

/** Resolve application configuration before supplying model registrations to the SDK adapter. */
export const ConfiguredModels = {
  layer: Layer.unwrap(
    Effect.gen(function* () {
      const configs = yield* modelSettings;
      if (new Set(configs.map(({ name }) => name)).size !== configs.length)
        return yield* Effect.fail(new AgentError("Model names must be unique"));
      const models = createModels();
      const providers = new Set(configs.map(({ provider }) => provider));
      if (providers.has("anthropic")) models.setProvider(anthropicProvider());
      if (providers.has("minimax")) models.setProvider(minimaxProvider());
      if (providers.has("openai")) models.setProvider(openaiProvider());
      const aliases = new Map(
        configs.map(({ name, provider, model }) => [name, { provider, model }]),
      );
      return Layer.succeed(
        AgentModels,
        AgentModels.of({
          models,
          resolve: (alias) =>
            Effect.gen(function* () {
              const target = aliases.get(alias);
              if (!target) return yield* Effect.fail(new AgentError(`Unknown model: ${alias}`));
              const model = models.getModel(target.provider, target.model);
              if (!model)
                return yield* Effect.fail(
                  new AgentError(
                    `Configured model is unavailable: ${target.provider}/${target.model}`,
                  ),
                );
              return model;
            }),
        }),
      );
    }),
  ),
};
