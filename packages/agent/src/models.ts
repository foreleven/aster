import type { StreamFn } from "@earendil-works/pi-agent-core";
import { createModels, type Model, type Api } from "@earendil-works/pi-ai";
import { minimaxProvider } from "@earendil-works/pi-ai/providers/minimax";
import { anthropicProvider } from "@earendil-works/pi-ai/providers/anthropic";
import { openaiProvider } from "@earendil-works/pi-ai/providers/openai";
import { Config, ConfigProvider, Context, Effect, Layer, Redacted, Schema } from "effect";
import { secretConfig } from "./configuration.js";
import { AgentError } from "./shared/contracts.js";

export const ModelConfig = Schema.Struct({
  name: Schema.String,
  provider: Schema.Literals(["minimax", "anthropic", "openai"]),
  model: Schema.String,
  url: Schema.String,
  apiKey: Schema.String,
});
export type ModelConfig = typeof ModelConfig.Type;

export interface ResolvedModel {
  readonly model: Model<Api>;
  readonly stream: StreamFn;
  readonly getApiKey: () => string;
}
export class Models extends Context.Service<
  Models,
  {
    readonly resolve: (name: string) => Effect.Effect<ResolvedModel, AgentError>;
  }
>()("agent/Models") {
  static layer(configs: readonly ModelConfig[]) {
    return Layer.effect(
      Models,
      Effect.gen(function* () {
        const provider = yield* ConfigProvider.ConfigProvider;
        return yield* Effect.try({
          try: () => {
            const entries = new Map(configs.map((config) => [config.name, { ...config }]));
            if (entries.size !== configs.length) throw new AgentError("Model names must be unique");
            const models = createModels();
            models.setProvider(minimaxProvider());
            models.setProvider(anthropicProvider());
            models.setProvider(openaiProvider());
            return Models.of({
              resolve: (name) =>
                Effect.gen(function* () {
                  const config = entries.get(name);
                  if (!config) return yield* Effect.fail(new AgentError(`Unknown model: ${name}`));
                  const key = yield* secretConfig(config.apiKey, provider).pipe(
                    Effect.mapError(
                      (cause) => new AgentError("Model credential unavailable", [], { cause }),
                    ),
                  );
                  return yield* Effect.try({
                    try: () => {
                      return {
                        model: {
                          id: config.model,
                          name: config.name,
                          provider: config.provider,
                          api:
                            config.provider === "openai"
                              ? "openai-completions"
                              : "anthropic-messages",
                          baseUrl: config.url,
                          reasoning: false,
                          input: ["text"],
                          contextWindow: 200_000,
                          maxTokens: 8192,
                          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
                        } satisfies Model<Api>,
                        stream: models.streamSimple.bind(models),
                        getApiKey: () => Redacted.value(key),
                      };
                    },
                    catch: (cause) =>
                      cause instanceof AgentError
                        ? cause
                        : new AgentError("Model resolution failed", [], { cause }),
                  });
                }),
            });
          },
          catch: (cause) =>
            cause instanceof AgentError
              ? cause
              : new AgentError("Models initialization failed", [], { cause }),
        });
      }),
    );
  }

  static readonly configured = Layer.unwrap(
    Config.schema(Schema.Array(ModelConfig), ["config", "models"]).pipe(
      Config.withDefault([]),
      Effect.map((configs) => Models.layer(configs)),
    ),
  );
}
