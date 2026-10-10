import type { StreamFn } from "@earendil-works/pi-agent-core";
import { createModels, type Model, type Api } from "@earendil-works/pi-ai";
import { minimaxProvider } from "@earendil-works/pi-ai/providers/minimax";
import { anthropicProvider } from "@earendil-works/pi-ai/providers/anthropic";
import { openaiProvider } from "@earendil-works/pi-ai/providers/openai";
import { Context, Effect, Layer, Redacted, Schema } from "effect";
import { AgentError } from "./shared/contracts.js";

/** Resolved model registration input; source loading and credential references belong to infrastructure. */
export const ModelConfig = Schema.Struct({
  name: Schema.NonEmptyString,
  provider: Schema.Literals(["minimax", "anthropic", "openai"]),
  model: Schema.NonEmptyString,
  url: Schema.NonEmptyString,
  apiKey: Schema.Redacted(Schema.String),
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
        if (new Set(configs.map((config) => config.name)).size !== configs.length)
          return yield* Effect.fail(new AgentError("Model names must be unique"));
        const entries = new Map(configs.map((config) => [config.name, { ...config }]));
        return yield* Effect.try({
          try: () => {
            const models = createModels();
            models.setProvider(minimaxProvider());
            models.setProvider(anthropicProvider());
            models.setProvider(openaiProvider());
            return Models.of({
              resolve: (name) =>
                Effect.gen(function* () {
                  const config = entries.get(name);
                  if (!config) return yield* Effect.fail(new AgentError(`Unknown model: ${name}`));
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
                        getApiKey: () => Redacted.value(config.apiKey),
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
}
