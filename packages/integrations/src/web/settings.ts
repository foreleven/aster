import { Config, ConfigProvider, Context, Effect, Layer, Redacted, Schema } from "effect";
import { ProcessEnvironment, RuntimeConfigurationError } from "@aster/core";

const NonBlank = Schema.String.check(Schema.isPattern(/\S/));
const WebConfig = Schema.Struct({
  apiKeyEnv: NonBlank.pipe(Schema.withDecodingDefaultKey(Effect.succeed("TAVILY_API_KEY"))),
});
const WebEntry = Schema.Struct({
  description: Schema.optional(NonBlank),
  config: WebConfig,
});

export interface WebSearchSettingsValue {
  readonly description: string;
  readonly endpoint: string;
  readonly apiKey: Redacted.Redacted<string>;
}

export class WebSearchSettings extends Context.Service<
  WebSearchSettings,
  WebSearchSettingsValue | undefined
>()("web-search/Settings") {
  static readonly layer = Layer.effect(
    WebSearchSettings,
    Effect.gen(function* () {
      const provider = yield* ConfigProvider.ConfigProvider;
      const configured = yield* provider.load(["contexts", "/web"]);
      if (!configured) return undefined;
      const entry = yield* Config.schema(WebEntry, ["contexts", "/web"]);
      const environment = yield* ProcessEnvironment;
      const value = Redacted.value(environment.values)[entry.config.apiKeyEnv]?.trim();
      if (!value)
        return yield* new RuntimeConfigurationError({
          message: `Web search credential environment variable is missing: ${entry.config.apiKeyEnv}`,
        });
      return {
        description: entry.description ?? "Search the public web with Tavily",
        endpoint: "https://api.tavily.com/search",
        apiKey: Redacted.make(value),
      } satisfies WebSearchSettingsValue;
    }),
  );
}
