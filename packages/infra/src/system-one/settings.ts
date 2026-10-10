import { secretConfig } from "../config/credentials.js";
import { Config, ConfigProvider, Effect, Schema } from "effect";
import { resolveSystemOneUrl } from "./url.js";

export const SystemOneConfig = Schema.Struct({
  url: Schema.String,
  model: Schema.String,
  apiKey: Schema.optional(Schema.String),
});
export type SystemOneConfig = typeof SystemOneConfig.Type;

export const readSystemOneSettings = Effect.gen(function* () {
  const provider = yield* ConfigProvider.ConfigProvider;
  if (!(yield* provider.load(["config", "system-one"]))) return undefined;
  const config = yield* Config.schema(SystemOneConfig, ["config", "system-one"]);
  return {
    ...config,
    url: yield* resolveSystemOneUrl(config.url, provider),
    apiKey: config.apiKey === undefined ? undefined : yield* secretConfig(config.apiKey, provider),
  };
});
