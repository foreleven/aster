import { secretConfig } from "../config/credentials.js";
import { Config, ConfigProvider, Effect, Redacted, Schema } from "effect";

export const SystemOneConfig = Schema.Struct({
  url: Schema.String,
  model: Schema.String,
  apiKey: Schema.optional(Schema.String),
});
export type SystemOneConfig = typeof SystemOneConfig.Type;

export const resolveSystemOneUrl = Effect.fn("SystemOne.resolveUrl")(function* (
  template: string,
  provider: ConfigProvider.ConfigProvider,
) {
  if (/^\$\{[A-Za-z_][A-Za-z0-9_]*\}$/.test(template.trim()))
    return yield* secretConfig(template, provider);
  const pattern = /\$\{([A-Za-z_][A-Za-z0-9_]*)\}/g;
  const references = [...new Set([...template.matchAll(pattern)].map((match) => match[1]!))];
  const resolved = new Map(
    yield* Effect.forEach(references, (name) =>
      secretConfig(`\${${name}}`, provider).pipe(Effect.map((value) => [name, value] as const)),
    ),
  );
  const queryStart = template.indexOf("?");
  // Replace the template once: values containing another ${...} are literal credentials.
  const url = template.replace(pattern, (_reference, name: string, offset: number) => {
    const value = Redacted.value(resolved.get(name)!);
    return queryStart >= 0 && offset > queryStart ? encodeURIComponent(value) : value;
  });
  return Redacted.make(url);
});

export interface SystemOneSettings {
  readonly endpoint: Redacted.Redacted<string>;
  readonly model: string;
  readonly apiKey?: Redacted.Redacted<string>;
}
export const resolveSystemOneSettings = Effect.fn("SystemOne.settings")(function* (
  config: SystemOneConfig,
  provider: ConfigProvider.ConfigProvider,
) {
  const resolved = yield* resolveSystemOneUrl(config.url, provider);
  const url = yield* Effect.try({
    try: () => new URL(Redacted.value(resolved)),
    catch: () =>
      new ConfigProvider.SourceError({ message: "config.system-one.url must be HTTP or HTTPS" }),
  });
  if (
    !["http:", "https:"].includes(url.protocol) ||
    url.hash ||
    url.username ||
    url.password ||
    !config.model.trim()
  )
    return yield* new ConfigProvider.SourceError({
      message: "Invalid System One endpoint or model",
    });
  url.pathname =
    url.pathname.replace(/\/+$/, "").replace(/\/v1(?:\/systemone)?$/, "") + "/v1/systemone";
  return {
    endpoint: Redacted.make(url.toString()),
    model: config.model.trim(),
    apiKey: config.apiKey === undefined ? undefined : yield* secretConfig(config.apiKey, provider),
  } satisfies SystemOneSettings;
});
export const readSystemOneSettings = Effect.gen(function* () {
  const provider = yield* ConfigProvider.ConfigProvider;
  if (!(yield* provider.load(["config", "system-one"]))) return undefined;
  return yield* resolveSystemOneSettings(
    yield* Config.schema(SystemOneConfig, ["config", "system-one"]),
    provider,
  );
});
