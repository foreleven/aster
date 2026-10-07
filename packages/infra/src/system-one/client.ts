import { TypeSafeClient, type Questions } from "@typesafe-ai/sdk";
import { DecisionError, SystemOneClient, secretConfig } from "@aster/core";
import { Config, ConfigProvider, Effect, Layer, Redacted, Schema } from "effect";

const SystemOneConfig = Schema.Struct({
  url: Schema.String,
  model: Schema.String,
  apiKey: Schema.String,
});
type SystemOneConfig = typeof SystemOneConfig.Type;

/** Explicit configuration applies to TypeSafe and compatible System One services. */
export const makeSystemOneClient = (
  config: SystemOneConfig | undefined,
  fetcher: typeof fetch = fetch,
): SystemOneClient => {
  if (!config)
    throw new Error("config.system-one is required when Signals, Goals or IM are configured");
  let url: URL;
  try {
    url = new URL(config.url);
  } catch {
    throw new Error("config.system-one.url must be an HTTP or HTTPS URL");
  }
  if (
    !["http:", "https:"].includes(url.protocol) ||
    url.search ||
    url.hash ||
    url.username ||
    url.password
  ) {
    throw new Error(
      "config.system-one.url must be an HTTP or HTTPS URL without credentials, query, or fragment",
    );
  }
  const model = config.model.trim();
  if (!model) throw new Error("config.system-one.model must be nonempty");
  const apiKey = config.apiKey;
  if (!apiKey.trim()) throw new Error("config.system-one.apiKey must be nonempty");
  // The SDK appends /v1/systemone. Accept a service root, /v1, or that full endpoint.
  const baseURL = url
    .toString()
    .replace(/\/+$/, "")
    .replace(/\/v1(?:\/systemone)?$/, "");
  const client = new TypeSafeClient({
    baseURL,
    defaultModel: model,
    apiKey: apiKey.trim(),
    fetch: fetcher,
    logLevel: "warn",
  });
  return {
    configured: true,
    systemOne: (request) =>
      Effect.tryPromise({
        // SDK cancellation covers the fetch and retry delay, not just our local wait.
        try: (signal) =>
          client.systemOne(
            {
              ...request,
              // The core contract deliberately hides the SDK's generic question union;
              // this is the single adapter boundary where the validated shapes meet.
              questions: request.questions as Questions,
              state:
                typeof request.state === "string" ? request.state : JSON.stringify(request.state),
            },
            { signal },
          ),
        catch: (cause) =>
          new DecisionError({
            cause,
            message: cause instanceof Error ? cause.message : String(cause),
          }),
      }).pipe(
        Effect.tapErrorTag("DecisionError", (error) =>
          Effect.logError(
            JSON.stringify({
              event: "system-one.request.failed",
              model,
              questionCount: Object.keys(request.questions).length,
              error: error.message,
            }),
          ),
        ),
        Effect.map((result) => ({
          answers: result.answers as {
            readonly [key: string]: {
              readonly type: string;
              readonly choice?: string;
              readonly score?: number;
              readonly confidence?: number;
              readonly legend?: Readonly<Record<string, unknown>>;
            };
          },
        })),
      ),
  };
};

export const SystemOneClientLive = {
  layer: Layer.effect(
    SystemOneClient,
    Effect.gen(function* () {
      const config = yield* Config.schema(Schema.optional(SystemOneConfig), [
        "config",
        "system-one",
      ]);
      if (!config)
        return {
          configured: false,
          systemOne: () =>
            Effect.fail(new DecisionError({ message: "System One is not configured" })),
        };
      const provider = yield* ConfigProvider.ConfigProvider;
      const apiKey = yield* secretConfig(config.apiKey, provider);
      return yield* Effect.try(() =>
        makeSystemOneClient({ ...config, apiKey: Redacted.value(apiKey) }),
      );
    }),
  ),
};
