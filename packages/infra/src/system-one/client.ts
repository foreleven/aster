import { TypeSafeClient, type Questions } from "@typesafe-ai/sdk";
import { DecisionError, SystemOneClient, secretConfig } from "@aster/core";
import { randomUUID } from "node:crypto";
import { Clock, Config, ConfigProvider, Effect, Layer, Predicate, Redacted, Schema } from "effect";

const SystemOneConfig = Schema.Struct({
  url: Schema.String,
  model: Schema.String,
  apiKey: Schema.String,
});
type SystemOneConfig = typeof SystemOneConfig.Type;

/** Only transport codes cross this boundary; nested error objects may contain request data. */
const transportErrorCodes = (error: unknown, depth = 0): string[] => {
  if (depth > 4 || !Predicate.isObject(error)) return [];
  const code =
    Predicate.hasProperty(error, "code") && typeof error.code === "string" ? error.code : undefined;
  return [
    ...(code && /^[A-Z][A-Z0-9_]{0,63}$/.test(code) ? [code] : []),
    ...(Predicate.hasProperty(error, "cause") ? transportErrorCodes(error.cause, depth + 1) : []),
    ...(error instanceof AggregateError
      ? error.errors.slice(0, 8).flatMap((cause: unknown) => transportErrorCodes(cause, depth + 1))
      : []),
  ];
};

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
  return {
    configured: true,
    systemOne: Effect.fn("SystemOneClient.systemOne")(function* (request) {
      const started = yield* Clock.currentTimeMillis;
      const requestId = yield* Effect.sync(randomUUID);
      const metadata = {
        requestId,
        model,
        endpoint: url.origin,
        questionCount: Object.keys(request.questions).length,
      };
      // SDK callbacks are synchronous. Capture the caller's Clock, logger and annotations.
      const runSync = Effect.runSyncWith(yield* Effect.context<never>());
      const elapsed = () => runSync(Clock.currentTimeMillis) - started;
      let attempt = 0;
      const log = (event: string, fields: Record<string, unknown> = {}) =>
        runSync(
          Effect.logInfo(
            JSON.stringify({ event, ...metadata, attempt, elapsedMs: elapsed(), ...fields }),
          ),
        );
      const sdkLog = (message: string, ...errors: unknown[]) =>
        log("system-one.request.sdk", {
          message,
          errorCodes: [...new Set(errors.flatMap((error) => transportErrorCodes(error)))],
        });
      // A client per invocation keeps SDK retry logs attached to one logical request.
      const client = new TypeSafeClient({
        baseURL,
        defaultModel: model,
        apiKey: apiKey.trim(),
        logLevel: "info",
        logger: { debug: () => {}, info: sdkLog, warn: sdkLog, error: sdkLog },
        fetch: async (input, init) => {
          attempt++;
          const attemptStarted = elapsed();
          log("system-one.request.attempt.started");
          const response = await fetcher(input, init);
          log("system-one.request.headers", {
            status: response.status,
            headersMs: elapsed() - attemptStarted,
          });
          // Do not consume or replace the body: the SDK owns buffering and its timeout.
          return response;
        },
      });
      log("system-one.request.started", {
        timeoutMs: client.timeout,
        maxRetries: client.retry.maxRetries,
      });
      return yield* Effect.tryPromise({
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
        Effect.tap(() => Effect.sync(() => log("system-one.request.completed"))),
        Effect.onInterrupt(() => Effect.sync(() => log("system-one.request.interrupted"))),
        Effect.tapErrorTag("DecisionError", (error) =>
          Effect.logError(
            JSON.stringify({
              event: "system-one.request.failed",
              ...metadata,
              attempt,
              elapsedMs: elapsed(),
              error: error.message,
              errorCodes: [...new Set(transportErrorCodes(error.cause))],
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
      );
    }),
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
