import { Effect, Layer } from "effect";
import { FetchHttpClient } from "effect/http";
import { makeSystemOneClient } from "../src/system-one/client.js";
import { resolveSystemOneSettings, type SystemOneConfig } from "../src/system-one/settings.js";
import { ConfigProvider } from "effect";
export const http = (fetch: typeof globalThis.fetch) =>
  FetchHttpClient.layer.pipe(Layer.provide(Layer.succeed(FetchHttpClient.Fetch, fetch)));
export const systemOne = (config: SystemOneConfig, fetch: typeof globalThis.fetch) =>
  resolveSystemOneSettings(config, ConfigProvider.fromUnknown({})).pipe(
    Effect.flatMap(makeSystemOneClient),
    Effect.provide(http(fetch)),
  );
