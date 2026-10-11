import { SystemOneClient, DecisionError } from "@aster/core";
import { ConfigProvider, Effect, Layer, Redacted } from "effect";
import { HttpClient } from "effect/http";
import * as NodeHttpClient from "@effect/platform-node/NodeHttpClient";
import { ProxyAgent } from "@effect/platform-node/Undici";
import { readSystemOneSettings } from "./settings.js";
import { makeSystemOneClient } from "./client.js";
export const SystemOneTransport = {
  layer: Layer.effect(
    SystemOneClient,
    Effect.gen(function* () {
      const settings = yield* readSystemOneSettings;
      if (settings?.proxy !== undefined) {
        const proxy = settings.proxy;
        // This Layer owns a private dispatcher; other transports retain the host client.
        const dispatcher = yield* Effect.acquireRelease(
          Effect.try({
            try: () => new ProxyAgent(Redacted.value(proxy)),
            catch: () => new ConfigProvider.SourceError({ message: "Invalid System One proxy" }),
          }),
          (agent) => Effect.promise(() => agent.destroy()),
        );
        const client = yield* NodeHttpClient.makeUndici.pipe(
          Effect.provideService(NodeHttpClient.Dispatcher, dispatcher),
        );
        return yield* makeSystemOneClient(settings).pipe(
          Effect.provideService(HttpClient.HttpClient, client),
        );
      }
      return settings
        ? yield* makeSystemOneClient(settings)
        : {
            configured: false,
            systemOne: () =>
              Effect.fail(new DecisionError({ message: "System One is not configured" })),
          };
    }),
  ),
};
