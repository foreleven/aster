import { SystemOneClient, DecisionError } from "@aster/core";
import { Effect, Layer } from "effect";
import { readSystemOneSettings } from "./settings.js";
import { makeSystemOneClient } from "./client.js";
export const SystemOneTransport = {
  layer: Layer.effect(
    SystemOneClient,
    Effect.gen(function* () {
      const settings = yield* readSystemOneSettings;
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
