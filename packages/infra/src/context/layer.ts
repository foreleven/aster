import { DurableContext, makeDurableContext } from "@aster/core";
import { Effect, Layer, Path } from "effect";
import { StorageSettings } from "../storage/settings.js";
import { makeContextSessionPersistence } from "./session-store.js";

export const ContextStorage = {
  layer: Layer.effect(
    DurableContext,
    Effect.gen(function* () {
      const { root } = yield* StorageSettings;
      const path = yield* Path.Path;
      return yield* makeContextSessionPersistence(path.join(root, "context-sessions")).pipe(
        Effect.flatMap(makeDurableContext),
      );
    }),
  ),
};
