import { Context, Effect, Layer } from "effect";
import { DurableContext, ExternalAgents, makeDurableContext } from "@aster/core";
import { makeContextSessionPersistence } from "./context-sessions.js";
import { join } from "node:path";
import { makeExternalAgents } from "../agents.js";
import { StorageSettings } from "./settings.js";

/** The host supplies native Context document persistence and external execution adapters. */
export const ConfiguredDurableInfrastructure = {
  layer: Layer.effectContext(
    Effect.gen(function* () {
      const settings = yield* StorageSettings;
      const contexts = yield* makeContextSessionPersistence(
        join(settings.root, "context-sessions"),
      ).pipe(Effect.flatMap(makeDurableContext));
      const agents = yield* makeExternalAgents();
      return Context.make(DurableContext, contexts).pipe(Context.add(ExternalAgents, agents));
    }),
  ),
};
