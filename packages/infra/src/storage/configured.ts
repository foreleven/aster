import { Context, Effect, Layer } from "effect";
import {
  ContextRecoveryError,
  DurableContext,
  ExternalAgents,
  makeDurableContext,
} from "@aster/core";
import { makeContextSessionPersistence } from "./context-sessions.js";
import { join } from "node:path";
import { makeExternalAgents } from "../agents.js";
import { makeFileContextStore } from "./file-context-store.js";
import { storageSettings } from "./settings.js";

/** The host supplies Local Context persistence and external execution adapters. */
export const ConfiguredDurableInfrastructure = {
  layer: Layer.effectContext(
    Effect.gen(function* () {
      const settings = yield* storageSettings;
      const contexts = yield* Effect.try({
        try: () => makeFileContextStore(settings.contextDirectory),
        catch: (cause) => new ContextRecoveryError({ path: "/", cause }),
      }).pipe(
        Effect.flatMap((store) =>
          makeContextSessionPersistence(store, join(settings.root, "context-sessions")),
        ),
        Effect.flatMap(makeDurableContext),
      );
      const agents = yield* makeExternalAgents();
      return Context.make(DurableContext, contexts).pipe(Context.add(ExternalAgents, agents));
    }),
  ),
};
