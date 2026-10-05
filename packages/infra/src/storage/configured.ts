import { Context, Effect, Layer } from "effect";
import { resolve } from "node:path";
import { Models } from "@aster/agent";
import { ConfigLocation, DurableContext, ExternalAgents } from "@aster/core";
import { LocalDurableContext } from "./local-durable.js";
import { RoutedDurableContext } from "./routed-durable.js";
import { makeExternalAgents, piAgentSettings } from "../agents.js";
import { PiDurableBackend } from "../pi/backend.js";
import { makeFileContextStore } from "./file-context-store.js";
import { PiDurableContext } from "./pi-durable-context.js";
import { routingAuthorityStore, storageSettings, StorageRoutingError } from "./routing.js";

/** Host-level infrastructure selection. Runtime/domain modules retain their ports;
 * one selected store writes each path and a shared Pi owner supplies execution. */
export const ConfiguredDurableInfrastructure = {
  layer: Layer.effectContext(
    Effect.gen(function* () {
      const settings = yield* storageSettings;
      const authority = yield* routingAuthorityStore(settings.root);
      const active = yield* authority.verify(settings.authority);
      const local = yield* Effect.try({
        try: () => makeFileContextStore(settings.authority.localDirectory),
        catch: (cause) =>
          new StorageRoutingError({ message: "Cannot open Local Context storage", cause }),
      }).pipe(Effect.flatMap(LocalDurableContext.fromStore));
      const execution = yield* piAgentSettings;
      const { baseDir } = yield* ConfigLocation;
      const pi = settings.authority.pi;
      if (
        pi &&
        execution?.storageDirectory &&
        resolve(baseDir, execution.storageDirectory) !== pi.directory
      )
        return yield* new StorageRoutingError({
          message:
            "agents.pi.storageDirectory must match config.durable.pi.directory for shared ownership",
        });
      const combined =
        pi && execution
          ? yield* PiDurableBackend.make({
              model: execution.model,
              directory: pi.directory,
              shardId: pi.ownerId,
            })
          : undefined;
      const piContexts =
        combined?.contexts ??
        (pi
          ? yield* PiDurableContext.directory({ directory: pi.directory, shardId: pi.ownerId })
          : undefined);
      const contexts = yield* RoutedDurableContext.make(
        { local, ...(piContexts ? { pi: piContexts } : {}) },
        settings.authority.routes,
      );
      if (
        !active &&
        settings.authority.routes.some((route) => route.backend === "pi") &&
        Object.keys(contexts.snapshot()).length > 0
      )
        return yield* new StorageRoutingError({
          message: "Existing Contexts require storage migrate before first Pi route activation",
        });
      const agents = yield* makeExternalAgents(combined?.agent);
      if (!active) yield* authority.publish(settings.authority, 0);
      return Context.make(DurableContext, contexts).pipe(Context.add(ExternalAgents, agents));
    }),
  ).pipe(Layer.provide(Models.configured)),
};
