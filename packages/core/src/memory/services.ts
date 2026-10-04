import type { ActorRef } from "@aster/actor";
import { Context, Deferred, Effect, Layer } from "effect";
import { MemoryBackend, MemoryRecall, ContextCaptureSink } from "../context/memory.js";
import { ContextRegistry } from "../context/registry.js";
import { RuntimeIntegrations, defineIntegration } from "../runtime/integration.js";
import { MemoryActor, memoryView, type MemoryCommand } from "./actor.js";

/** Internal consumer assembly. Hosts supply only MemoryBackend, never the Actor graph. */
export const memoryLayer = Layer.effectContext(
  Effect.gen(function* () {
    const backend = yield* MemoryBackend;
    const registry = yield* ContextRegistry;
    yield* registry.registerViews([memoryView]);
    const modules = yield* RuntimeIntegrations;
    const ready = yield* Deferred.make<ActorRef<MemoryCommand>>();
    yield* modules.register(
      defineIntegration({
        name: "memory",
        phase: "consumer",
        services: Context.make(MemoryBackend, backend).pipe(Context.add(ContextRegistry, registry)),
        activate: (system) =>
          Effect.gen(function* () {
            const root = yield* system.spawn("memory", MemoryActor);
            yield* root.ask<void>((replyTo) => ({ _tag: "Ready", replyTo }));
            yield* Deferred.succeed(ready, root);
            return { ready: Effect.void, stop: system.stop(root) };
          }),
      }),
    );
    return Context.make(MemoryRecall, backend.recall).pipe(
      Context.add(ContextCaptureSink, {
        capture: (input) =>
          Deferred.await(ready).pipe(
            Effect.flatMap((root) =>
              root.ask<void>((replyTo) => ({ _tag: "Capture", input, replyTo })),
            ),
            Effect.orDie,
          ),
        drain: backend.drain,
      }),
    );
  }),
);
