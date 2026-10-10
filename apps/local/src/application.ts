import { localConfiguration } from "./configuration.js";
import { Effect, Fiber, Layer } from "effect";
import { withStorageLease, StorageSettings } from "@aster/infra";
import { localRuntimeLayer } from "./services.js";
import { LocalHttpApi } from "./http-api.js";
import { waitForShutdown } from "./shutdown.js";

export const startApplication = async (
  projectRoot: string,
  configPath: string,
  options: { readonly source?: boolean } = {},
) => {
  const configured = LocalHttpApi.layer(options).pipe(Layer.provide(localRuntimeLayer));
  const sources = localConfiguration(projectRoot, configPath);
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        // Process signals and the ownership lock outlive every runtime/adapter finalizer.
        const shutdown = yield* waitForShutdown.pipe(Effect.forkScoped);
        const { root } = yield* StorageSettings;
        const stopped = Fiber.join(shutdown);
        const running = Effect.gen(function* () {
          const api = yield* LocalHttpApi;
          console.log(
            `Aster running at ${api.url}. Work Contexts and Goals are active; press Ctrl+C to stop.`,
          );
          yield* stopped;
        }).pipe(Effect.provide(configured));
        // Cancellation covers the whole Layer graph, including interrupted acquisition.
        yield* withStorageLease(root, running).pipe(Effect.raceFirst(stopped));
      }),
    ).pipe(Effect.provide(sources)),
  );
};
