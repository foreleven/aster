import { Context, Effect, Layer } from "effect";
import { Models } from "@aster/agent";
import { DurableContext, ExternalAgents } from "@aster/core";
import { PiDurableContext } from "../storage/pi-durable-context.js";
import { makePiRuntime, piExternalAgent } from "./agent.js";

/** One storage lease and one Harness own Context transactions and task execution.
 * Core receives only its existing storage and execution ports. */
const make = Effect.fn("PiDurableBackend.make")(function* (
  options: Parameters<typeof makePiRuntime>[0],
) {
  const runtime = yield* makePiRuntime(options);
  const contexts = yield* PiDurableContext.fromRuntime(runtime);
  return { contexts, agent: piExternalAgent(runtime) };
});

export const PiDurableBackend = {
  make,
  /** Explicit host composition for a Pi-owned shard. File/Pi routing and migration
   * decide which Contexts belong here; this Layer never opens a shadow writer. */
  layer: (options: Parameters<typeof make>[0]) =>
    Layer.effectContext(
      make(options).pipe(
        Effect.map(({ contexts, agent }) =>
          Context.make(DurableContext, contexts).pipe(Context.add(ExternalAgents, { pi: agent })),
        ),
      ),
    ).pipe(Layer.provide(Models.configured)),
};
