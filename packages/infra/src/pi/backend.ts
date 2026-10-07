import { Effect } from "effect";
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

export const PiDurableBackend = { make };
