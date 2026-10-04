import {
  Clock,
  Context,
  Effect,
  Fiber,
  HashSet,
  Layer,
  Ref,
  Semaphore,
  SynchronizedRef,
} from "effect";
import { LarkConfig } from "../config.js";
import { ImStorage } from "./storage.js";
import { parseImPolicy } from "./policy.js";
import { ImSummaryError } from "../shared/errors.js";

export interface AgentAdmission {
  readonly run: <A, E, R>(
    chat: string,
    execute: Effect.Effect<A, E, R>,
  ) => Effect.Effect<A, E | ImSummaryError, R>;
}

/** Equal-sized permits preserve FIFO admission; the service Scope owns every waiting/running fiber. */
export const makeImAgentQueue = Effect.fn("ImAgentQueue.make")(function* (
  settings: { startIntervalMs: number; concurrency: number },
  checkpoint: { load: () => number | undefined; save: (at: number) => void },
) {
  if (
    !Number.isSafeInteger(settings.startIntervalMs) ||
    settings.startIntervalMs <= 0 ||
    !Number.isSafeInteger(settings.concurrency) ||
    settings.concurrency <= 0
  )
    return yield* new ImSummaryError({ message: "Invalid IM Agent admission limits" });
  const restoredStart = checkpoint.load();
  if (restoredStart !== undefined && (!Number.isFinite(restoredStart) || restoredStart < 0))
    return yield* new ImSummaryError({ message: "Invalid persisted Agent start time" });
  const scope = yield* Effect.scope;
  const slots = yield* Semaphore.make(settings.concurrency);
  const lastStart = yield* SynchronizedRef.make(restoredStart);
  const keys = yield* Ref.make(HashSet.empty<string>());
  // Only start admission is serialized; execution holds a concurrency slot, not this lock.
  const admit = SynchronizedRef.updateEffect(
    lastStart,
    Effect.fnUntraced(function* (previous) {
      const now = yield* Clock.currentTimeMillis;
      if (previous !== undefined)
        yield* Effect.sleep(Math.max(0, previous + settings.startIntervalMs - now));
      const at = yield* Clock.currentTimeMillis;
      // Persist before execution so a restart cannot bypass spacing.
      checkpoint.save(at);
      return at;
    }),
  );
  const run: AgentAdmission["run"] = (key, execute) =>
    Effect.gen(function* () {
      const work = Effect.acquireUseRelease(
        Effect.gen(function* () {
          const claimed = yield* Ref.modify(keys, (current) =>
            HashSet.has(current, key) ? [false, current] : [true, HashSet.add(current, key)],
          );
          if (!claimed)
            return yield* new ImSummaryError({
              message: `Chat already has an Agent request: ${key}`,
            });
        }),
        () => admit.pipe(Effect.andThen(execute), slots.withPermit),
        () => Ref.update(keys, HashSet.remove(key)),
      );
      // Register caller cleanup atomically with the fork, including cancellation before join starts.
      return yield* Effect.acquireUseRelease(
        Effect.forkIn(Effect.interruptible(work), scope),
        Fiber.join,
        Fiber.interrupt,
      );
    });
  return { run } satisfies AgentAdmission;
});

export class ImAgentQueue extends Context.Service<ImAgentQueue, AgentAdmission>()(
  "lark/ImAgentQueue",
) {
  static readonly layer = Layer.effect(
    ImAgentQueue,
    Effect.gen(function* () {
      const config = yield* LarkConfig;
      const storage = yield* ImStorage;
      const policy = parseImPolicy(config.im);
      return yield* makeImAgentQueue(
        { startIntervalMs: policy.agentStartIntervalMs, concurrency: policy.agentConcurrency },
        { load: storage.lastAgentStart, save: storage.saveAgentStart },
      );
    }),
  );
}
