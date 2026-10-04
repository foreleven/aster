import { randomUUID } from "node:crypto";
import { Deferred, Effect } from "effect";
import type { GoalActorContext } from "./task-execution.js";

type Phase =
  | { readonly _tag: "Restoring" }
  | { readonly _tag: "Idle" }
  | {
      readonly _tag: "Running";
      readonly generation: string;
      readonly cancellation: Deferred.Deferred<void>;
    };

/** Private Behavior state. Only mailbox handlers call mutators; background work only checks generation. */
export const goalEvaluationSchedule = () => {
  let phase: Phase = { _tag: "Restoring" };
  let pending = false;
  let activated = false;
  let queued = false;

  const generation = () => (phase._tag === "Running" ? phase.generation : undefined);
  const enqueue = Effect.fnUntraced(function* (context: GoalActorContext) {
    if (queued) {
      pending = true;
      return;
    }
    queued = true;
    yield* context.self.tell({ _tag: "RunNext" });
  });
  const start = Effect.fnUntraced(function* () {
    if (phase._tag !== "Idle" || !activated) {
      pending = true;
      return undefined;
    }
    const cancellation = yield* Deferred.make<void>();
    const currentGeneration = randomUUID();
    phase = { _tag: "Running", generation: currentGeneration, cancellation };
    pending = false;
    return {
      generation: currentGeneration,
      cancelled: Deferred.await(cancellation).pipe(Effect.andThen(Effect.interrupt)),
    };
  });
  const cancel = Effect.fnUntraced(function* () {
    const previous = phase;
    if (phase._tag !== "Restoring") phase = { _tag: "Idle" };
    pending = false;
    if (previous._tag === "Running") yield* Deferred.succeed(previous.cancellation, undefined);
  });
  return {
    generation,
    enqueue,
    start,
    cancel,
    dequeue: Effect.sync(() => {
      queued = false;
    }),
    hasPending: () => pending,
    isQueued: () => queued,
    finish: (id: string) =>
      Effect.sync(() => {
        if (generation() !== id) return false;
        phase = { _tag: "Idle" };
        return true;
      }),
    reconciled: Effect.sync(() => {
      if (phase._tag === "Restoring") phase = { _tag: "Idle" };
    }),
    activate: Effect.sync(() => {
      activated = true;
    }),
    isRecovered: () => phase._tag !== "Restoring",
    isReady: () => activated && phase._tag !== "Restoring",
  };
};
