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
export const compactionRetry = "Continue after context compaction";

/** Private Behavior state. Only mailbox handlers call mutators; background work only checks generation. */
export const goalEvaluationSchedule = () => {
  let phase: Phase = { _tag: "Restoring" };
  let pending: string[] = [];
  let queued = false;
  let budgetRetries = 0;
  const generation = () => (phase._tag === "Running" ? phase.generation : undefined);
  const enqueue = Effect.fnUntraced(function* (context: GoalActorContext, reason: string) {
    if (queued) {
      pending.push(reason);
      return;
    }
    queued = true;
    yield* context.self.tell({ _tag: "Evaluate", reason });
  });
  const start = Effect.fnUntraced(function* (reason: string) {
    if (phase._tag !== "Idle") {
      pending.push(reason);
      return undefined;
    }
    const cancellation = yield* Deferred.make<void>();
    const currentGeneration = randomUUID();
    phase = { _tag: "Running", generation: currentGeneration, cancellation };
    if (reason !== compactionRetry) budgetRetries = 0;
    const combined = [...pending, reason].join("\n");
    pending = [];
    return {
      generation: currentGeneration,
      reason: combined,
      cancelled: Deferred.await(cancellation).pipe(Effect.andThen(Effect.interrupt)),
    };
  });
  const cancel = Effect.fnUntraced(function* () {
    const previous = phase;
    phase = { _tag: "Idle" };
    pending = [];
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
    hasPending: () => pending.length > 0,
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
    retryBudget: (message: string) =>
      Effect.sync(() => {
        if (!/context budget/i.test(message) || budgetRetries >= 1 || pending.length > 0)
          return false;
        budgetRetries++;
        return true;
      }),
  };
};
