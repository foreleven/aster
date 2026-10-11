import { Effect } from "effect";
import { ApplicationError } from "../operations.js";
import { CurrentActors } from "../services/actors.js";
import { loadTaskState } from "./queries.js";
import { DurableContext } from "../context/store.js";
import {
  prepareInternalTask,
  prepareExternalTask,
  type InternalTaskInput,
  type ExternalTaskInput,
  type TaskOrigin,
  type FollowupTaskInput,
} from "./contracts.js";
import { deliverTask, followupTask } from "./delivery.js";

/** Lookup existing owner receipts only; this never dispatches or wakes Task execution. */
export const taskOperationReceipt = Effect.fn("Tasks.operationReceipt")(function* (
  registry: DurableContext["Service"],
  owner: string,
  operationId: string,
) {
  for (const entry of registry.directory()) {
    if (!/^\/tasks\/[^/]+$/.test(entry.path)) continue;
    const task = yield* loadTaskState(registry, entry.path).pipe(Effect.orDie);
    if (task.admission.source !== owner && task.admission.replyTo !== owner) continue;
    const input = task.inputs.find((input) => input.requestId === operationId);
    if (input) return { receipt: input.receipt, taskPath: entry.path };
  }
  return undefined;
});

export const startInternalTask = Effect.fn("Tasks.startInternal")(function* (
  origin: TaskOrigin,
  input: InternalTaskInput,
) {
  const actors = yield* CurrentActors;
  const receipt = yield* deliverTask(actors, {
    ...origin,
    task: { _tag: "Agent", replyTo: origin.source, task: prepareInternalTask(input) },
  });
  return { status: "accepted" as const, ...receipt };
});

export const delegateTask = Effect.fn("Tasks.delegate")(function* (
  origin: TaskOrigin,
  input: ExternalTaskInput,
) {
  const actors = yield* CurrentActors;
  const receipt = yield* deliverTask(actors, {
    ...origin,
    task: {
      _tag: "Delegate",
      agent: input.executor,
      replyTo: origin.source,
      task: prepareExternalTask(input),
    },
  });
  return { status: "accepted" as const, ...receipt };
});

export const sendTaskInput = Effect.fn("Tasks.sendInput")(function* (input: FollowupTaskInput) {
  const registry = yield* DurableContext;
  const task = yield* loadTaskState(registry, input.target);
  if (task.admission.source !== input.source && task.admission.replyTo !== input.source)
    return yield* new ApplicationError({
      kind: "conflict",
      message: "Task does not belong to this Goal",
    });
  return yield* followupTask(yield* CurrentActors, input);
});
