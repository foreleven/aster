import { createHash } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import { ApplicationError, TaskMessage, TaskDeliveryInput, type Task } from "@aster/api-contracts";
import { Effect, Schema } from "effect";
import type { ContextRegistry } from "../context/registry.js";

export const taskPath = (source: string, requestId: string) =>
  `/runs/${createHash("sha256")
    .update(JSON.stringify([source, requestId]))
    .digest("hex")}`;
export const delegateInput = (
  message: TaskMessage,
  task: Extract<Task, { _tag: "Delegate" }>,
): TaskDeliveryInput => ({
  requestId: message.requestId,
  source: message.source,
  target: taskPath(message.source, message.requestId),
  createdAt: message.createdAt,
  causal: message.causal,
  evidence: message.evidence,
  agent: task.agent,
  task: task.task,
  replyTo: task.replyTo,
  action: task.action,
});
const Occurrences = Schema.Struct({
  occurrences: Schema.Array(Schema.Struct({ message: TaskMessage })),
});
/** A Signal may only deliver its frozen occurrence; Goal tools run in the active owner's scope. */
export const sourceTask = Effect.fn("Task.source")(function* (
  registry: ContextRegistry["Service"],
  source: string,
  requestId: string,
) {
  const record = registry.get(source);
  if (source.startsWith("/goals/")) {
    if ((record?.state as { status?: string } | undefined)?.status === "active") return undefined;
  } else if (record) {
    const saved = yield* Schema.decodeUnknownEffect(Occurrences)(record.state).pipe(Effect.orDie);
    const message = saved.occurrences.find((item) => item.message.requestId === requestId)?.message;
    if (message) return message;
  }
  return yield* new ApplicationError({
    kind: "conflict",
    message: "Task source is missing, ended, or has no committed occurrence",
  });
});
export const validateTaskMessage = Effect.fn("Task.validateMessage")(function* (
  registry: ContextRegistry["Service"],
  input: TaskMessage,
) {
  const saved = yield* sourceTask(registry, input.source, input.requestId);
  if (saved && !isDeepStrictEqual(saved, input))
    return yield* new ApplicationError({
      kind: "conflict",
      message: "Task differs from its frozen Signal occurrence",
    });
});
