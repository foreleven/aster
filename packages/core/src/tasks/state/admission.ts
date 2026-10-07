import type { AgentConversations } from "@aster/agent";
import { signalMessage } from "../../signals/state/store.js";
import { createHash } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import { ApplicationError, TaskMessage, TaskDeliveryInput, type Task } from "@aster/api-contracts";
import { Effect } from "effect";
import { ContextRegistry } from "../../context/registry.js";

export const taskPathFor = (source: string, requestId: string) =>
  `/tasks/${createHash("sha256")
    .update(JSON.stringify([source, requestId]))
    .digest("hex")}`;
export const delegateInput = (
  message: TaskMessage,
  task: Extract<Task, { _tag: "Delegate" | "Agent" }>,
): TaskDeliveryInput => ({
  requestId: message.requestId,
  source: message.source,
  target: taskPathFor(message.source, message.requestId),
  createdAt: message.createdAt,
  causal: message.causal,
  ...(message.evidence ? { evidence: message.evidence } : {}),
  agent: task._tag === "Agent" ? "internal" : task.agent,
  task: task.task,
  replyTo: task.replyTo,
  ...(task._tag === "Delegate" && task.action ? { action: task.action } : {}),
});
/** A Signal may only deliver its frozen occurrence; Goal tools run in the active owner's scope. */
export const sourceTask: (
  registry: ContextRegistry["Service"],
  messages: AgentConversations["Service"],
  source: string,
  requestId: string,
) => Effect.Effect<TaskMessage | undefined, ApplicationError> = Effect.fn("Task.source")(function* (
  registry: ContextRegistry["Service"],
  messages: AgentConversations["Service"],
  source: string,
  requestId: string,
): Effect.fn.Return<TaskMessage | undefined, ApplicationError> {
  const record = registry.get(source);
  if (source.startsWith("/goals/")) {
    if ((record?.state as { status?: string } | undefined)?.status === "active") return undefined;
  } else if (record && source.startsWith("/signals/")) {
    const message = yield* signalMessage(messages, source, requestId);
    if (message) return message;
  }
  return yield* new ApplicationError({
    kind: "conflict",
    message: "Task source is missing, ended, or has no committed occurrence",
  });
});
export const validateTaskMessage: (
  registry: ContextRegistry["Service"],
  input: TaskMessage,
  messages: AgentConversations["Service"],
) => Effect.Effect<void, ApplicationError> = Effect.fn("Task.validateMessage")(function* (
  registry: ContextRegistry["Service"],
  input: TaskMessage,
  messages: AgentConversations["Service"],
): Effect.fn.Return<void, ApplicationError> {
  const saved = yield* sourceTask(registry, messages, input.source, input.requestId);
  if (saved && !isDeepStrictEqual(saved, input))
    return yield* new ApplicationError({
      kind: "conflict",
      message: "Task differs from its frozen Signal occurrence",
    });
});
