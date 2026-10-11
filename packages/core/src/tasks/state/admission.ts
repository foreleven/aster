import { loadGoalStatus } from "../../goals/queries.js";
import type { AgentConversations } from "../../agent/runtime/session.js";
import { signalDelivery } from "../../signals/state/store.js";
import { createHash } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import { ApplicationError } from "../../operations.js";
import { TaskMessage, TaskDeliveryInput, type Task } from "../contracts.js";
import { Effect, Schema } from "effect";
import { DurableContext } from "../../context/store.js";

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
  remainingAgentTurns: message.remainingAgentTurns,
  ...(message.evidence ? { evidence: message.evidence } : {}),
  agent: task._tag === "Agent" ? "internal" : task.agent,
  task: task.task,
  replyTo: task.replyTo,
});
/** A Signal may only deliver its frozen occurrence; Goal tools run in the active owner's scope. */
export const sourceTask: (
  registry: DurableContext["Service"],
  messages: AgentConversations["Service"],
  source: string,
  requestId: string,
) => Effect.Effect<TaskMessage | undefined, ApplicationError> = Effect.fn("Task.source")(function* (
  registry: DurableContext["Service"],
  messages: AgentConversations["Service"],
  source: string,
  requestId: string,
): Effect.fn.Return<TaskMessage | undefined, ApplicationError> {
  const record = registry.get(source);
  if (source.startsWith("/goals/")) {
    if ((yield* loadGoalStatus(registry, source)).status === "active") return undefined;
  } else if (record && source.startsWith("/signals/")) {
    const message = yield* signalDelivery(messages, source, requestId);
    if (message && Schema.is(TaskMessage)(message)) return message;
  }
  return yield* new ApplicationError({
    kind: "conflict",
    message: "Task source is missing, ended, or has no committed occurrence",
  });
});
export const validateTaskMessage: (
  registry: DurableContext["Service"],
  input: TaskMessage,
  messages: AgentConversations["Service"],
) => Effect.Effect<void, ApplicationError> = Effect.fn("Task.validateMessage")(function* (
  registry: DurableContext["Service"],
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
