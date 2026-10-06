import {
  ApplicationError,
  TaskInspection,
  TaskDeliveryInput,
  TaskPath,
  FollowupTaskInput,
} from "@aster/api-contracts";
import { AgentConversations } from "@aster/agent";
import { Effect, Match, Schema } from "effect";
import type { ContextRegistry } from "../context/registry.js";
import { approvalEntries } from "../approvals/actor.js";
import { TaskState } from "./state.js";

export const inspectTask: (
  registry: ContextRegistry["Service"],
  conversations: AgentConversations["Service"],
  path: string,
) => Effect.Effect<TaskInspection, ApplicationError> = Effect.fn("Task.inspect")(
  function* (registry, conversations, path) {
    yield* Schema.decodeUnknownEffect(TaskPath)(path).pipe(
      Effect.mapError(
        () => new ApplicationError({ kind: "invalid-input", message: "Invalid Task path" }),
      ),
    );
    const record = registry.get(path);
    if (!record)
      return yield* new ApplicationError({ kind: "not-found", message: "Task not found" });
    const state = Schema.decodeUnknownSync(TaskState)(record.state);
    const entries = yield* conversations
      .read(path)
      .pipe(
        Effect.mapError(
          () => new ApplicationError({ kind: "unavailable", message: "Task messages unavailable" }),
        ),
      );
    const admission = Schema.decodeUnknownSync(TaskDeliveryInput)(
      entries.find((entry) => entry.id === state.inputs[0]!.entryId)?.data,
    );
    const messages = entries.flatMap((entry) => {
      if (entry.kind === "task.admission")
        return [
          { id: entry.id, kind: "instruction", text: admission.task.instructions, at: entry.at },
        ];
      if (entry.kind === "task.input")
        return [
          {
            id: entry.id,
            kind: "follow-up",
            text: Schema.decodeUnknownSync(FollowupTaskInput)(entry.data).text,
            at: entry.at,
          },
        ];
      if (entry.kind === "task.result")
        return [
          {
            id: entry.id,
            kind: "result",
            text: Schema.decodeUnknownSync(Schema.Struct({ text: Schema.String }))(entry.data).text,
            at: entry.at,
          },
        ];
      return [];
    });
    messages.push(
      ...(yield* conversations.tools(path).pipe(
        Effect.mapError(
          () =>
            new ApplicationError({
              kind: "unavailable",
              message: "Task execution details unavailable",
            }),
        ),
      )),
    );
    messages.sort((a, b) => a.id - b.id);
    const outcome = entries.find((entry) => entry.id === state.outcomeEntryId);
    const text = outcome
      ? Schema.decodeUnknownSync(Schema.Struct({ text: Schema.String }))(outcome.data).text
      : undefined;
    return {
      path,
      revision: record.revision ?? 0,
      agent: state.admission.agent,
      status: state.status,
      instructions: admission.task.instructions,
      sources: [...new Set(admission.task.input.flatMap((item) => item.sources))],
      hasExecution: !!state.session || state.admission.agent === "internal",
      messages,
      ...(state.status === "completed" ? { result: text } : { error: text }),
      resumptions: state.resumptions?.map((item) => ({
        requestId: item.input.requestId,
        status: item.status,
      })),
      requests: approvalEntries(registry)
        .filter((entry) => entry.contextPath === path)
        .map((entry) => ({
          id: entry.id,
          kind: entry.request.kind,
          prompt: entry.request.prompt,
          responseStatus: Match.value({
            marker: state.responses?.find((item) => item.requestId === entry.id)?.status,
            status: entry.status,
          }).pipe(
            Match.when({ marker: "unknown" }, () => "uncertain" as const),
            Match.when({ status: "acknowledged" }, () => "sent" as const),
            Match.orElse(() => "pending" as const),
          ),
        })),
    };
  },
);
