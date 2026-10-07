import { ExecutionCheckpoint } from "./execution/checkpoint.js";
import { StoredTaskInput } from "./state/snapshot.js";
import { Option, Schema, Effect, Match } from "effect";
import {
  ApplicationError,
  TaskInspection,
  TaskDeliveryInput,
  TaskPath,
} from "@aster/api-contracts";
import { contextView } from "../context/definition.js";
import { TaskSnapshot } from "./state/snapshot.js";
import { AgentConversations } from "@aster/agent";
import { type ContextRegistry } from "../context/registry.js";
import { approvalEntries } from "../approvals/actor.js";
import { type CapturePolicy } from "../memory/capture.js";
const PublicTask = Schema.Struct({
  status: TaskSnapshot.fields.status,
  sourcePath: Schema.String,
  replyTo: Schema.String,
  agent: Schema.String,
  inputs: Schema.Int,
});
export const taskView: import("../context/definition.js").ContextViewPolicy = {
  matches: (path) => /^\/tasks\/[^/]+$/.test(path),
  project: (record) => {
    const canonical = Schema.decodeUnknownOption(TaskSnapshot)(record.state);
    let publicState = Schema.decodeUnknownOption(PublicTask)(record.state);
    if (Option.isSome(canonical))
      publicState = Option.some({
        status: canonical.value.status,
        sourcePath: canonical.value.admission.source,
        replyTo: canonical.value.admission.replyTo,
        agent: canonical.value.admission.agent,
        inputs: canonical.value.inputs.length,
      });
    if (Option.isNone(publicState)) return undefined;
    return {
      path: record.path,
      revision: record.revision ?? 0,
      description: record.description,
      state: publicState.value,
      messages: [],
      projection: { version: 1, visibility: "public" },
    };
  },
};
export const tasksRootView = contextView({
  matches: (path) => path === "/tasks",
  state: Schema.Struct({}),
});

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
    const state = Schema.decodeUnknownSync(TaskSnapshot)(record.state);
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
            text: renderInput(entry.data),
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
    const executionEntry = entries.findLast((entry) => entry.kind === "task.execution");
    const execution = executionEntry
      ? Schema.decodeUnknownSync(ExecutionCheckpoint)(executionEntry.data)
      : undefined;
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
      hasExecution: !!execution?.session || state.admission.agent === "internal",
      messages,
      ...(state.status === "completed" ? { result: text } : { error: text }),
      requests: approvalEntries(registry)
        .filter((entry) => entry.contextPath === path)
        .map((entry) => ({
          id: entry.id,
          kind: entry.request.kind,
          prompt: entry.request.prompt,
          responseStatus: Match.value({
            marker: execution?.deliveries.find((item) => item.requestId === entry.id)?.status,
            status: entry.status,
          }).pipe(
            Match.when({ marker: "unknown" }, () => "uncertain" as const),
            Match.when({ marker: "sending" }, () => "sending" as const),
            Match.when({ status: "acknowledged" }, () => "sent" as const),
            Match.orElse(() => "pending" as const),
          ),
        })),
    };
  },
);

/** Resolve frozen evidence from Pi; later source edits cannot change a capture. */
export const taskCapture = (messages: AgentConversations["Service"]): CapturePolicy => ({
  matches: (path) => taskView.matches!(path),
  capture: (record) => {
    const decoded = Schema.decodeUnknownOption(TaskSnapshot)(record.state);
    if (Option.isNone(decoded)) return undefined;
    const state = decoded.value;
    return {
      sessionId: `${record.path}:${state.outcomeEntryId === undefined ? "trigger" : `outcome:${state.outcomeEntryId}`}`,
      records: Effect.gen(function* () {
        const entry = yield* messages.get(record.path, state.inputs[0]!.entryId).pipe(Effect.orDie);
        const admission = Schema.decodeUnknownSync(TaskDeliveryInput)(entry.data);
        return admission.evidence ? [record, admission.evidence] : undefined;
      }),
    };
  },
});

const renderInput = (data: unknown) =>
  Match.value(Schema.decodeUnknownSync(StoredTaskInput)(data).input).pipe(
    Match.tag("Initial", ({ input }) => input.task.instructions),
    Match.tag("Message", ({ input }) => input.text),
    Match.tag(
      "Answer",
      ({ response }) => response.text ?? response.decision ?? JSON.stringify(response.answers),
    ),
    Match.tag("Check", () => "Check original execution"),
    Match.tag("Retry", () => "Retry failed execution"),
    Match.exhaustive,
  );
