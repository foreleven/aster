import { readTaskState } from "./queries.js";
import { CurrentActors } from "../services/actors.js";
import { createHash } from "node:crypto";
import { Effect, Match, Option, Schema } from "effect";
import { ContextQueryError } from "../queries/registry.js";
import { publicJson } from "../json.js";
import { ApplicationError } from "../operations.js";
import { TaskDeliveryInput, TaskPath } from "./contracts.js";
import { ExecutionCheckpoint } from "./execution/checkpoint.js";
import { StoredTaskInput, TaskOutcome, TaskSnapshot } from "./state/snapshot.js";

import { AgentConversations } from "@aster/agent/harness";
import { approvalEntries } from "../approvals/actor.js";
import { type DurableContext } from "../context/store.js";
import { type CapturePolicy } from "../memory/capture.js";

export const inspectTask = (
  registry: DurableContext["Service"],
  conversations: Pick<AgentConversations["Service"], "read" | "tools">,
  path: string,
  actors: CurrentActors["Service"],
) =>
  Effect.gen(function* () {
    yield* Schema.decodeUnknownEffect(TaskPath)(path).pipe(
      Effect.mapError(
        () => new ApplicationError({ kind: "invalid-input", message: "Invalid Task path" }),
      ),
    );
    const record = registry.get(path);
    if (!record)
      return yield* new ApplicationError({ kind: "not-found", message: "Task not found" });
    const state = yield* readTaskState(actors, path);
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
      title: state.admission.title,
      agent: state.admission.agent,
      status: state.status,
      instructions: admission.task.instructions,
      sources: [...new Set(admission.task.input.flatMap((item) => item.sources))],
      hasExecution: !!execution?.session || state.admission.agent === "internal",
      messages,
      result: state.status === "completed" ? text : undefined,
      error: state.status === "completed" ? undefined : text,
      requests: (yield* approvalEntries(actors))
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
  }).pipe(Effect.withSpan("Task.inspect"));

/** Resolve frozen admission evidence and completed results from Pi. */
export const taskCapture = (messages: AgentConversations["Service"]): CapturePolicy => ({
  matches: (path) => /^\/tasks\/[^/]+$/.test(path),
  capture: (record) => ({
    sessionId: `${record.path}:text:${createHash("sha256").update(record.text).digest("hex")}`,
    records: Effect.gen(function* () {
      const entries = yield* messages.read(record.path).pipe(Effect.orDie);
      const entry = entries.find((entry) => entry.kind === "task.admission");
      if (!entry) return undefined;
      const admission = Schema.decodeUnknownSync(TaskDeliveryInput)(entry.data);
      const publicState = Schema.decodeUnknownOption(
        Schema.fromJsonString(
          Schema.Struct({
            status: TaskSnapshot.fields.status,
            outcomeEntryId: Schema.optional(Schema.Int),
          }),
        ),
      )(record.text);
      const resultEntry =
        Option.isSome(publicState) && publicState.value.outcomeEntryId !== undefined
          ? entries.find(
              (entry) =>
                entry.kind === "task.result" && entry.id === publicState.value.outcomeEntryId,
            )
          : undefined;
      const result = resultEntry
        ? Schema.decodeUnknownSync(TaskOutcome)(resultEntry.data)
        : undefined;
      const resultRecord = result
        ? {
            ...record,
            description: `${record.description} result`,
            text: result.text,
          }
        : undefined;
      if (!admission.evidence && !resultRecord) return undefined;
      return [
        record,
        ...(admission.evidence ? [admission.evidence] : []),
        ...(resultRecord ? [resultRecord] : []),
      ];
    }),
  }),
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

export const taskQueryData = Effect.fn("Task.queryData")(function* (
  path: string,
  state: TaskSnapshot,
  detail: boolean,
) {
  const conversations = yield* AgentConversations;
  const summary = {
    path: path,
    title: state.admission.title,
    status: state.status,
    goal: state.admission.replyTo,
    agent: state.admission.agent,
  };
  if (!detail) return publicJson(summary);
  const admission = yield* conversations
    .get(path, state.inputs[0]!.entryId)
    .pipe(
      Effect.mapError(
        () => new ContextQueryError({ kind: "unavailable", message: "Task evidence unavailable" }),
      ),
    );
  const input = yield* Schema.decodeUnknownEffect(TaskDeliveryInput)(admission.data).pipe(
    Effect.orDie,
  );
  const outcome =
    state.outcomeEntryId === undefined
      ? undefined
      : yield* conversations.get(path, state.outcomeEntryId).pipe(
          Effect.mapError(
            () =>
              new ContextQueryError({
                kind: "unavailable",
                message: "Task result unavailable",
              }),
          ),
        );
  const result = outcome
    ? yield* Schema.decodeUnknownEffect(TaskOutcome)(outcome.data).pipe(Effect.orDie)
    : undefined;
  return publicJson({
    ...summary,
    instructions: input.task.instructions,
    result: result?.text,
  });
});
