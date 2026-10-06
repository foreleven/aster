import { createHash } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import { AgentConversations } from "@aster/agent";
import { ApplicationError } from "@aster/api-contracts";
import { Context, Effect, Layer, Match, Option, Schema } from "effect";
import { ContextRegistry } from "../../context/registry.js";
import { approvalEntries } from "../../approvals/actor.js";
import { ExternalAgents } from "../execution/contracts.js";
import { TaskInput, TaskOutcome, StoredTaskInput, type TaskWork } from "./snapshot.js";
import { makeTaskStore } from "./store.js";
import { taskPathFor, sourceTask, delegateInput } from "./admission.js";

const makeTaskState = Effect.fn("TaskState.make")(function* (path: string) {
  const store = yield* makeTaskStore(path);
  const messages = yield* AgentConversations;
  const registry = yield* ContextRegistry;
  const agents = yield* ExternalAgents;
  const conflict = (message: string) => new ApplicationError({ kind: "conflict", message });
  const applyOutcome = Effect.fnUntraced(function* (outcome: TaskOutcome, entryId: number) {
    const state = yield* store.read;
    if (state.roundId !== outcome.roundId) return false;
    const inputs = state.inputs.map((input) =>
      outcome.covered.includes(input.requestId)
        ? { ...input, status: "completed" as const }
        : input,
    );
    const pending = inputs.some((input) => input.status === "pending");
    yield* store.save({
      inputs,
      status: outcome.status === "completed" && pending ? "ready" : outcome.status,
      outcomeEntryId: entryId,
      waiting: outcome.requests,
    });
    return true;
  });
  const settle = Effect.fn("TaskState.settle")(function* (outcome: TaskOutcome) {
    const state = yield* store.read;
    if (state.roundId !== outcome.roundId) return false;
    if (outcome.covered.some((id) => !state.inputs.some((input) => input.requestId === id)))
      return yield* Effect.die(new Error("Executor settled an unadmitted input"));
    const identity = createHash("sha256").update(JSON.stringify(outcome)).digest("hex");
    const entry = yield* messages
      .append(path, `outcome:${identity}`, "task.result", outcome)
      .pipe(Effect.orDie);
    return yield* applyOutcome(outcome, entry.id);
  });
  // Pi admission/result handoffs may survive a failed snapshot commit.
  if (yield* store.exists) {
    const entries = yield* messages.read(path).pipe(Effect.orDie);
    for (const entry of entries) {
      if (entry.kind === "task.input") {
        const state = yield* store.read;
        if (state.inputs.some((input) => input.requestId === entry.requestId)) continue;
        const saved = Schema.decodeUnknownSync(StoredTaskInput)(entry.data);
        yield* store.save({
          inputs: [
            ...state.inputs,
            {
              requestId: entry.requestId,
              entryId: entry.id,
              receipt: saved.receipt,
              status: "pending",
            },
          ],
          ...(["completed", "failed", "uncertain", "waiting_input"].includes(state.status)
            ? { status: "ready" as const }
            : {}),
          ...(saved.input._tag === "Message" && ["completed", "failed"].includes(state.status)
            ? { roundId: undefined }
            : {}),
        });
      }
      if (entry.kind === "task.result" && entry.id > ((yield* store.read).outcomeEntryId ?? -1))
        yield* applyOutcome(Schema.decodeUnknownSync(TaskOutcome)(entry.data), entry.id);
    }
  }
  return {
    path,
    snapshot: store.read,
    exists: store.exists,
    accept: Effect.fn("TaskState.accept")(function* (raw: TaskInput) {
      const input = yield* Schema.decodeUnknownEffect(TaskInput)(raw).pipe(
        Effect.mapError(
          () => new ApplicationError({ kind: "invalid-input", message: "Invalid Task input" }),
        ),
      );
      const id = input._tag === "Answer" ? input.requestId : input.input.requestId;
      if (input._tag === "Initial") {
        const value = input.input;
        if (value.target !== path || path !== taskPathFor(value.source, id))
          return yield* conflict("Task identity mismatch");
        if (yield* store.exists) {
          yield* messages
            .append(path, id, "task.admission", value)
            .pipe(Effect.mapError((error) => conflict(error.message)));
          return { receipt: (yield* store.read).inputs[0]!.receipt, replayed: true };
        }
        const source = yield* sourceTask(registry, value.source, id);
        if (
          source &&
          (source.task._tag === "Goal" ||
            !isDeepStrictEqual(delegateInput(source, source.task), value))
        )
          return yield* conflict("Task differs from its frozen Signal occurrence");
        if (value.agent !== "internal" && !agents[value.agent])
          return yield* new ApplicationError({
            kind: "invalid-input",
            message: "Task executor is not configured",
          });
        if (
          (registry.get(value.replyTo)?.state as { status?: string } | undefined)?.status !==
          "active"
        )
          return yield* conflict("Reply Goal is missing or ended");
        const entry = yield* messages.append(path, id, "task.admission", value).pipe(Effect.orDie);
        const receipt = { requestId: id, revision: 1 };
        const { source: owner, replyTo, agent, causal } = value;
        yield* store.commit(
          {
            admission: { source: owner, replyTo, agent, causal },
            status: "ready",
            inputs: [{ requestId: id, entryId: entry.id, receipt, status: "pending" }],
          },
          0,
        );
        return { receipt, replayed: false };
      }
      const state = yield* store.read;
      const previous = state.inputs.find((item) => item.requestId === id);
      if (previous) {
        const entry = yield* messages.get(path, previous.entryId).pipe(Effect.orDie);
        if (
          entry.kind !== "task.input" ||
          !isDeepStrictEqual(Schema.decodeUnknownSync(StoredTaskInput)(entry.data).input, input)
        )
          return yield* conflict("Input identity belongs to another payload");
        return { receipt: previous.receipt, replayed: true };
      }
      yield* Match.value(input).pipe(
        Match.tag("Message", ({ input }) =>
          Effect.gen(function* () {
            if (
              input.target !== path ||
              input.source !== state.admission.replyTo ||
              (registry.get(input.source)?.state as { status?: string } | undefined)?.status !==
                "active" ||
              ["cancelled", "uncertain"].includes(state.status)
            )
              return yield* conflict("Task cannot accept this follow-up");
          }),
        ),
        Match.tag("Answer", ({ requestId, response }) =>
          Effect.gen(function* () {
            const entry = approvalEntries(registry).find((item) => item.id === requestId);
            if (
              !entry ||
              entry.target !== `/user${path}` ||
              !["resolved", "acknowledged"].includes(entry.status) ||
              !isDeepStrictEqual(entry.response, response) ||
              !state.waiting?.some((item) => item.id === requestId)
            )
              return yield* conflict("Answer has no matching committed decision");
          }),
        ),
        Match.tag("Check", "Retry", ({ input, _tag }) =>
          Effect.gen(function* () {
            if (
              input.target !== path ||
              input.expectedRevision !== (yield* store.current)!.revision ||
              !["failed", "uncertain"].includes(state.status) ||
              (_tag === "Retry" && state.status !== "failed")
            )
              return yield* conflict("Task changed or requires reconciliation before retry");
          }),
        ),
        Match.exhaustive,
      );
      const receipt = { requestId: id, revision: ((yield* store.current)!.revision ?? 0) + 1 };
      const entry = yield* messages
        .append(path, id, "task.input", { input, receipt })
        .pipe(Effect.mapError((error) => conflict(error.message)));
      yield* store.save({
        inputs: [...state.inputs, { requestId: id, entryId: entry.id, receipt, status: "pending" }],
        ...(["completed", "failed", "uncertain", "waiting_input"].includes(state.status)
          ? { status: "ready" as const }
          : {}),
        ...(input._tag === "Message" && ["completed", "failed"].includes(state.status)
          ? { roundId: undefined }
          : {}),
      });
      return { receipt, replayed: false };
    }),
    start: Effect.gen(function* () {
      const state = yield* store.read;
      if (!["ready", "running", "waiting_input"].includes(state.status))
        return Option.none<TaskWork>();
      const inputs = state.inputs.filter((input) => input.status === "pending");
      if (!inputs.length && state.status !== "running") return Option.none<TaskWork>();
      const roundId = state.roundId ?? inputs[0]!.requestId;
      yield* store.save({ status: "running", roundId });
      return Option.some({ roundId, admissionEntryId: state.inputs[0]!.entryId, inputs });
    }),
    settle,
    cancel: Effect.fn("TaskState.cancel")(function* (reason: string) {
      const state = yield* store.read;
      if (
        state.status === "running" ||
        state.status === "uncertain" ||
        (state.status === "waiting_input" &&
          !state.waiting?.some((request) => request.kind === "confirmation"))
      )
        return false;
      if (["completed", "cancelled"].includes(state.status)) return true;
      const roundId = state.roundId ?? state.inputs[0]!.requestId;
      yield* store.save({ roundId });
      yield* settle({
        roundId,
        status: "cancelled",
        text: reason,
        covered: state.inputs.map((input) => input.requestId),
      });
      return true;
    }),
  };
});

/** Mailbox-only business operations. Executor progress is owned by TaskExecution. */
export class TaskState extends Context.Service<
  TaskState,
  Effect.Success<ReturnType<typeof makeTaskState>>
>()("tasks/State") {
  static readonly layer = (path: string) => Layer.effect(TaskState, makeTaskState(path));
}
