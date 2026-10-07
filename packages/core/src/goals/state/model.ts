import { TaskSnapshot } from "../../tasks/state/snapshot.js";
import { makeGoalStore } from "./store.js";
import type { GoalSnapshot, StoredGoalInput } from "./snapshot.js";
import { goalInputs } from "./inputs.js";
import { goalAdmission } from "./admission.js";
import { ContextRegistry } from "../../context/registry.js";
import type { GoalDefinition } from "../../config/schema.js";
import { Context, Effect, Layer, Match, Ref, Result, Schema, Semaphore } from "effect";
import { ApplicationError } from "../../operations.js";
import { TaskPath } from "../../tasks/contracts.js";
import { AgentConversations, type AgentError } from "@aster/agent";

/** One instance per Actor incarnation. All mutations, including local tools, share its writer. */
const makeGoalState = Effect.fn("GoalState.make")(function* (
  path: string,
  definition: GoalDefinition,
) {
  const registry = yield* ContextRegistry;
  const messages = yield* AgentConversations;
  const initial: GoalSnapshot = {
    definition,
    status: "active",
    summary: "Ready to begin",
    tasks: [],
    inputs: [],
    receipts: [],
  };
  const working = yield* makeGoalStore(path, initial);
  const { read, current, save } = working;
  const writer = yield* Semaphore.make(1);
  const closed = yield* Ref.make(false);
  yield* Effect.addFinalizer(() => writer.withPermit(Ref.set(closed, true)));
  const mutate = <A, E>(effect: Effect.Effect<A, E>) =>
    writer.withPermit(
      Effect.gen(function* () {
        if (yield* Ref.get(closed))
          return yield* new ApplicationError({
            kind: "conflict",
            message: "Goal state belongs to a retired Actor",
          });
        return yield* effect;
      }),
    );
  const inputs = goalInputs(working, messages);
  const belongsToGoal = (task: TaskSnapshot) =>
    task.admission.source === path || task.admission.replyTo === path;
  const attachTask = Effect.fn("GoalState.attachTask")(function* (taskPath: string) {
    const task = yield* Schema.decodeUnknownEffect(TaskSnapshot)(
      registry.get(taskPath)?.state,
    ).pipe(
      Effect.mapError(() => new ApplicationError({ kind: "not-found", message: "Task not found" })),
    );
    if (!Schema.is(TaskPath)(taskPath) || !belongsToGoal(task))
      return yield* new ApplicationError({
        kind: "conflict",
        message: "Task does not belong to this Goal",
      });
    const state = yield* read;
    if (!state.tasks.includes(taskPath))
      yield* save({ tasks: [...state.tasks, taskPath] }).pipe(Effect.orDie);
  });
  const patchInput = Effect.fnUntraced(function* (id: string, patch: Partial<StoredGoalInput>) {
    const state = yield* read;
    yield* save({
      inputs: state.inputs.map((input) => (input.inputId === id ? { ...input, ...patch } : input)),
    }).pipe(Effect.orDie);
  });
  const reply = (inputId: string, suffix: string, text: string) =>
    messages
      .append(path, `${inputId}:${suffix}`, "goal.reply", { inputId, text })
      .pipe(Effect.orDie, Effect.asVoid);

  const restore = Effect.gen(function* () {
    yield* inputs.recover();
    if (definition.slug !== "personal" && (yield* read).inputs.length === 0)
      yield* inputs.accept({ _tag: "GoalStarted" }, "initial", 4).pipe(Effect.orDie);
    const recovered = yield* read;
    yield* save({
      definition,
      // Task admission is authoritative if its Goal attachment was interrupted.
      tasks: Object.values(registry.snapshot()).flatMap((record) =>
        Schema.is(TaskPath)(record.path) &&
        belongsToGoal(Schema.decodeUnknownSync(TaskSnapshot)(record.state))
          ? [record.path]
          : [],
      ),
      inputs: recovered.inputs.map((input) =>
        recovered.status === "active" && input.status === "unknown"
          ? { ...input, status: "running" }
          : input,
      ),
    }).pipe(Effect.orDie);
  });

  const updateSummary = Effect.fn("GoalState.updateSummary")(function* (summary: string) {
    if ((yield* read).status !== "active" || !summary.trim() || summary.length > 6000)
      return yield* new ApplicationError({
        kind: "conflict",
        message: "Goal is inactive or summary is invalid",
      });
    yield* save({ summary }).pipe(Effect.orDie);
    return { revision: (yield* current).revision! };
  });

  const recordGate = Effect.fn("GoalState.recordGate")(function* (
    inputId: string,
    result: Result.Result<{ relevant: boolean; reason: string }, AgentError>,
  ) {
    if (Result.isFailure(result)) {
      yield* patchInput(inputId, { status: "failed", error: result.failure.message });
    } else {
      const { relevant } = result.success;
      yield* patchInput(inputId, { relevant, ...(relevant ? {} : { status: "ignored" }) });
    }
  });

  // Pi reply commits before the input settles. Replaying the same result reuses its reply identity.
  const settle = Effect.fn("GoalState.settle")(function* (
    inputId: string,
    result: Result.Result<string, AgentError>,
  ) {
    if (Result.isFailure(result)) {
      const error = result.failure;
      yield* reply(
        inputId,
        `reply:${error.outcome}`,
        error.outcome === "failed"
          ? "I couldn't complete this request. Please retry or give me another instruction."
          : "The outcome of this request is uncertain. I need to reconcile it before continuing.",
      );
      yield* patchInput(inputId, {
        status: error.outcome === "failed" ? "failed" : "unknown",
        error: error.message,
      });
      return;
    }
    const input = (yield* read).inputs.find((input) => input.inputId === inputId)!;
    const text =
      result.success.trim() ||
      Match.value(input.kind).pipe(
        Match.when(
          "UserInput",
          () => "I couldn't produce a reply. Please give me another instruction.",
        ),
        Match.when(
          "ExecutionFeedback",
          () =>
            "The task has an update. Please review its result or pending request in Task details.",
        ),
        Match.orElse(() => ""),
      );
    if (text) yield* reply(inputId, "reply", text);
    yield* patchInput(inputId, { status: "completed" });
  });

  const exhaust = Effect.fn("GoalState.exhaust")(function* (input: StoredGoalInput) {
    if (input.kind === "ExecutionFeedback")
      yield* reply(
        input.inputId,
        "budget",
        "The task has an update. I have reached the automatic follow-up limit; please review Task details or send me another instruction.",
      );
    yield* patchInput(input.inputId, {
      status: "ignored",
      error: "Automatic feedback budget exhausted",
    });
  });

  yield* restore;
  const accept = goalAdmission(registry, working, messages);
  return {
    read,
    inspect: Effect.gen(function* () {
      const state = yield* read;
      return { goal: state.definition, state: registry.reader.get(path)!.state };
    }),
    listTasks: Effect.map(read, (state) =>
      state.tasks.flatMap((path) => {
        const task = registry.reader.get(path);
        return task ? [task] : [];
      }),
    ),
    attachTask: (taskPath: string) => mutate(attachTask(taskPath)),
    accept: (...args: Parameters<typeof accept>) => mutate(accept(...args)),
    resolve: inputs.resolve,
    start: (inputId: string) => mutate(patchInput(inputId, { status: "running" })),
    updateSummary: (summary: string) => mutate(updateSummary(summary)),
    recordGate: (...args: Parameters<typeof recordGate>) => mutate(recordGate(...args)),
    settle: (...args: Parameters<typeof settle>) => mutate(settle(...args)),
    exhaust: (input: StoredGoalInput) => mutate(exhaust(input)),
  };
});

/** Actor-local business model; the Layer owns its lifetime, and Pi owns messages. */
export class GoalState extends Context.Service<
  GoalState,
  Effect.Success<ReturnType<typeof makeGoalState>>
>()("goals/State") {
  static readonly layer = (path: string, definition: GoalDefinition) =>
    Layer.effect(GoalState, makeGoalState(path, definition));
}
