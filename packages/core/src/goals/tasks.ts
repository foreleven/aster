import { SignalSchedule } from "../config/schema.js";
import { CausalChain } from "@aster/api-contracts";
import { Data, Match, Result, Schema } from "effect";

export const GoalTask = Schema.Struct({
  id: Schema.String,
  title: Schema.String,
  instructions: Schema.String,
  status: Schema.Literals(["open", "completed", "deleted"]),
  revision: Schema.Number,
  evidence: Schema.Array(Schema.String),
  createdAt: Schema.String,
  updatedAt: Schema.String,
  execution: Schema.optional(
    Schema.Struct({
      runPath: Schema.String,
      evaluationId: Schema.optional(Schema.String),
      status: Schema.String,
      causal: Schema.optional(CausalChain),
      revision: Schema.optional(Schema.Number),
    }),
  ),
  result: Schema.optional(Schema.String),
});
export type GoalTask = typeof GoalTask.Type;
export const GoalTaskChange = Schema.Union([
  Schema.Struct({
    operation: Schema.Literal("task_create"),
    id: Schema.String,
    title: Schema.String,
    instructions: Schema.String,
    evidence: Schema.optional(Schema.Array(Schema.String)),
  }),
  Schema.Struct({
    operation: Schema.Literal("task_update"),
    id: Schema.String,
    revision: Schema.Number,
    title: Schema.optional(Schema.String),
    instructions: Schema.optional(Schema.String),
    status: Schema.optional(Schema.Literals(["open", "completed"])),
    evidence: Schema.optional(Schema.Array(Schema.String)),
  }),
  Schema.Struct({
    operation: Schema.Literal("task_delete"),
    id: Schema.String,
    revision: Schema.Number,
  }),
  Schema.Struct({
    operation: Schema.Literal("task_execute"),
    id: Schema.String,
    revision: Schema.Number,
  }),
]);
export type GoalTaskChange = typeof GoalTaskChange.Type;
export const TaskToolRequest = Schema.Union([
  Schema.Struct({ operation: Schema.Literal("task_list") }),
  Schema.Struct({ operation: Schema.Literal("task_get"), id: Schema.String }),
  GoalTaskChange,
]);
export type TaskToolRequest = typeof TaskToolRequest.Type;
const SignalPatch = Schema.Struct({
  taskId: Schema.optional(Schema.NullOr(Schema.String)),
  when: Schema.optional(Schema.String),
  task: Schema.optional(Schema.String),
  notBefore: Schema.optional(Schema.NullOr(Schema.String)),
  schedule: Schema.optional(Schema.NullOr(SignalSchedule)),
});
export const GoalSignalChange = Schema.Union([
  Schema.Struct({
    operation: Schema.Literal("signal_create"),
    id: Schema.String,
    definition: SignalPatch,
  }),
  Schema.Struct({
    operation: Schema.Literal("signal_update"),
    id: Schema.String,
    revision: Schema.Number,
    definition: SignalPatch,
  }),
  Schema.Struct({
    operation: Schema.Literal("signal_delete"),
    id: Schema.String,
    revision: Schema.Number,
  }),
]);
export type GoalSignalChange = typeof GoalSignalChange.Type;
export const SignalToolRequest = Schema.Union([
  Schema.Struct({ operation: Schema.Literal("signal_list") }),
  Schema.Struct({ operation: Schema.Literal("signal_get"), id: Schema.String }),
  GoalSignalChange,
]);
export type SignalToolRequest = typeof SignalToolRequest.Type;
export const GoalToolRequest = Schema.Union([TaskToolRequest, SignalToolRequest]);
export type GoalToolRequest = typeof GoalToolRequest.Type;
export const isSignalToolRequest = Schema.is(SignalToolRequest);
export const activeExecution = (status?: string) =>
  !!status &&
  [
    "preparing",
    "checking",
    "awaiting-confirmation",
    "ready",
    "submitting",
    "running",
    "waiting_input",
    "uncertain",
  ].includes(status);
export const startedExecution = (status?: string) =>
  !!status && ["submitting", "running", "waiting_input", "uncertain"].includes(status);

export class GoalToolError extends Data.TaggedError("GoalToolError")<{
  readonly message: string;
  /** Missing acknowledgement is not proof that the owner rejected a mutation. */
  readonly outcome?: "unknown";
}> {}

export type TaskDecision =
  | { readonly _tag: "Read"; readonly value: unknown }
  | {
      readonly _tag: "Save";
      readonly tasks: readonly GoalTask[];
      readonly value: GoalTask;
      readonly cancelRun?: string;
    }
  | {
      readonly _tag: "Execute";
      readonly tasks: readonly GoalTask[];
      readonly task: GoalTask;
      readonly runPath: string;
    };

/** Pure decision only. The Goal mailbox commits tasks before sending cancellation or spawning a Run. */
export const decideTaskOperation = (
  tasks: readonly GoalTask[],
  request: TaskToolRequest,
  options: {
    readonly at: string;
    readonly evaluationId?: string;
    readonly runPath?: string;
    readonly causal?: CausalChain;
    readonly execution?: { readonly status?: string; readonly revision?: number };
  },
): Result.Result<TaskDecision, GoalToolError> => {
  const fail = (message: string) => Result.fail(new GoalToolError({ message }));
  const read = (value: unknown): Result.Result<TaskDecision, GoalToolError> =>
    Result.succeed({ _tag: "Read", value });
  const update = (
    request: Extract<
      TaskToolRequest,
      { operation: "task_update" | "task_delete" | "task_execute" }
    >,
  ): Result.Result<TaskDecision, GoalToolError> => {
    const task = tasks.find((item) => item.id === request.id);
    if (!task || task.status === "deleted") return fail("Task not found or deleted");
    if (request.revision !== task.revision)
      return fail("Task revision changed; read the task again");
    const status = options.execution?.status ?? task.execution?.status;
    const save = (value: GoalTask): Result.Result<TaskDecision, GoalToolError> => {
      const updated: GoalTask = {
        ...value,
        revision: value.revision + 1,
        updatedAt: options.at,
        // Preserve the proposal revision when task content moves forward.
        ...(value.execution
          ? {
              execution: {
                ...value.execution,
                revision: value.execution.revision ?? value.revision,
              },
            }
          : {}),
      };
      return Result.succeed({
        _tag: "Save",
        tasks: tasks.map((item) => (item.id === updated.id ? updated : item)),
        value: updated,
        ...(value.execution && !startedExecution(status)
          ? { cancelRun: value.execution.runPath }
          : {}),
      });
    };
    return Match.value(request).pipe(
      Match.when({ operation: "task_execute" }, () => {
        if (task.status !== "open") return fail("Only open tasks can execute");
        const revision = options.execution?.revision ?? task.execution?.revision;
        // Persisted proposals reserve execution before their Run exists. Never duplicate started work.
        if (
          task.execution &&
          activeExecution(status) &&
          (startedExecution(status) || revision === undefined || revision === task.revision)
        )
          return read({ ...task.execution, reused: true });
        if (!options.runPath) return fail("Execution requires a Run path");
        const execution = {
          runPath: options.runPath,
          ...(options.evaluationId ? { evaluationId: options.evaluationId } : {}),
          ...(options.causal ? { causal: options.causal } : {}),
          status: "preparing",
          revision: task.revision,
        };
        return Result.succeed({
          _tag: "Execute" as const,
          task,
          runPath: options.runPath,
          tasks: tasks.map((item) => (item.id === task.id ? { ...item, execution } : item)),
        });
      }),
      Match.when({ operation: "task_update" }, (patch) => {
        if (
          (patch.title !== undefined && !patch.title.trim()) ||
          (patch.instructions !== undefined && !patch.instructions.trim())
        )
          return fail("Task fields must not be empty");
        return save({
          ...task,
          title: patch.title ?? task.title,
          instructions: patch.instructions ?? task.instructions,
          evidence: patch.evidence ?? task.evidence,
          status: patch.status ?? task.status,
        });
      }),
      Match.when({ operation: "task_delete" }, () => save({ ...task, status: "deleted" })),
      Match.exhaustive,
    );
  };
  return Match.value(request).pipe(
    Match.when({ operation: "task_list" }, () =>
      read(tasks.filter((item) => item.status !== "deleted")),
    ),
    Match.when({ operation: "task_get" }, ({ id }) => {
      const task = tasks.find((item) => item.id === id);
      return task ? read(task) : fail("Task not found");
    }),
    Match.when({ operation: "task_create" }, (request) => {
      if (
        !/^[a-z0-9][a-z0-9-]*$/.test(request.id) ||
        !request.title.trim() ||
        !request.instructions.trim()
      )
        return fail("Task requires a stable slug, title and instructions");
      const existing =
        tasks.find((item) => item.id === request.id) ??
        tasks.find(
          (item) =>
            item.status !== "deleted" &&
            item.title.trim().toLowerCase() === request.title.trim().toLowerCase(),
        );
      if (existing) return read(existing);
      const created: GoalTask = {
        id: request.id,
        title: request.title,
        instructions: request.instructions,
        evidence: request.evidence ?? [],
        status: "open",
        revision: 1,
        createdAt: options.at,
        updatedAt: options.at,
      };
      return Result.succeed({ _tag: "Save" as const, tasks: [...tasks, created], value: created });
    }),
    Match.when({ operation: "task_update" }, update),
    Match.when({ operation: "task_delete" }, update),
    Match.when({ operation: "task_execute" }, update),
    Match.exhaustive,
  );
};
