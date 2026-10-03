import { createHash } from "node:crypto";
import { Effect, Schema } from "effect";
import type { CausalChain } from "@aster/api-contracts";
import { decideTaskOperation, GoalToolError, type GoalTask, GoalTaskChange } from "./tasks.js";

/** Validate the whole ordered proposal without writing or dispatching anything.
 * Stable execution reservations are part of the caller's single Goal commit. */
export const planTaskChanges = Effect.fn("Goal.planTaskChanges")(function* (options: {
  readonly tasks: readonly GoalTask[];
  readonly changes: readonly GoalTaskChange[];
  readonly evaluationId: string;
  readonly goalPath: string;
  readonly at: string;
  readonly causal?: CausalChain;
  readonly completed: boolean;
  readonly execution: (
    task: GoalTask,
  ) => { readonly status?: string; readonly revision?: number } | undefined;
}) {
  let tasks = options.tasks;
  const changes = yield* Schema.decodeUnknownEffect(
    Schema.Array(GoalTaskChange).check(Schema.isMaxLength(32)),
  )(options.changes).pipe(
    Effect.mapError(() => new GoalToolError({ message: "Invalid Task change proposals" })),
  );
  const executions = new Set<string>();
  for (const [index, change] of changes.entries()) {
    if (executions.has(change.id) && change.operation !== "task_execute")
      return yield* new GoalToolError({
        message: "Apply all Task edits before proposing its execution",
      });
    if (options.completed && change.operation === "task_execute")
      return yield* new GoalToolError({
        message: "A completed Goal cannot propose a new execution",
      });
    const task = tasks.find((item) => item.id === change.id);
    const runId = createHash("sha256")
      .update(JSON.stringify([options.evaluationId, index]))
      .digest("hex");
    const decision = yield* Effect.fromResult(
      decideTaskOperation(tasks, change, {
        at: options.at,
        evaluationId: options.evaluationId,
        causal: options.causal,
        runPath: `${options.goalPath}/runs/${runId}`,
        execution: task && options.execution(task),
      }),
    );
    if (decision._tag !== "Read") tasks = decision.tasks;
    if (change.operation === "task_execute") executions.add(change.id);
  }
  return tasks;
});
