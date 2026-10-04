import type { GoalSettings } from "../config/settings.js";
import type { GoalHistoryStore } from "./history.js";
import type { TaskExecutionServices } from "../tasks/execution.js";
import { Effect, Schema } from "effect";
import type { ActorContext, ActorRef } from "@aster/actor";
import { childActorName, spawnContextChild } from "../context/actor.js";
import type { ContextRegistry } from "../context/registry.js";
import type { GoalDefinition } from "../config/schema.js";
import { SignalRunActor, type RunCommand } from "../tasks/run.js";
import { RunState } from "../tasks/run-state.js";
import type { GoalSignals } from "./signal-coordination.js";
import type { GoalMailbox } from "./actors.js";
import type { goalWorkingState } from "./working-state.js";
import { goalOutputCause } from "./state.js";
import { planTaskChanges } from "./task-proposals.js";
import { activeExecution, startedExecution, type GoalTask, type GoalTaskChange } from "./tasks.js";

export type GoalActorContext = ActorContext<
  GoalMailbox,
  GoalSignals | GoalSettings | GoalHistoryStore | TaskExecutionServices | ContextRegistry
>;

/** Called only by the Goal mailbox. Persist a proposal before creating its Run or revoking approval. */
export const goalTaskExecution = (
  registry: ContextRegistry["Service"],
  working: ReturnType<typeof goalWorkingState>,
  goal: () => GoalDefinition,
  path: () => string,
) => {
  const { state, current } = working;
  const active = () => state().status === "active";
  const taskById = (id: string) => state().tasks.find((task) => task.id === id);
  const runState = (task: GoalTask) => {
    const record = task.execution && registry.get(task.execution.runPath);
    return record ? Schema.decodeUnknownSync(RunState)(record.state) : undefined;
  };
  const cancelPending = Effect.fn("Goal.cancelPending")(function* (context: GoalActorContext) {
    for (const task of state().tasks) {
      if (
        !task.execution ||
        !activeExecution(runState(task)?.status) ||
        startedExecution(runState(task)?.status)
      )
        continue;
      const child = yield* context.child(
        childActorName(`runs/${task.execution.runPath.split("/").at(-1)!}`),
      );
      if (child)
        yield* (child as ActorRef<RunCommand>).tell({
          _tag: "Cancel",
          reason: "Goal ended; revoke execution that has not started",
        });
    }
  });
  const recover = Effect.fn("Goal.recoverRuns")(function* (
    context: GoalActorContext,
    resumeExisting = true,
  ) {
    for (const record of Object.values(registry.snapshot())) {
      if (!record.path.startsWith(`${path()}/runs/`)) continue;
      const relative = `runs/${record.path.split("/").at(-1)!}`;
      const existing = (yield* context.child(childActorName(relative))) as
        ActorRef<RunCommand> | undefined;
      const ref =
        existing ??
        (yield* spawnContextChild(context, relative, SignalRunActor).pipe(Effect.orDie));
      if (resumeExisting || !existing)
        yield* ref.tell({ _tag: "Resume", path: record.path, subscriber: context.self });
      const run = Schema.decodeUnknownSync(RunState)(record.state);
      const task = run.goalTask && taskById(run.goalTask.taskId);
      if (
        run.goalTask &&
        activeExecution(run.status) &&
        !startedExecution(run.status) &&
        (!active() || !task || task.status !== "open" || task.revision !== run.goalTask.revision)
      )
        yield* ref.tell({
          _tag: "Cancel",
          reason: "Goal or Task changed; prior confirmation is invalid",
        });
    }
    // Recover a proposal committed immediately before its Run was created.
    for (const task of state().tasks) {
      if (
        !task.execution ||
        task.execution.status !== "preparing" ||
        task.execution.revision !== task.revision ||
        registry.get(task.execution.runPath) ||
        task.status !== "open" ||
        !active()
      )
        continue;
      const relative = `runs/${task.execution.runPath.split("/").at(-1)!}`;
      const ref =
        ((yield* context.child(childActorName(relative))) as ActorRef<RunCommand> | undefined) ??
        (yield* spawnContextChild(context, relative, SignalRunActor).pipe(Effect.orDie));
      yield* ref.tell({
        _tag: "Initialize",
        causal: task.execution.causal,
        path: task.execution.runPath,
        definition: {
          slug: `${goal().slug}--${task.id}`,
          when: "Restore execution awaiting preparation",
          task: task.instructions,
          agent: "doubao-delegate",
          mode: "confirm",
        },
        sourceContext: current(),
        subscriber: context.self,
        goalTask: {
          goalPath: path(),
          taskId: task.id,
          revision: task.revision,
          evaluationId: task.execution?.evaluationId,
        },
      });
    }
  });
  const planChanges = (
    changes: readonly GoalTaskChange[],
    evaluationId: string,
    at: string,
    completed: boolean,
  ) =>
    planTaskChanges({
      tasks: state().tasks,
      changes,
      evaluationId,
      at,
      completed,
      goalPath: path(),
      causal: goalOutputCause(state()),
      execution: (task) => {
        const run = runState(task);
        return run && { status: run.status, revision: run.goalTask?.revision };
      },
    });
  return { cancelPending, recover, planChanges };
};
