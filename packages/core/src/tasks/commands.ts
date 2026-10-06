import type { ActorContext, ActorRef } from "@aster/actor";
import { ApplicationError, TaskPath, type TaskDeliveryInput } from "@aster/api-contracts";
import { Effect, Schema } from "effect";
import type { TasksRootCommand } from "./root.js";
import type { TaskAdmissionReply, TaskCommand } from "./protocol.js";
import type { ContextRegistry } from "../context/registry.js";
import { taskActorPath } from "./address.js";
import { TaskState } from "./state.js";

/** Submission creates a durable Task. Its lifetime is independent of the calling conversation. */
export const startTask = Effect.fn("Tasks.start")(function* <C, R>(
  actor: ActorContext<C, R>,
  input: TaskDeliveryInput,
) {
  const root = yield* actor
    .select("/user/tasks")
    .resolve()
    .pipe(
      Effect.mapError(
        () => new ApplicationError({ kind: "unavailable", message: "Task owner unavailable" }),
      ),
    );
  const reply = yield* (root as ActorRef<TasksRootCommand>)
    .ask<TaskAdmissionReply>((replyTo) => ({
      _tag: "StartTask",
      input,
      replyTo,
    }))
    .pipe(
      Effect.mapError(
        () =>
          new ApplicationError({
            kind: "unavailable",
            message: "Task receipt unavailable; retain the original request identity",
          }),
      ),
    );
  if (reply._tag === "Rejected") return yield* reply.error;
  return { ...reply.receipt, taskPath: input.target };
});

/** Ending a Goal revokes unstarted work. Submitted external work retains its execution owner. */
export const cancelGoalTasks = Effect.fn("Tasks.cancelGoal")(function* <C, R>(
  actor: ActorContext<C, R>,
  registry: ContextRegistry["Service"],
  source: string,
) {
  for (const record of Object.values(registry.snapshot())) {
    if (!Schema.is(TaskPath)(record.path)) continue;
    const state = Schema.decodeUnknownSync(TaskState)(record.state);
    if (
      state.admission.replyTo !== source ||
      !["ready", "awaiting-confirmation"].includes(state.status)
    )
      continue;
    const ref = yield* actor.select(taskActorPath(record.path)).resolve().pipe(Effect.option);
    if (ref._tag === "Some")
      yield* (ref.value as ActorRef<TaskCommand>).tell({
        _tag: "Cancel",
        reason: "Goal ended before execution began",
      });
  }
});

/** Goal startup reattaches feedback after both peer owners exist, including already-finished Tasks. */
export const attachGoalTasks = Effect.fn("Tasks.attachGoal")(function* <R>(
  actor: ActorContext<import("../goals/actors.js").GoalMailbox, R>,
  registry: ContextRegistry["Service"],
  source: string,
) {
  for (const record of Object.values(registry.snapshot())) {
    if (!Schema.is(TaskPath)(record.path)) continue;
    const state = Schema.decodeUnknownSync(TaskState)(record.state);
    if (state.admission.replyTo !== source) continue;
    const ref = yield* actor.select(taskActorPath(record.path)).resolve().pipe(Effect.option);
    if (ref._tag === "Some")
      yield* (ref.value as ActorRef<TaskCommand>).tell({
        _tag: "Resume",
      });
  }
});

export const followupTask = Effect.fn("Tasks.followUp")(function* <C, R>(
  actor: ActorContext<C, R>,
  input: import("@aster/api-contracts").FollowupTaskInput,
) {
  const target = yield* actor
    .select(`/user${input.target}`)
    .resolve()
    .pipe(
      Effect.mapError(() => new ApplicationError({ kind: "not-found", message: "Task not found" })),
    );
  const reply = yield* (target as ActorRef<TaskCommand>)
    .ask<TaskAdmissionReply>((replyTo) => ({ _tag: "FollowupTask", input, replyTo }))
    .pipe(
      Effect.mapError(
        () =>
          new ApplicationError({
            kind: "unavailable",
            message: "Task receipt missing; reuse the original input identity",
          }),
      ),
    );
  if (reply._tag === "Rejected") return yield* reply.error;
  return reply.receipt;
});
