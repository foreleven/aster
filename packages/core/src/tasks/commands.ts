import type { ActorContext, ActorRef } from "@aster/actor";
import { ApplicationError, type TaskDeliveryInput } from "@aster/api-contracts";
import { Effect } from "effect";
import type { RunRootCommand } from "./root.js";
import type { RunAdmissionReply, RunCommand } from "./run.js";
import type { ContextRegistry } from "../context/registry.js";
import { runActorPath } from "./address.js";

/** Submission creates a durable Run. Its lifetime is independent of the calling conversation. */
export const startTask = Effect.fn("Tasks.start")(function* <C, R>(
  actor: ActorContext<C, R>,
  input: TaskDeliveryInput,
) {
  const root = yield* actor
    .select("/user/runs")
    .resolve()
    .pipe(
      Effect.mapError(
        () => new ApplicationError({ kind: "unavailable", message: "Task owner unavailable" }),
      ),
    );
  const reply = yield* (root as ActorRef<RunRootCommand>)
    .ask<RunAdmissionReply>((replyTo) => ({
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
  return { ...reply.receipt, runPath: input.target };
});

/** Ending a Goal revokes unstarted work. Submitted external work retains its execution owner. */
export const cancelGoalTasks = Effect.fn("Tasks.cancelGoal")(function* <C, R>(
  actor: ActorContext<C, R>,
  registry: ContextRegistry["Service"],
  source: string,
) {
  for (const record of Object.values(registry.snapshot())) {
    const state = record.state as { admission?: { input: { replyTo: string } }; status?: string };
    if (
      state.admission?.input.replyTo !== source ||
      !["ready", "awaiting-confirmation"].includes(state.status ?? "")
    )
      continue;
    const ref = yield* actor.select(runActorPath(record.path)).resolve().pipe(Effect.option);
    if (ref._tag === "Some")
      yield* (ref.value as ActorRef<RunCommand>).tell({
        _tag: "Cancel",
        reason: "Goal ended before execution began",
      });
  }
});

/** Goal startup reattaches feedback after both peer owners exist, including already-finished Runs. */
export const attachGoalTasks = Effect.fn("Tasks.attachGoal")(function* <R>(
  actor: ActorContext<import("../goals/actors.js").GoalMailbox, R>,
  registry: ContextRegistry["Service"],
  source: string,
) {
  for (const record of Object.values(registry.snapshot())) {
    const state = record.state as { admission?: { input: { replyTo: string } } };
    if (state.admission?.input.replyTo !== source) continue;
    const ref = yield* actor.select(runActorPath(record.path)).resolve().pipe(Effect.option);
    if (ref._tag === "Some")
      yield* (ref.value as ActorRef<RunCommand>).tell({
        _tag: "Resume",
        path: record.path,
      });
  }
});
