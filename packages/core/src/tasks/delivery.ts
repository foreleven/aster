import type { WritebackRequest } from "@aster/api-contracts";
import { createHash } from "node:crypto";
import { type ActorContext, type ActorRef } from "@aster/actor";
import {
  ApplicationError,
  GoalPath,
  TaskPath,
  type TaskDeliveryInput,
  TaskMessage,
  CommandReceipt,
} from "@aster/api-contracts";
import { Effect, Schedule, Schema, Match } from "effect";
import { type TasksRootCommand } from "./root.js";
import { type TaskAdmissionReply, type TaskCommand } from "./protocol.js";
import { type ContextRegistry } from "../context/registry.js";
import { TaskSnapshot, type TaskOutcome } from "./state/snapshot.js";
import {
  type GoalMailbox,
  type GoalTaskReply,
  type GoalCommand,
  type GoalCommandReply,
} from "../goals/protocol.js";
import { delegateInput, taskPathFor } from "./state/admission.js";

/** Submission creates a durable Task. Its lifetime is independent of the calling conversation. */
export const startTask = Effect.fn("Tasks.start")(function* <C, R>(
  actor: Pick<ActorContext<C, R>, "select">,
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
  // Admission commits first. Goal attachment is idempotent and restored from Task metadata after interruption.
  const unavailable = () =>
    new ApplicationError({
      kind: "unavailable",
      message:
        "Task accepted but Goal attachment is unconfirmed; retain the original request identity",
    });
  const goals = new Set([input.replyTo]);
  if (Schema.is(GoalPath)(input.source)) goals.add(input.source);
  for (const goal of goals) {
    const target = yield* actor.select(`/user${goal}`).resolve().pipe(Effect.mapError(unavailable));
    const attached = yield* (target as ActorRef<GoalMailbox>)
      .ask<GoalTaskReply>((replyTo) => ({ _tag: "AttachTask", taskPath: input.target, replyTo }))
      .pipe(Effect.mapError(unavailable));
    if (attached._tag === "Rejected") return yield* unavailable();
  }
  return { ...reply.receipt, taskPath: input.target };
});

/** Ending a Goal revokes unstarted work. Submitted external work retains its execution owner. */
export const cancelGoalTasks = Effect.fn("Tasks.cancelGoal")(function* <C, R>(
  actor: Pick<ActorContext<C, R>, "select">,
  registry: ContextRegistry["Service"],
  source: string,
) {
  for (const record of Object.values(registry.snapshot())) {
    if (!Schema.is(TaskPath)(record.path)) continue;
    const state = Schema.decodeUnknownSync(TaskSnapshot)(record.state);
    if (state.admission.replyTo !== source || !["ready", "waiting_input"].includes(state.status))
      continue;
    const ref = yield* actor.select(taskActorPath(record.path)).resolve().pipe(Effect.option);
    if (ref._tag === "Some")
      yield* (ref.value as ActorRef<TaskCommand>).tell({
        _tag: "Cancel",
        reason: "Goal ended before execution began",
      });
  }
});

export const followupTask = Effect.fn("Tasks.followUp")(function* <C, R>(
  actor: Pick<ActorContext<C, R>, "select">,
  input: import("@aster/api-contracts").FollowupTaskInput,
) {
  const target = yield* actor
    .select(`/user${input.target}`)
    .resolve()
    .pipe(
      Effect.mapError(() => new ApplicationError({ kind: "not-found", message: "Task not found" })),
    );
  const reply = yield* (target as ActorRef<TaskCommand>)
    .ask<TaskAdmissionReply>((replyTo) => ({ _tag: "Input", input, replyTo }))
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

/** The caller owns durable retry: Signal occurrence or Pi tool identity. Targets acknowledge commits. */
export const deliverTask = Effect.fn("Task.deliver")(function* <C, R>(
  actor: Pick<ActorContext<C, R>, "select">,
  raw: TaskMessage,
): Effect.fn.Return<CommandReceipt, ApplicationError> {
  const input = yield* Schema.decodeUnknownEffect(TaskMessage)(raw).pipe(
    Effect.mapError(
      () => new ApplicationError({ kind: "invalid-input", message: "Invalid Task message" }),
    ),
  );
  return yield* Match.value(input.task).pipe(
    Match.tag("Goal", (task) =>
      Effect.gen(function* () {
        const target = yield* actor
          .select(`/user${task.target}`)
          .resolve()
          .pipe(
            Effect.mapError(
              () =>
                new ApplicationError({
                  kind: "unavailable",
                  message: "Goal destination unavailable",
                }),
            ),
          );
        const reply = yield* (target as ActorRef<GoalCommand>)
          .ask<GoalCommandReply>((replyTo) => ({
            _tag: "SubmitInput",
            requestId: input.requestId,
            input: { _tag: "TaskMessage", delivery: input },
            replyTo,
          }))
          .pipe(
            Effect.mapError(
              () =>
                new ApplicationError({
                  kind: "unavailable",
                  message: "Task receipt missing; retain the original identity",
                }),
            ),
          );
        if (reply._tag === "Rejected") return yield* reply.error;
        return reply.receipt;
      }),
    ),
    Match.tag("Delegate", "Agent", (task) => startTask(actor, delegateInput(input, task))),
    Match.exhaustive,
  );
});

/** Callers validate TaskPath before selecting an execution owner. */
export const taskActorPath = (path: string) => `/user${path}`;

/** Feedback uses a stable identity and retries only missing acknowledgements in the caller scope. */
export const deliverTaskFeedback = Effect.fn("Tasks.feedback")(function* <C, R>(
  owner: Pick<ActorContext<C, R>, "select">,
  path: string,
  saved: TaskSnapshot,
  outcome: TaskOutcome,
) {
  const requestId = createHash("sha256")
    .update(JSON.stringify([path, saved.outcomeEntryId]))
    .digest("hex");
  yield* Effect.gen(function* () {
    const target = yield* owner
      .select(`/user${saved.admission.replyTo}`)
      .resolve()
      .pipe(
        Effect.mapError(
          () =>
            new ApplicationError({
              kind: "unavailable",
              message: "Reply Goal unavailable",
            }),
        ),
      );
    const reply = yield* (target as ActorRef<GoalCommand>)
      .ask<GoalCommandReply>((replyTo) => ({
        _tag: "SubmitInput",
        requestId,
        replyTo,
        input: {
          _tag: "ExecutionFeedback",
          taskPath: path,
          text: outcome.text,
          status: outcome.status,
        },
      }))
      .pipe(
        Effect.mapError(
          () =>
            new ApplicationError({
              kind: "unavailable",
              message: "Feedback acknowledgement missing",
            }),
        ),
      );
    if (reply._tag === "Rejected") return yield* reply.error;
  }).pipe(
    Effect.retry({
      schedule: Schedule.spaced("3 seconds"),
      while: (error) => error.kind === "unavailable",
    }),
  );
});

const Receipts = Schema.Struct({
  receipts: Schema.Array(Schema.Struct({ requestId: Schema.String, receipt: CommandReceipt })),
});
const Inputs = Schema.Struct({
  inputs: Schema.Array(Schema.Struct({ requestId: Schema.String, receipt: CommandReceipt })),
});
/** Read-only reconciliation. Absence is not permission to submit after pause/delete. */
export const taskDeliveryReceipt = (registry: ContextRegistry["Service"], message: TaskMessage) =>
  Effect.sync(() => {
    const target =
      message.task._tag === "Goal"
        ? message.task.target
        : taskPathFor(message.source, message.requestId);
    const record = registry.get(target);
    if (!record) return undefined;
    const entries =
      message.task._tag === "Goal"
        ? Schema.decodeUnknownSync(Receipts)(record.state).receipts
        : Schema.decodeUnknownSync(Inputs)(record.state).inputs;
    return entries.find((entry) => entry.requestId === message.requestId)?.receipt;
  });

/** The Task owns its Pi layout and freezes the transport-independent publication request. */
export const taskPublication = (
  input: import("@aster/api-contracts").TaskDeliveryInput,
  outcome: import("./state/snapshot.js").TaskOutcome,
  at: string,
): WritebackRequest | undefined => {
  if (outcome.status !== "completed" || !input.action || !outcome.text.trim()) return undefined;
  return {
    requestId: createHash("sha256")
      .update(JSON.stringify(["publication", input.target]))
      .digest("hex")
      .slice(0, 48),
    source: input.target,
    taskSource: input.source,
    causationId: input.requestId,
    createdAt: at,
    action: input.action,
    content: outcome.text,
    causal: { rootRequestId: input.causal.rootRequestId, remainingAgentTurns: 0 },
  };
};
