import { type ActorContext, type ActorRef } from "@aster/actor";
import { ApplicationError, TaskMessage, type CommandReceipt } from "@aster/api-contracts";
import { Effect, Match, Schema } from "effect";
import type { GoalCommand, GoalCommandReply } from "../goals/protocol.js";
import { startTask } from "./commands.js";
import { delegateInput } from "./admission.js";
/** The caller owns durable retry: Signal occurrence or Pi tool identity. Targets acknowledge commits. */
export const deliverTask = Effect.fn("Task.deliver")(function* <C, R>(
  actor: ActorContext<C, R>,
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
    Match.tag("Delegate", (task) => startTask(actor, delegateInput(input, task))),
    Match.exhaustive,
  );
});
