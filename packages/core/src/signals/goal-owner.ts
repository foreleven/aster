import { ApplicationError, type CommandReceipt } from "@aster/api-contracts";
import { Context, Effect, Layer } from "effect";
import { SignalCommands } from "./commands.js";
import type { GoalSignalInput } from "./goal-command.js";
import type { SignalCommandReply } from "./actors.js";
export class GoalSignals extends Context.Service<
  GoalSignals,
  {
    readonly applySignal: (
      input: GoalSignalInput,
    ) => Effect.Effect<CommandReceipt, ApplicationError>;
    readonly deactivate: (goal: string) => Effect.Effect<void, ApplicationError>;
  }
>()("goals/Signals") {
  static readonly layer = Layer.effect(
    GoalSignals,
    Effect.gen(function* () {
      const root = yield* SignalCommands;
      const unavailable = () =>
        new ApplicationError({
          kind: "unavailable",
          message: "Signal acknowledgement missing; retain the original request identity",
        });
      return {
        applySignal: (input) =>
          root
            .ask<SignalCommandReply>((replyTo) => ({ _tag: "ApplyGoalCommand", input, replyTo }))
            .pipe(
              Effect.mapError(unavailable),
              Effect.flatMap((reply) =>
                reply._tag === "Accepted"
                  ? Effect.succeed(reply.receipt)
                  : Effect.fail(reply.error),
              ),
            ),
        deactivate: (goal) =>
          root
            .ask<void>((replyTo) => ({ _tag: "Deactivate", goal, replyTo }))
            .pipe(Effect.mapError(unavailable)),
      };
    }),
  );
}
