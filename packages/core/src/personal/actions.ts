import type { RunRootCommand } from "../tasks/root.js";
import type { RunAdmissionReply } from "../tasks/run.js";
import { ExternalAgents } from "../tasks/model.js";
import type { ActorRef } from "@aster/actor";
import {
  ApplicationError,
  type GoalDeliveryInput,
  type SignalDeliveryInput,
  type CommandReceipt,
  type ApprovalDeliveryInput,
  type ApprovalRequestDeliveryInput,
  type TaskDeliveryInput,
  type ResumeRunDeliveryInput,
} from "@aster/api-contracts";
import type { ApprovalCommand, ApprovalCommandReply } from "../approvals/actor.js";
import { Context, Deferred, Effect, Layer } from "effect";
import type { SignalRootCommand, SignalCommandReply } from "../signals/actors.js";
import type { GoalsRootCommand, GoalDeliveryReply } from "../goals/actors.js";

/** Runtime binds domain owners; the outbox never writes their repositories. */
export class PersonalActions extends Context.Service<
  PersonalActions,
  {
    readonly resumeRun: (
      input: ResumeRunDeliveryInput,
    ) => Effect.Effect<CommandReceipt, ApplicationError>;
    readonly startTask: (
      input: TaskDeliveryInput,
    ) => Effect.Effect<CommandReceipt, ApplicationError>;
    readonly executors: Effect.Effect<readonly string[]>;
    readonly requestApproval: (
      input: ApprovalRequestDeliveryInput,
    ) => Effect.Effect<CommandReceipt, ApplicationError>;
    readonly respondApproval: (
      input: ApprovalDeliveryInput,
    ) => Effect.Effect<CommandReceipt, ApplicationError>;
    readonly sendGoalMessage: (
      input: GoalDeliveryInput,
    ) => Effect.Effect<CommandReceipt, ApplicationError>;
    readonly applySignal: (
      input: SignalDeliveryInput,
    ) => Effect.Effect<CommandReceipt, ApplicationError>;
    readonly bind: (
      goals: ActorRef<GoalsRootCommand> | undefined,
      signals?: ActorRef<SignalRootCommand>,
      approvals?: ActorRef<ApprovalCommand>,
      runs?: ActorRef<RunRootCommand>,
    ) => Effect.Effect<boolean>;
  }
>()("personal/Actions") {
  static readonly unavailable = Layer.succeed(PersonalActions, {
    executors: Effect.succeed([]),
    resumeRun: () =>
      Effect.fail(
        new ApplicationError({ kind: "unavailable", message: "Run commands unavailable" }),
      ),
    startTask: () =>
      Effect.fail(
        new ApplicationError({ kind: "unavailable", message: "Task commands unavailable" }),
      ),
    requestApproval: () =>
      Effect.fail(
        new ApplicationError({ kind: "unavailable", message: "Approval commands unavailable" }),
      ),
    respondApproval: () =>
      Effect.fail(
        new ApplicationError({ kind: "unavailable", message: "Approval commands unavailable" }),
      ),
    bind: () => Effect.succeed(false),
    applySignal: () =>
      Effect.fail(
        new ApplicationError({ kind: "unavailable", message: "Signal commands unavailable" }),
      ),
    sendGoalMessage: () =>
      Effect.fail(
        new ApplicationError({ kind: "unavailable", message: "Goal delivery is unavailable" }),
      ),
  });
  static readonly layer = Layer.effect(
    PersonalActions,
    Effect.gen(function* () {
      const agents = yield* ExternalAgents;
      const ready = yield* Deferred.make<ActorRef<GoalsRootCommand> | undefined>();
      const signalRoot = yield* Deferred.make<ActorRef<SignalRootCommand> | undefined>();
      const runRoot = yield* Deferred.make<ActorRef<RunRootCommand> | undefined>();
      const approvalRoot = yield* Deferred.make<ActorRef<ApprovalCommand> | undefined>();
      return {
        executors: Effect.succeed(Object.keys(agents).sort()),
        bind: (root, signals, approvals, runs) =>
          Deferred.succeed(runRoot, runs).pipe(
            Effect.andThen(Deferred.succeed(approvalRoot, approvals)),
            Effect.andThen(Deferred.succeed(signalRoot, signals)),
            Effect.andThen(Deferred.succeed(ready, root)),
          ),
        resumeRun: Effect.fn("PersonalActions.resumeRun")(function* (input) {
          const root = yield* Deferred.await(runRoot);
          if (!root)
            return yield* new ApplicationError({
              kind: "unavailable",
              message: "Run commands unavailable",
            });
          const reply = yield* root
            .ask<RunAdmissionReply>((replyTo) => ({ _tag: "ResumePersonalRun", input, replyTo }))
            .pipe(
              Effect.mapError(
                () =>
                  new ApplicationError({
                    kind: "unavailable",
                    message: "Run acknowledgement missing; reconcile the same request",
                  }),
              ),
            );
          if (reply._tag === "Rejected") return yield* reply.error;
          if (reply.receipt.requestId !== input.requestId)
            return yield* new ApplicationError({
              kind: "unavailable",
              message: "Run returned another request's receipt",
            });
          return reply.receipt;
        }),
        startTask: Effect.fn("PersonalActions.startTask")(function* (input) {
          const root = yield* Deferred.await(runRoot);
          if (!root)
            return yield* new ApplicationError({
              kind: "unavailable",
              message: "Task commands unavailable",
            });
          const reply = yield* root
            .ask<RunAdmissionReply>((replyTo) => ({ _tag: "StartPersonalTask", input, replyTo }))
            .pipe(
              Effect.mapError(
                () =>
                  new ApplicationError({
                    kind: "unavailable",
                    message: "Task admission acknowledgement missing; acceptance is unknown",
                  }),
              ),
            );
          if (reply._tag === "Rejected") return yield* reply.error;
          if (reply.receipt.requestId !== input.requestId)
            return yield* new ApplicationError({
              kind: "unavailable",
              message: "Run returned a receipt for another Task",
            });
          return reply.receipt;
        }),
        requestApproval: Effect.fn("PersonalActions.requestApproval")(function* (input) {
          const root = yield* Deferred.await(approvalRoot);
          if (!root)
            return yield* new ApplicationError({
              kind: "unavailable",
              message: "Approval commands unavailable",
            });
          const reply = yield* root
            .ask<ApprovalCommandReply>((replyTo) => ({ _tag: "RequestPersonal", input, replyTo }))
            .pipe(
              Effect.mapError(
                () =>
                  new ApplicationError({
                    kind: "unavailable",
                    message: "Approval acknowledgement missing; acceptance is unknown",
                  }),
              ),
            );
          if (reply._tag === "Rejected") return yield* reply.error;
          if (reply.receipt.requestId !== input.requestId)
            return yield* new ApplicationError({
              kind: "unavailable",
              message: "Approval returned a receipt for another command",
            });
          return reply.receipt;
        }),
        respondApproval: Effect.fn("PersonalActions.respondApproval")(function* (input) {
          const root = yield* Deferred.await(approvalRoot);
          if (!root)
            return yield* new ApplicationError({
              kind: "unavailable",
              message: "Approval commands unavailable",
            });
          const reply = yield* root
            .ask<ApprovalCommandReply>((replyTo) => ({ _tag: "RespondPersonal", input, replyTo }))
            .pipe(
              Effect.mapError(
                () =>
                  new ApplicationError({
                    kind: "unavailable",
                    message: "Approval acknowledgement missing; acceptance is unknown",
                  }),
              ),
            );
          if (reply._tag === "Rejected") return yield* reply.error;
          if (reply.receipt.requestId !== input.requestId)
            return yield* new ApplicationError({
              kind: "unavailable",
              message: "Approval returned a receipt for another command",
            });
          return reply.receipt;
        }),
        applySignal: Effect.fn("PersonalActions.applySignal")(function* (input) {
          const root = yield* Deferred.await(signalRoot);
          if (!root)
            return yield* new ApplicationError({
              kind: "unavailable",
              message: "Signal commands unavailable",
            });
          const reply = yield* root
            .ask<SignalCommandReply>((replyTo) => ({
              _tag: "ApplyPersonalCommand",
              input,
              replyTo,
            }))
            .pipe(
              Effect.mapError(
                () =>
                  new ApplicationError({
                    kind: "unavailable",
                    message: "Signal command acknowledgement missing; acceptance is unknown",
                  }),
              ),
            );
          if (reply._tag === "Rejected") return yield* reply.error;
          if (reply.receipt.requestId !== input.requestId)
            return yield* new ApplicationError({
              kind: "unavailable",
              message: "Signal returned a receipt for another command",
            });
          return reply.receipt;
        }),
        sendGoalMessage: Effect.fn("PersonalActions.sendGoalMessage")(function* (input) {
          const root = yield* Deferred.await(ready);
          if (!root)
            return yield* new ApplicationError({
              kind: "not-found",
              message: "No Goals configured",
            });
          const reply = yield* root
            .ask<GoalDeliveryReply>((replyTo) => ({
              _tag: "Route",
              slug: input.target.slice("/goals/".length),
              command: { _tag: "Deliver", input, replyTo },
            }))
            .pipe(
              Effect.mapError(
                () =>
                  new ApplicationError({
                    kind: "unavailable",
                    message: "Goal delivery acknowledgement missing; acceptance is unknown",
                  }),
              ),
            );
          if (reply._tag === "Rejected") return yield* reply.error;
          if (reply.receipt.requestId !== input.requestId)
            return yield* new ApplicationError({
              kind: "unavailable",
              message: "Goal returned a receipt for another delivery",
            });
          return reply.receipt;
        }),
      };
    }),
  );
}
