import { createHash } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import {
  ApplicationError,
  CommandReceipt,
  TaskDeliveryInput,
  ResumeRunDeliveryInput,
} from "@aster/api-contracts";
import { taskPath, sourceTask, delegateInput } from "./admission.js";
import { transitionRun, type RunTransition } from "./run-transition.js";
import { RunState, terminalRunText } from "./run-state.js";
import { makeRunWriteback, planWriteback, WritebackFinished } from "./writeback.js";
import { Clock, Effect, Match, Layer, Schema, Struct, Schedule } from "effect";
import { ReplyTo, type ActorContext, type ActorRef } from "@aster/actor";
import { ContextActor, contextSpawnOptions, contextPath } from "../context/actor.js";
import { defineContext } from "../context/definition.js";
import { ContextRegistry } from "../context/registry.js";
import {
  DelegationActor,
  DelegationUpdate,
  type ResumeExecutionReply,
} from "../delegation/actor.js";
import { ExternalAgents, taskPrompt, DEFAULT_EXECUTOR_PROMPT } from "./model.js";
import { ApprovalResolved, sendApproval, approvalEntries } from "../approvals/actor.js";
import type { GoalCommand } from "../goals/actors.js";

export const RunAdmissionReply = Schema.Union([
  Schema.TaggedStruct("Accepted", { receipt: CommandReceipt }),
  Schema.TaggedStruct("Rejected", { error: ApplicationError }),
]);
export type RunAdmissionReply = typeof RunAdmissionReply.Type;
export const StartTask = Schema.TaggedStruct("StartTask", {
  input: TaskDeliveryInput,
  replyTo: ReplyTo<RunAdmissionReply>(),
});
export const ResumeRun = Schema.TaggedStruct("ResumeRun", {
  input: ResumeRunDeliveryInput,
  replyTo: ReplyTo<RunAdmissionReply>(),
});
export const RunReady = Schema.TaggedStruct("Ready", { replyTo: ReplyTo<void>() });
export const RunCommand = Schema.Union([
  RunReady,
  WritebackFinished,
  ResumeRun,
  Schema.TaggedStruct("DeliverResumption", {}),
  Schema.TaggedStruct("ResumptionDelivered", {
    requestId: Schema.String,
    result: Schema.Union([
      Schema.TaggedStruct("Success", { value: CommandReceipt }),
      Schema.TaggedStruct("Failure", { error: Schema.instanceOf(ApplicationError) }),
    ]),
  }),
  StartTask,
  Schema.TaggedStruct("FeedbackDelivered", { error: Schema.optional(Schema.String) }),
  Schema.TaggedStruct("Cancel", {
    reason: Schema.String,
    replyTo: Schema.optional(ReplyTo<void>()),
  }),
  DelegationUpdate,
  ApprovalResolved,
  Schema.TaggedStruct("Resume", {
    path: Schema.String,
  }),
]);
export type RunCommand = typeof RunCommand.Type;
export class TaskRunActor extends ContextActor.Service<TaskRunActor, ExternalAgents>()(
  "tasks/RunActor",
  {
    command: RunCommand,
    context: defineContext({
      state: RunState,
      message: Schema.Unknown,
    }),
  },
) {
  static readonly layer = Layer.effect(
    TaskRunActor,
    Effect.gen(function* () {
      const registry = yield* ContextRegistry;
      const agents = yield* ExternalAgents;
      let resumptionInFlight: string | undefined;
      let runPath = "";
      let owner: ActorContext<RunCommand, ExternalAgents | ContextRegistry>;
      const state = () => Schema.decodeUnknownSync(RunState)(registry.get(runPath)!.state);
      const transition = Effect.fn("Run.transition")(function* (change: RunTransition) {
        const current = registry.get(runPath)!;
        const next = transitionRun(state(), change);
        const at = new Date(yield* Clock.currentTimeMillis).toISOString();
        const writeback =
          change.type === "Finished"
            ? planWriteback(runPath, next.state, at)
            : next.state.writeback;
        return yield* registry
          .commit(
            {
              ...current,
              state: {
                ...next.state,
                ...(writeback ? { writeback } : {}),
              },
              messages: [...current.messages, { ...next.event, at }],
            },
            { expectedRevision: current.revision ?? 0 },
          )
          .pipe(Effect.asVoid, Effect.orDie);
      });
      const writeback = yield* makeRunWriteback({ path: () => runPath, state });
      const valid = () => {
        const source = state().admission.input.replyTo;
        if (!source?.startsWith("/goals/")) return true;
        return (
          (registry.get(source)?.state as { status?: string } | undefined)?.status === "active"
        );
      };
      // The Run event is durable; replay on restart and retry the same receipt identity in a scoped fiber.
      // Waiting for a Goal acknowledgement must never block the Run mailbox.
      const notify = (text: string, terminal: boolean) => {
        const current = state();
        const requestId = createHash("sha256")
          .update(JSON.stringify([runPath, current.status, text]))
          .digest("hex");
        return owner.pipeToSelf(
          Effect.gen(function* () {
            const target = yield* owner
              .select(`/user${current.admission.input.replyTo}`)
              .resolve()
              .pipe(
                Effect.mapError(
                  () =>
                    new ApplicationError({
                      kind: "unavailable",
                      message: "Reply Goal is unavailable",
                    }),
                ),
              );
            const reply = yield* (target as ActorRef<GoalCommand>)
              .ask<import("../goals/protocol.js").GoalCommandReply>((replyTo) => ({
                _tag: "SubmitInput",
                requestId,
                replyTo,
                input: {
                  _tag: "ExecutionFeedback",
                  runPath,
                  causal: current.admission.input.causal,
                  text,
                  terminal,
                  status: current.status,
                },
              }))
              .pipe(
                Effect.mapError(
                  () =>
                    new ApplicationError({
                      kind: "unavailable",
                      message: "Goal feedback receipt missing",
                    }),
                ),
              );
            if (reply._tag === "Rejected") return yield* reply.error;
          }).pipe(
            Effect.retry({
              schedule: Schedule.spaced("3 seconds"),
              while: (error) => error.kind === "unavailable",
            }),
          ),
          (result) => ({
            _tag: "FeedbackDelivered",
            error: result._tag === "Failure" ? result.error.message : undefined,
          }),
        );
      };
      const executionTask = () => ({
        ...state().admission.input.task,
        instructions: `${state().executorPrompt}\n\n${state().admission.input.task.instructions}`,
        input: [
          ...state().admission.input.task.input,
          ...(state().admission.input.evidence
            ? [
                {
                  content: JSON.stringify(state().admission.input.evidence),
                  sources: [state().admission.input.evidence!.path],
                },
              ]
            : []),
        ],
      });
      const replayTerminal = Effect.fnUntraced(function* (includeUncertain = false) {
        const text = terminalRunText(state(), includeUncertain);
        if (text === undefined) return false;
        yield* notify(text, true);
        return true;
      });
      const cancel = (
        context: ActorContext<RunCommand, ExternalAgents | ContextRegistry>,
        reason: string,
      ) =>
        Effect.gen(function* () {
          if (
            [
              "submitting",
              "running",
              "waiting_input",
              "uncertain",
              "completed",
              "cancelled",
            ].includes(state().status)
          )
            return;
          yield* transition({ type: "Cancelled", text: reason });
          yield* sendApproval(context, { _tag: "Revoke", id: `${runPath}:confirm` });
          yield* notify(reason, true);
        });
      const launch = (
        context: ActorContext<RunCommand, ExternalAgents | ContextRegistry>,
        recovering = false,
      ) =>
        Effect.gen(function* () {
          const current = state();
          if (!recovering && !valid()) {
            yield* cancel(context, "Task revision is no longer executable");
            return;
          }
          yield* transition({ type: "Delegating" });
          const child =
            ((yield* context.child("delegation")) as
              ActorRef<import("@aster/actor").CommandOf<typeof DelegationActor>> | undefined) ??
            (yield* context
              .spawn(
                "delegation",
                DelegationActor,
                contextSpawnOptions(`/delegations/${runPath.split("/").at(-1)!}`),
              )
              .pipe(Effect.orDie));
          yield* child.tell({
            _tag: "Start",
            recovering,
            replyTo: context.self,
            request: { runPath, task: executionTask(), agent: current.admission.input.agent },
          });
        });
      const requestConfirmation = (
        context: ActorContext<RunCommand, ExternalAgents | ContextRegistry>,
      ) =>
        sendApproval(context, {
          _tag: "Enqueue",
          entry: {
            id: `${runPath}:confirm`,
            target: context.path,
            contextPath: runPath,
            kind: "confirmation",
            status: "pending",
            request: {
              id: `${runPath}:confirm`,
              kind: "approval",
              prompt: taskPrompt(executionTask()),
            },
          },
        });
      const admitTask = Effect.fn("Run.admitTask")(function* (
        raw: TaskDeliveryInput,
        actor: ActorContext<RunCommand, ExternalAgents | ContextRegistry>,
      ) {
        const input = yield* Schema.decodeUnknownEffect(TaskDeliveryInput)(raw).pipe(
          Effect.mapError(
            () => new ApplicationError({ kind: "invalid-input", message: "Invalid Task command" }),
          ),
        );
        const path = contextPath(actor);
        if (input.target !== path || path !== taskPath(input.source, input.requestId))
          return yield* new ApplicationError({
            kind: "invalid-input",
            message: "Task command identity does not match its Run",
          });
        const existing = registry.get(path);
        if (existing) {
          runPath = path;
          const admission = Schema.decodeUnknownSync(RunState)(existing.state).admission;
          if (!isDeepStrictEqual(admission.input, input))
            return yield* new ApplicationError({
              kind: "conflict",
              message: "Run belongs to another Task command",
            });
          return { receipt: admission.receipt, created: false };
        }
        const source = yield* sourceTask(registry, input.source, input.requestId);
        if (
          source &&
          (source.task._tag !== "Delegate" ||
            !isDeepStrictEqual(delegateInput(source, source.task), input))
        )
          return yield* new ApplicationError({
            kind: "conflict",
            message: "Task differs from its committed Signal occurrence",
          });
        if (
          (registry.get(input.replyTo)?.state as { status?: string } | undefined)?.status !==
          "active"
        )
          return yield* new ApplicationError({
            kind: "conflict",
            message: "Reply Goal is missing or ended",
          });
        if (!agents[input.agent])
          return yield* new ApplicationError({
            kind: "invalid-input",
            message: "Task executor is not configured",
          });
        const receipt = { requestId: input.requestId, revision: 1 };
        yield* registry
          .commit(
            {
              path,
              description: `Task: ${input.task.instructions}`,
              state: {
                admission: { input, receipt },
                executorPrompt: agents[input.agent]!.executorPrompt ?? DEFAULT_EXECUTOR_PROMPT,
                status: "awaiting-confirmation",
              },
              messages: [
                {
                  type: "Triggered",
                  at: new Date(yield* Clock.currentTimeMillis).toISOString(),
                  sourcePath: input.source,
                  task: input.task.instructions,
                  agent: input.agent,
                  mode: "confirm",
                  sourceContext: input.evidence,
                  requestId: input.requestId,
                  target: path,
                  revision: receipt.revision,
                },
              ],
            },
            { expectedRevision: 0 },
          )
          .pipe(
            Effect.catchTag("ContextConflict", () =>
              Effect.fail(
                new ApplicationError({ kind: "conflict", message: "Task Run already exists" }),
              ),
            ),
            Effect.catchTag("ContextCommitError", Effect.die),
            Effect.catchTag("ContextValidationError", Effect.die),
          );
        runPath = path;
        return { receipt, created: true };
      });
      const admitResumption = Effect.fn("Run.admitResumption")(function* (
        raw: ResumeRunDeliveryInput,
      ) {
        const input = yield* Schema.decodeUnknownEffect(ResumeRunDeliveryInput)(raw).pipe(
          Effect.mapError(
            () =>
              new ApplicationError({ kind: "invalid-input", message: "Invalid Run resumption" }),
          ),
        );
        if (!runPath || input.target !== runPath)
          return yield* new ApplicationError({
            kind: "invalid-input",
            message: "Resumption targets another Run",
          });
        const current = registry.get(runPath)!;
        const saved = state();
        const previous = saved.resumptions?.find(
          (item) => item.input.requestId === input.requestId,
        );
        if (previous) {
          if (!isDeepStrictEqual(previous.input, input))
            return yield* new ApplicationError({
              kind: "conflict",
              message: "Resume request ID belongs to another command",
            });
          return previous.receipt;
        }
        if (!["failed", "uncertain"].includes(saved.status))
          return yield* new ApplicationError({
            kind: "conflict",
            message:
              "Only failed or uncertain executions can be resumed; pending confirmation cannot be bypassed",
          });
        if (!valid())
          return yield* new ApplicationError({
            kind: "conflict",
            message: "Task revision is no longer executable",
          });
        if (saved.resumptions?.some((item) => item.status === "pending"))
          return yield* new ApplicationError({
            kind: "conflict",
            message: "A resumption is already pending; reconcile its original request",
          });
        const receipt = { requestId: input.requestId, revision: (current.revision ?? 0) + 1 };
        yield* registry
          .commit(
            {
              ...current,
              state: {
                ...saved,
                resumptions: [...(saved.resumptions ?? []), { input, receipt, status: "pending" }],
              },
              messages: [
                ...current.messages,
                {
                  type: "ResumeRequested",
                  requestId: input.requestId,
                  at: new Date(yield* Clock.currentTimeMillis).toISOString(),
                },
              ],
            },
            { expectedRevision: input.expectedRevision },
          )
          .pipe(
            Effect.catchTag("ContextConflict", () =>
              Effect.fail(
                new ApplicationError({ kind: "conflict", message: "Run revision changed" }),
              ),
            ),
            Effect.catchTag("ContextCommitError", Effect.die),
            Effect.catchTag("ContextValidationError", Effect.die),
          );
        return receipt;
      });
      const deliverResumption = Effect.fn("Run.deliverResumption")(function* (
        actor: ActorContext<RunCommand, ExternalAgents | ContextRegistry>,
      ) {
        if (resumptionInFlight) return;
        const pending = state().resumptions?.find((item) => item.status === "pending");
        if (!pending) return;
        const saved = state();
        const child =
          ((yield* actor.child("delegation")) as
            ActorRef<import("@aster/actor").CommandOf<typeof DelegationActor>> | undefined) ??
          (yield* actor
            .spawn(
              "delegation",
              DelegationActor,
              contextSpawnOptions(`/delegations/${runPath.split("/").at(-1)!}`, {
                metadata: { resumeOnly: true },
              }),
            )
            .pipe(Effect.orDie));
        yield* child.tell({
          _tag: "Start",
          request: { runPath, task: executionTask(), agent: saved.admission.input.agent },
          replyTo: actor.self,
          resumeOnly: true,
        });
        resumptionInFlight = pending.input.requestId;
        yield* actor.pipeToSelf(
          child
            .ask<ResumeExecutionReply>((replyTo) => ({
              _tag: "ResumeExecution",
              input: pending.input,
              replyTo,
            }))
            .pipe(
              Effect.mapError(
                () =>
                  new ApplicationError({
                    kind: "unavailable",
                    message: "Resume receipt missing; reconcile the original request",
                  }),
              ),
              Effect.flatMap((reply) =>
                reply._tag === "Accepted"
                  ? Effect.succeed(reply.receipt)
                  : Effect.fail(reply.error),
              ),
            ),
          (result) => ({ _tag: "ResumptionDelivered", requestId: pending.input.requestId, result }),
        );
      });
      return TaskRunActor.of({
        started: (context) =>
          Effect.gen(function* () {
            owner = context;
            if (registry.get(contextPath(context)))
              yield* context.self.tell({ _tag: "Resume", path: contextPath(context) });
          }),
        receive: (command, context) =>
          Match.value(command).pipe(
            Match.tag("Ready", ({ replyTo }) => replyTo.tell(undefined)),
            Match.tag("FeedbackDelivered", ({ error }) =>
              error ? Effect.logWarning(error) : Effect.void,
            ),
            Match.tag("ResumeRun", ({ input, replyTo }) =>
              Effect.gen(function* () {
                const result = yield* admitResumption(input).pipe(Effect.result);
                if (result._tag === "Failure")
                  return yield* replyTo.tell({ _tag: "Rejected", error: result.failure });
                yield* replyTo.tell({ _tag: "Accepted", receipt: result.success });
                yield* context.self.tell({ _tag: "DeliverResumption" });
              }),
            ),
            Match.tag("DeliverResumption", () => deliverResumption(context)),
            Match.tag("ResumptionDelivered", ({ requestId, result }) =>
              Effect.gen(function* () {
                if (resumptionInFlight !== requestId) return;
                const current = registry.get(runPath)!;
                const success = result._tag === "Success" && result.value.requestId === requestId;
                yield* registry
                  .commit(
                    {
                      ...current,
                      state: {
                        ...state(),
                        resumptions: state().resumptions?.map((item) =>
                          item.input.requestId !== requestId
                            ? item
                            : {
                                ...Struct.omit(item, ["error"]),
                                status: success ? "delivered" : "pending",
                                ...(!success
                                  ? {
                                      error:
                                        result._tag === "Failure"
                                          ? result.error.message
                                          : "Resume receipt identity mismatch",
                                    }
                                  : {}),
                              },
                        ),
                      },
                    },
                    { expectedRevision: current.revision ?? 0 },
                  )
                  .pipe(Effect.orDie);
                resumptionInFlight = undefined;
              }),
            ),
            Match.tag("StartTask", ({ input, replyTo }) =>
              Effect.gen(function* () {
                const result = yield* admitTask(input, context).pipe(Effect.result);
                if (result._tag === "Failure")
                  return yield* replyTo.tell({ _tag: "Rejected", error: result.failure });
                yield* replyTo.tell({ _tag: "Accepted", receipt: result.success.receipt });
                if (result.success.created) {
                  yield* requestConfirmation(context);
                  yield* notify("Task accepted; awaiting user confirmation", false);
                }
              }),
            ),
            Match.tag("Cancel", ({ reason, replyTo }) =>
              cancel(context, reason).pipe(
                Effect.andThen(() => (replyTo ? replyTo.tell(undefined) : Effect.void)),
              ),
            ),
            Match.tag("ApprovalResolved", ({ requestId, response }) =>
              Effect.gen(function* () {
                if (yield* writeback.resolve(requestId, context)) return;
                if (requestId !== `${runPath}:confirm`) return;
                const decision = approvalEntries(registry).find((entry) => entry.id === requestId);
                if (
                  !decision ||
                  !["resolved", "acknowledged"].includes(decision.status) ||
                  decision.target !== context.path ||
                  !isDeepStrictEqual(decision.response, response)
                )
                  return;
                if (!valid() && state().status === "awaiting-confirmation") {
                  yield* cancel(context, "Confirmation refers to an obsolete task revision");
                  return;
                }
                if (!state().approvals?.includes(requestId)) {
                  if (state().status !== "awaiting-confirmation") return;
                  yield* transition({ type: "ConfirmationResolved", requestId, response });
                  yield* sendApproval(context, {
                    _tag: "Acknowledge",
                    id: requestId,
                    target: context.path,
                  });
                  if (response.decision === "approve") yield* launch(context);
                  else yield* notify("The user rejected this execution", true);
                } else
                  yield* sendApproval(context, {
                    _tag: "Acknowledge",
                    id: requestId,
                    target: context.path,
                  });
              }),
            ),
            Match.tag("Resume", ({ path }) =>
              Effect.gen(function* () {
                if (runPath) {
                  yield* replayTerminal(true);
                  return;
                }
                runPath = path;
                if (!registry.get(path)) return;
                const current = state();
                yield* writeback.recover(context);
                if (current.resumptions?.some((item) => item.status === "pending")) {
                  yield* context.self.tell({ _tag: "DeliverResumption" });
                  return;
                }
                if (["ready", "awaiting-confirmation"].includes(current.status) && !valid()) {
                  yield* cancel(context, "Task changed before restart");
                  return;
                }
                if (
                  current.resumptions?.length &&
                  ["failed", "uncertain"].includes(current.status)
                ) {
                  yield* launch(context, true);
                  return;
                }
                if (yield* replayTerminal(true)) return;
                if (current.status === "awaiting-confirmation") yield* requestConfirmation(context);
                else if (current.status === "ready") yield* launch(context);
                else yield* launch(context, true);
              }),
            ),
            Match.tag("Submitted", ({ result }) =>
              Effect.gen(function* () {
                if (result._tag === "Failure") {
                  yield* transition({ type: "Error", text: result.error.message });
                  yield* notify(result.error.message, true);
                  return;
                }
                yield* transition({ type: "Submitted", sessionId: result.value });
                yield* notify(`Agent execution started: ${result.value}`, false);
              }),
            ),
            Match.tag("Progress", ({ result }) =>
              Effect.gen(function* () {
                if (state().status === "waiting_input") return;
                const text = result._tag === "Success" ? result.value : result.error.message;
                yield* transition({ type: "WaitingInput", text });
                yield* notify(text, false);
              }),
            ),
            Match.tag("Finished", ({ outcome }) =>
              Effect.gen(function* () {
                if (state().status === "completed") {
                  if (outcome._tag !== "Completed" || outcome.text !== state().outcomeText)
                    return yield* Effect.die(
                      new Error("A completed Run cannot replace its frozen result"),
                    );
                  return;
                }
                yield* transition({ type: "Finished", outcome });
                yield* notify(outcome.text, true);
                yield* writeback.recover(context);
              }),
            ),
            Match.tag("WritebackFinished", writeback.finish),
            Match.exhaustive,
          ),
      });
    }),
  );
}
