import { createHash } from "node:crypto";
import { CausalChain } from "@aster/api-contracts";
import { runNotifications } from "../notifications/run.js";
import { GoalState } from "../goals/state.js";
import { isDeepStrictEqual } from "node:util";
import {
  ApplicationError,
  CommandReceipt,
  TaskDeliveryInput,
  ResumeRunDeliveryInput,
} from "@aster/api-contracts";
import { personalTaskPath } from "./admission.js";
import { transitionRun, type RunTransition } from "./run-transition.js";
import { GoalTaskReference, RunState, terminalRunText } from "./run-state.js";
import { makeRunWriteback, planWriteback, WritebackFinished } from "./writeback.js";
import { Clock, Effect, Match, Layer, Schema, Struct } from "effect";
import { ReplyTo, type ActorContext, type ActorRef } from "@aster/actor";
import { ContextActor, contextSpawnOptions, contextPath } from "../context/actor.js";
import { defineContext, ContextRecord } from "../context/model.js";
import { ContextRegistry } from "../context/registry.js";
import { SignalDefinition } from "../config/schema.js";
import {
  DelegationActor,
  DelegationUpdate,
  type ResumeExecutionReply,
} from "../delegation/actor.js";
import { Task, TaskPreparation, ExternalAgents, taskPrompt } from "./model.js";
import { ApprovalResolved, sendApproval } from "../approvals/actor.js";
import type { GoalCommand } from "../goals/actors.js";

// Runs own preparation, confirmation and external execution; a Signal only owns
// when to create an occurrence. Goals can create Runs without creating a monitor.
interface Triggered {
  readonly type: "Triggered";
  readonly at: string;
  readonly sourcePath: string;
  readonly task: string;
  readonly agent: string;
  readonly mode: "auto" | "confirm";
  readonly sourceContext: ContextRecord;
}

export const RunAdmissionReply = Schema.Union([
  Schema.TaggedStruct("Accepted", { receipt: CommandReceipt }),
  Schema.TaggedStruct("Rejected", { error: ApplicationError }),
]);
export type RunAdmissionReply = typeof RunAdmissionReply.Type;
export const StartPersonalTask = Schema.TaggedStruct("StartPersonalTask", {
  input: TaskDeliveryInput,
  replyTo: ReplyTo<RunAdmissionReply>(),
});
export const ResumePersonalRun = Schema.TaggedStruct("ResumePersonalRun", {
  input: ResumeRunDeliveryInput,
  replyTo: ReplyTo<RunAdmissionReply>(),
});
export const RunCommand = Schema.Union([
  WritebackFinished,
  ResumePersonalRun,
  Schema.TaggedStruct("DeliverResumption", {}),
  Schema.TaggedStruct("ResumptionDelivered", {
    requestId: Schema.String,
    result: Schema.Union([
      Schema.TaggedStruct("Success", { value: CommandReceipt }),
      Schema.TaggedStruct("Failure", { error: Schema.instanceOf(ApplicationError) }),
    ]),
  }),
  StartPersonalTask,
  Schema.TaggedStruct("Cancel", {
    reason: Schema.String,
    replyTo: Schema.optional(ReplyTo<void>()),
  }),
  Schema.TaggedStruct("Initialize", {
    causal: Schema.optional(CausalChain),
    path: Schema.String,
    definition: SignalDefinition,
    sourceContext: ContextRecord,
    subscriber: Schema.optional(ReplyTo<GoalCommand>()),
    goalTask: Schema.optional(GoalTaskReference),
    replyTo: Schema.optional(ReplyTo<void>()),
  }),
  DelegationUpdate,
  ApprovalResolved,
  Schema.TaggedStruct("Prepared", { task: Task }),
  Schema.TaggedStruct("Ready", { ready: Schema.Boolean }),
  Schema.TaggedStruct("PreparationFailed", { error: Schema.String }),
  Schema.TaggedStruct("Resume", {
    path: Schema.String,
    subscriber: Schema.optional(ReplyTo<GoalCommand>()),
  }),
]);
export type RunCommand = typeof RunCommand.Type;
export class SignalRunActor extends ContextActor.Service<
  SignalRunActor,
  TaskPreparation | ExternalAgents
>()("signals/RunActor", {
  command: RunCommand,
  context: defineContext({
    identity: "An occurrence of a Signal",
    state: RunState,
    message: Schema.Unknown,
    capture: (record) => {
      const trigger = record.messages.find(
        (message) =>
          typeof message === "object" &&
          message !== null &&
          "type" in message &&
          message.type === "Triggered",
      ) as Triggered | undefined;
      const terminal = [
        "completed",
        "uncertain",
        "failed",
        "cancelled",
        "rejected",
        "preparation-failed",
        "blocked",
      ].includes(String(record.state.status));
      return trigger
        ? {
            sessionId: `${record.path}:${terminal ? `outcome:${record.state.status}` : "trigger"}`,
            records: [record, trigger.sourceContext],
          }
        : undefined;
    },
  }),
}) {
  static readonly layer = Layer.effect(
    SignalRunActor,
    Effect.gen(function* () {
      const registry = yield* ContextRegistry;
      const preparation = yield* TaskPreparation;
      const agents = yield* ExternalAgents;
      let resumptionInFlight: string | undefined;
      let runPath = "";
      let subscriber: ActorRef<GoalCommand> | undefined;
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
                businessOutbox: runNotifications({
                  path: runPath,
                  revision: (current.revision ?? 0) + 1,
                  at,
                  previous: state(),
                  next: next.state,
                }),
              },
              messages: [...current.messages, { ...next.event, at }],
            },
            { expectedRevision: current.revision ?? 0 },
          )
          .pipe(Effect.asVoid, Effect.orDie);
      });
      const writeback = yield* makeRunWriteback({ path: () => runPath, state });
      const valid = () => {
        const reference = state().goalTask;
        if (!reference) return true;
        const record = registry.get(reference.goalPath);
        const goal = record && Schema.decodeUnknownSync(GoalState)(record.state);
        const task = goal?.tasks.find((t) => t.id === reference.taskId);
        return (
          goal?.status === "active" &&
          task?.status === "open" &&
          task.revision === reference.revision
        );
      };
      const notify = (text: string, terminal: boolean) =>
        subscriber
          ? subscriber
              .ask<import("../goals/protocol.js").GoalCommandReply>((replyTo) => ({
                _tag: "SubmitInput",
                requestId: createHash("sha256")
                  .update(JSON.stringify([runPath, state().status, text]))
                  .digest("hex"),
                replyTo,
                input: {
                  _tag: "ExecutionFeedback",
                  runPath,
                  causal: state().admission?.input.causal ?? state().causal,
                  text,
                  terminal,
                  status: state().status,
                  taskId: state().goalTask?.taskId,
                  evaluationId: state().goalTask?.evaluationId,
                },
              }))
              .pipe(
                Effect.asVoid,
                Effect.catchTag("AskTimeoutError", (error) => Effect.logWarning(error.message)),
              )
          : Effect.void;
      const replayTerminal = Effect.fnUntraced(function* (includeUncertain = false) {
        const text = terminalRunText(state(), registry.get(runPath)!.messages, includeUncertain);
        if (text === undefined) return false;
        yield* notify(text, true);
        return true;
      });
      const cancel = (
        context: ActorContext<RunCommand, TaskPreparation | ExternalAgents | ContextRegistry>,
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
        context: ActorContext<RunCommand, TaskPreparation | ExternalAgents | ContextRegistry>,
        recovering = false,
      ) =>
        Effect.gen(function* () {
          const current = state();
          if (!current.task) return;
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
            request: { runPath, task: current.task, agent: current.definition.agent },
          });
        });
      const requestConfirmation = (
        context: ActorContext<RunCommand, TaskPreparation | ExternalAgents | ContextRegistry>,
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
              prompt: taskPrompt(state().task!),
            },
          },
        });
      const check = (
        context: ActorContext<RunCommand, TaskPreparation | ExternalAgents | ContextRegistry>,
      ) =>
        context.pipeToSelf(
          preparation.ready(state().definition, registry.project(state().source), state().task!),
          (result) =>
            result._tag === "Success"
              ? { _tag: "Ready", ready: result.value }
              : { _tag: "PreparationFailed", error: result.error.message },
        );
      const admitPersonal = Effect.fn("Run.admitPersonal")(function* (
        raw: TaskDeliveryInput,
        actor: ActorContext<RunCommand, TaskPreparation | ExternalAgents | ContextRegistry>,
      ) {
        const input = yield* Schema.decodeUnknownEffect(TaskDeliveryInput)(raw).pipe(
          Effect.mapError(
            () => new ApplicationError({ kind: "invalid-input", message: "Invalid Task command" }),
          ),
        );
        const path = contextPath(actor);
        if (input.target !== path || path !== personalTaskPath(input.requestId))
          return yield* new ApplicationError({
            kind: "invalid-input",
            message: "Task command identity does not match its Run",
          });
        const existing = registry.get(path);
        if (existing) {
          const admission = Schema.decodeUnknownSync(RunState)(existing.state).admission;
          if (!admission || !isDeepStrictEqual(admission.input, input))
            return yield* new ApplicationError({
              kind: "conflict",
              message: "Run belongs to another Task command",
            });
          return { receipt: admission.receipt, created: false };
        }
        if (!agents[input.agent])
          return yield* new ApplicationError({
            kind: "invalid-input",
            message: "Task executor is not configured",
          });
        const receipt = { requestId: input.requestId, revision: 1 };
        const definition = {
          slug: path.slice("/runs/".length),
          when: "Explicit Personal task",
          task: input.task.instructions,
          agent: input.agent,
          mode: "confirm" as const,
        };
        const sourceContext = {
          path: input.source,
          description: "Explicit Personal task",
          state: { requestId: input.requestId, causationId: input.causationId },
          messages: [],
        };
        yield* registry
          .commit(
            {
              path,
              description: `Task: ${input.task.instructions}`,
              state: {
                admission: { input, receipt },
                signalSlug: definition.slug,
                sourcePath: input.source,
                definition,
                source: sourceContext,
                status: "checking",
                task: input.task,
              },
              messages: [
                {
                  type: "Triggered",
                  at: input.createdAt,
                  sourcePath: input.source,
                  task: input.task.instructions,
                  agent: input.agent,
                  mode: "confirm",
                  sourceContext,
                  requestId: input.requestId,
                  causationId: input.causationId,
                  target: path,
                  revision: receipt.revision,
                },
              ],
            },
            { expectedRevision: input.expectedRevision },
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
        if (!saved.task || !["failed", "uncertain"].includes(saved.status))
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
                  causationId: input.causationId,
                  at: input.createdAt,
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
        actor: ActorContext<RunCommand, TaskPreparation | ExternalAgents | ContextRegistry>,
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
          request: { runPath, task: saved.task!, agent: saved.definition.agent },
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
      return SignalRunActor.of({
        started: (context) =>
          Effect.gen(function* () {
            const record = registry.get(contextPath(context));
            const saved = record && Schema.decodeUnknownSync(RunState)(record.state);
            if (!saved) return;
            const parent = saved.goalTask
              ? yield* context.select("..").resolve().pipe(Effect.orDie)
              : undefined;
            yield* context.self.tell({
              _tag: "Resume",
              path: contextPath(context),
              subscriber: parent as ActorRef<GoalCommand> | undefined,
            });
          }),
        receive: (command, context) =>
          Match.value(command).pipe(
            Match.tag("ResumePersonalRun", ({ input, replyTo }) =>
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
            Match.tag("StartPersonalTask", ({ input, replyTo }) =>
              Effect.gen(function* () {
                const result = yield* admitPersonal(input, context).pipe(Effect.result);
                if (result._tag === "Failure")
                  return yield* replyTo.tell({ _tag: "Rejected", error: result.failure });
                yield* replyTo.tell({ _tag: "Accepted", receipt: result.success.receipt });
                if (result.success.created) yield* check(context);
              }),
            ),
            Match.tag("Initialize", (command) =>
              Effect.gen(function* () {
                if (registry.get(command.path)) {
                  yield* context.self.tell({
                    _tag: "Resume",
                    path: command.path,
                    subscriber: command.subscriber,
                  });
                  if (command.replyTo) yield* command.replyTo.tell(undefined);
                  return;
                }
                runPath = command.path;
                subscriber = command.subscriber;
                yield* registry
                  .commit(
                    {
                      path: runPath,
                      description: `An occurrence of Signal ${command.definition.slug}`,
                      state: {
                        causal: command.causal,
                        goalTask: command.goalTask,
                        signalSlug: command.definition.slug,
                        sourcePath: command.sourceContext.path,
                        definition: command.definition,
                        source: command.sourceContext,
                        status: "preparing",
                      },
                      messages: [
                        {
                          type: "Triggered",
                          at: new Date().toISOString(),
                          sourcePath: command.sourceContext.path,
                          task: command.definition.task,
                          agent: command.definition.agent,
                          mode: command.definition.mode,
                          sourceContext: command.sourceContext,
                        },
                      ],
                    },
                    { expectedRevision: 0 },
                  )
                  .pipe(Effect.asVoid, Effect.orDie);
                if (command.replyTo) yield* command.replyTo.tell(undefined);
                yield* Effect.logInfo(
                  JSON.stringify({ event: "task.preparation.started", runPath }),
                );
                yield* context.pipeToSelf(
                  preparation.prepare(
                    command.definition,
                    registry.project(command.sourceContext),
                    registry.publicSnapshot(),
                  ),
                  (result) =>
                    result._tag === "Success"
                      ? { _tag: "Prepared", task: result.value }
                      : { _tag: "PreparationFailed", error: result.error.message },
                );
              }),
            ),
            Match.tag("Cancel", ({ reason, replyTo }) =>
              cancel(context, reason).pipe(
                Effect.andThen(() => (replyTo ? replyTo.tell(undefined) : Effect.void)),
              ),
            ),
            Match.tag("Prepared", ({ task }) =>
              Effect.gen(function* () {
                if (state().status !== "preparing") return;
                if (!valid()) {
                  yield* cancel(context, "Task changed during preparation");
                  return;
                }
                yield* transition({ type: "TaskPrepared", task });
                yield* Effect.logInfo(JSON.stringify({ event: "task.prepared", runPath }));
                yield* check(context);
              }),
            ),
            Match.tag("Ready", ({ ready }) =>
              Effect.gen(function* () {
                if (state().status !== "checking") return;
                if (!valid()) {
                  yield* cancel(context, "Task changed during readiness check");
                  return;
                }
                yield* Effect.logInfo(JSON.stringify({ event: "task.readiness", runPath, ready }));
                if (!ready) {
                  yield* transition({ type: "NotExecutable" });
                  yield* notify("The execution plan is not currently executable", true);
                  return;
                }
                if (state().definition.mode === "confirm") {
                  yield* transition({ type: "ConfirmationRequested" });
                  yield* requestConfirmation(context);
                  yield* notify("Execution plan prepared; awaiting user confirmation", false);
                } else {
                  yield* transition({ type: "Ready" });
                  yield* launch(context);
                }
              }),
            ),
            Match.tag("PreparationFailed", ({ error }) =>
              Effect.gen(function* () {
                if (["cancelled", "completed"].includes(state().status)) return;
                yield* transition({ type: "PreparationFailed", text: error });
                yield* notify(error, true);
              }),
            ),
            Match.tag("ApprovalResolved", ({ requestId, response }) =>
              Effect.gen(function* () {
                if (yield* writeback.resolve(requestId, context)) return;
                if (requestId !== `${runPath}:confirm`) return;
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
            Match.tag("Resume", ({ path, subscriber: restoredSubscriber }) =>
              Effect.gen(function* () {
                if (runPath) {
                  if (restoredSubscriber) {
                    subscriber = restoredSubscriber;
                    yield* replayTerminal(true);
                  }
                  return;
                }
                runPath = path;
                subscriber = restoredSubscriber;
                if (!registry.get(path)) return;
                const current = state();
                yield* writeback.recover(context);
                if (current.resumptions?.some((item) => item.status === "pending")) {
                  yield* context.self.tell({ _tag: "DeliverResumption" });
                  return;
                }
                if (
                  ["preparing", "checking", "ready", "awaiting-confirmation"].includes(
                    current.status,
                  ) &&
                  !valid()
                ) {
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
                if (yield* replayTerminal()) return;
                if (current.status === "awaiting-confirmation" && current.task)
                  yield* requestConfirmation(context);
                else if (current.status === "checking" && current.task) yield* check(context);
                else if (current.status === "ready" && current.task) yield* launch(context);
                else if (current.task && current.definition) yield* launch(context, true);
                else {
                  const text =
                    "Task was not prepared before interruption; no external task submitted";
                  yield* transition({ type: "RecoveryFailed", text });
                  yield* replayTerminal();
                }
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
