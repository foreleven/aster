import { GoalState } from "../goals/state.js";
import { transitionRun, type RunTransition } from "./run-transition.js";
import { GoalTaskReference, RunState, terminalRunText } from "./run-state.js";
import { Clock, Effect, Match, Layer, Schema } from "effect";
import { ReplyTo, type ActorContext, type ActorRef } from "@aster/actor";
import { ContextActor, contextSpawnOptions, contextPath } from "../context/actor.js";
import { defineContext, ContextRecord } from "../context/model.js";
import { ContextRegistry } from "../context/registry.js";
import { SignalDefinition } from "../config/schema.js";
import { DelegationActor, DelegationUpdate } from "../delegation/actor.js";
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

export const RunCommand = Schema.Union([
  Schema.TaggedStruct("Cancel", {
    reason: Schema.String,
    replyTo: Schema.optional(ReplyTo<void>()),
  }),
  Schema.TaggedStruct("Initialize", {
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
      let runPath = "";
      let subscriber: ActorRef<GoalCommand> | undefined;
      const state = () => Schema.decodeUnknownSync(RunState)(registry.get(runPath)!.state);
      const transition = Effect.fn("Run.transition")(function* (change: RunTransition) {
        const current = registry.get(runPath)!;
        const next = transitionRun(state(), change);
        const at = new Date(yield* Clock.currentTimeMillis).toISOString();
        return yield* registry.set({
          ...current,
          state: next.state,
          messages: [...current.messages, { ...next.event, at }],
        });
      });
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
          ? subscriber.tell({
              _tag: "Execution",
              runPath,
              text,
              terminal,
              status: state().status,
              taskId: state().goalTask?.taskId,
            })
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
            ["submitting", "running", "waiting_input", "uncertain", "completed"].includes(
              state().status,
            )
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
          preparation.ready(state().definition, state().source, state().task!),
          (result) =>
            result._tag === "Success"
              ? { _tag: "Ready", ready: result.value }
              : { _tag: "PreparationFailed", error: result.error.message },
        );
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
                yield* registry.set({
                  path: runPath,
                  description: `An occurrence of Signal ${command.definition.slug}`,
                  state: {
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
                });
                if (command.replyTo) yield* command.replyTo.tell(undefined);
                yield* Effect.logInfo(
                  JSON.stringify({ event: "task.preparation.started", runPath }),
                );
                yield* context.pipeToSelf(
                  preparation.prepare(
                    command.definition,
                    command.sourceContext,
                    registry.snapshot(),
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
                if (
                  ["preparing", "checking", "ready", "awaiting-confirmation"].includes(
                    current.status,
                  ) &&
                  !valid()
                ) {
                  yield* cancel(context, "Task changed before restart");
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
                yield* transition({ type: "Finished", outcome });
                yield* notify(outcome.text, true);
              }),
            ),
            Match.exhaustive,
          ),
      });
    }),
  );
}
