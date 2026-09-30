import { goalEvaluationSchedule, compactionRetry } from "./evaluation-schedule.js";
import { goalTaskExecution } from "./task-execution.js";
import { GoalState } from "./state.js";
import { goalWorkingState } from "./working-state.js";
import { GoalOperationError } from "./errors.js";
import { createHash } from "node:crypto";
import { ReplyTo, Actor, type ActorRef } from "@aster/actor";
import { ContextActor } from "../context/actor.js";
import { defineContext } from "../context/model.js";
import { ContextRegistry } from "../context/registry.js";
import { Effect, Layer, Match, Schema, Semaphore } from "effect";
import type { AgentMessage } from "@aster/agent";
import type { GoalDefinition } from "../config/schema.js";
import { GoalRuntime } from "./runtime.js";
import { evaluateGoal } from "./evaluation.js";
import { GoalPlan } from "./plan.js";
import { isSignalToolRequest, GoalToolError, GoalToolRequest } from "./tasks.js";
import { makeMemoryGoalHistory, repairGoalHistory } from "./history.js";
import { TaskPreparation, ExternalAgents } from "../tasks/model.js";

export const GoalCommandReply = Schema.Union([
  Schema.TaggedStruct("Accepted", {}),
  Schema.TaggedStruct("Unavailable", { message: Schema.String }),
]);
export type GoalCommandReply = typeof GoalCommandReply.Type;

export const GoalCommand = Schema.Union([
  Schema.TaggedStruct("Evaluate", { reason: Schema.String }),
  Schema.TaggedStruct("UserMessage", {
    text: Schema.String,
    replyTo: Schema.optional(ReplyTo<GoalCommandReply>()),
  }),
  Schema.TaggedStruct("End", { replyTo: Schema.optional(ReplyTo<GoalCommandReply>()) }),
  Schema.TaggedStruct("Occurrence", {
    id: Schema.String,
    signalPath: Schema.String,
    text: Schema.String,
    replyTo: ReplyTo<{ accepted: boolean }>(),
  }),
  Schema.TaggedStruct("Execution", {
    runPath: Schema.String,
    text: Schema.String,
    terminal: Schema.Boolean,
    status: Schema.optional(Schema.String),
    taskId: Schema.optional(Schema.String),
  }),
  Schema.TaggedStruct("Tool", {
    generation: Schema.optional(Schema.String),
    request: GoalToolRequest,
    replyTo: ReplyTo<{ value?: unknown; error?: string }>(),
  }),
  Schema.TaggedStruct("SignalEdited", {
    replyTo: ReplyTo<{ value?: unknown; error?: string }>(),
    result: Schema.Union([
      Schema.TaggedStruct("Success", { value: Schema.Unknown }),
      Schema.TaggedStruct("Failure", { error: Schema.instanceOf(GoalToolError) }),
    ]),
  }),
  Schema.TaggedStruct("Transcript", {
    generation: Schema.String,
    message: Schema.Unknown,
    replyTo: ReplyTo<void>(),
  }),
  Schema.TaggedStruct("Compacted", {
    generation: Schema.String,
    summary: Schema.String,
    through: Schema.Number,
    replyTo: ReplyTo<void>(),
  }),
  Schema.TaggedStruct("Planned", {
    generation: Schema.String,
    result: Schema.Union([
      Schema.TaggedStruct("Success", { value: GoalPlan }),
      Schema.TaggedStruct("Failure", { error: Schema.instanceOf(Error) }),
    ]),
  }),
  Schema.TaggedStruct("Reconciled", {
    result: Schema.Union([
      Schema.TaggedStruct("Success", { value: Schema.Array(ReplyTo<unknown>()) }),
      Schema.TaggedStruct("Failure", { error: Schema.instanceOf(Error) }),
    ]),
  }),
]);
export type GoalCommand = typeof GoalCommand.Type;
export interface GoalMessage {
  readonly type: "user" | "assistant" | "execution" | "error";
  readonly at: string;
  readonly text: string;
  readonly references: readonly string[];
}

export class GoalActor extends ContextActor.Service<
  GoalActor,
  GoalRuntime | TaskPreparation | ExternalAgents
>()("goals/Actor", {
  command: GoalCommand,
  context: defineContext({
    identity: "Ongoing work goal",
    state: GoalState,
    message: Schema.Unknown,
  }),
}) {
  static readonly layer = Layer.effect(
    GoalActor,
    Effect.gen(function* () {
      const registry = yield* ContextRegistry,
        runtime = yield* GoalRuntime;
      const history = runtime.history ?? makeMemoryGoalHistory();
      // Preserve ordered Signal revisions without making remote asks block this mailbox.
      // End/UserMessage must remain processable while another Actor is unresponsive.
      const signalOperations = yield* Semaphore.make(1);
      let definition: GoalDefinition;
      let path = "";
      const schedule = goalEvaluationSchedule();
      const { current, state, save, append, event } = goalWorkingState(
        registry,
        history,
        () => definition,
        () => path,
        runtime.contextTokens ?? 48000,
      );
      const taskById = (id?: string) => state().tasks.find((t) => t.id === id);
      const active = () => state().status === "active";
      const execution = goalTaskExecution(
        registry,
        { current, state, save, append, event },
        () => definition,
        () => path,
      );
      return GoalActor.of({
        started: (context) =>
          Effect.gen(function* () {
            const slug = context.path.split("/").at(-1)!;
            definition = runtime.definitions.find((g) => g.slug === slug)!;
            if (!definition) return yield* Effect.die(new Error(`Unknown Goal ${slug}`));
            path = `/goals/${slug}`;
            if (!registry.get(path))
              yield* registry.set({
                path,
                description: definition.description,
                state: {
                  ...definition,
                  status: "active",
                  summary: "Not yet evaluated",
                  progress: "Not yet evaluated",
                  tasks: [],
                  historyThrough: 0,
                  historyCount: 0,
                  pendingEvaluation: false,
                  receivedEvents: [],
                },
                messages: [],
              });
            // Preserve actual recorded calls and explicitly mark results lost to interruption.
            yield* repairGoalHistory(history, slug, state().historyThrough).pipe(Effect.orDie);
            yield* save();
            yield* execution.recover(context);
            yield* context.pipeToSelf(
              runtime.reconcile(slug, context.self).pipe(
                signalOperations.withPermit,
                Effect.mapError(
                  (cause) =>
                    new GoalOperationError({
                      goal: definition.slug,
                      operation: "reconcile",
                      cause,
                      message: cause.message,
                    }),
                ),
              ),
              (result) => ({ _tag: "Reconciled", result }),
            );
          }),
        receive: (command, context) =>
          Match.value(command).pipe(
            Match.tag("Transcript", (command) =>
              Effect.gen(function* () {
                if (command.generation !== schedule.generation()) {
                  yield* command.replyTo.tell(undefined);
                  return;
                }
                yield* append(command.message as AgentMessage);
                yield* command.replyTo.tell(undefined);
                return;
              }),
            ),
            Match.tag("Compacted", (command) =>
              Effect.gen(function* () {
                if (command.generation !== schedule.generation()) {
                  yield* command.replyTo.tell(undefined);
                  return;
                }
                if (command.through > state().historyThrough)
                  yield* save({ summary: command.summary, historyThrough: command.through });
                yield* command.replyTo.tell(undefined);
                return;
              }),
            ),
            Match.tag("UserMessage", (command) =>
              Effect.gen(function* () {
                yield* append({ role: "user", content: command.text, timestamp: Date.now() });
                // Acceptance includes the durable evaluation intent, not just a mailbox enqueue.
                if (active()) yield* save({ pendingEvaluation: true });
                if (command.replyTo) yield* command.replyTo.tell({ _tag: "Accepted" });
                if (active())
                  yield* schedule.enqueue(context, "User provided additional information");
                return;
              }),
            ),
            Match.tag("End", (command) =>
              Effect.gen(function* () {
                yield* schedule.cancel();
                yield* save({ status: "completed", pendingEvaluation: false });
                yield* event("User ended the Goal");
                if (command.replyTo) yield* command.replyTo.tell({ _tag: "Accepted" });
                yield* execution.cancelPending(context);
                yield* context.pipeToSelf(
                  runtime.deactivate(definition.slug).pipe(
                    signalOperations.withPermit,
                    Effect.as([]),
                    Effect.mapError(
                      (cause) =>
                        new GoalOperationError({
                          goal: definition.slug,
                          operation: "deactivate",
                          cause,
                          message: cause.message,
                        }),
                    ),
                  ),
                  (result) => ({ _tag: "Reconciled", result }),
                );
                return;
              }),
            ),
            Match.tag("Occurrence", (command) =>
              Effect.gen(function* () {
                if (!state().receivedEvents.includes(command.id)) {
                  yield* event(`Signal matched ${command.signalPath}\n${command.text}`);
                  yield* save({
                    receivedEvents: [...state().receivedEvents, command.id],
                    pendingEvaluation: active(),
                  });
                  if (active())
                    yield* schedule.enqueue(context, `Signal matched: ${command.signalPath}`);
                }
                yield* command.replyTo.tell({ accepted: true });
                return;
              }),
            ),
            Match.tag("Execution", (command) =>
              Effect.gen(function* () {
                const eventId = createHash("sha256")
                  .update(
                    `${command.runPath}:${command.status ?? command.terminal}:${command.text}`,
                  )
                  .digest("hex");
                if (state().receivedEvents.includes(eventId)) return;
                const task = taskById(command.taskId);
                if (task && (!task.execution || task.execution.runPath === command.runPath))
                  yield* save({
                    tasks: state().tasks.map((t) =>
                      t.id === task.id
                        ? {
                            ...t,
                            execution: {
                              ...t.execution,
                              runPath: command.runPath,
                              status:
                                command.status ?? (command.terminal ? "completed" : "running"),
                            },
                            ...(command.terminal ? { result: command.text } : {}),
                          }
                        : t,
                    ),
                  });
                yield* event(`Execution feedback ${command.runPath}\n${command.text}`);
                yield* save({
                  receivedEvents: [...state().receivedEvents, eventId],
                  pendingEvaluation: (command.terminal && active()) || state().pendingEvaluation,
                });
                if (command.terminal && active())
                  yield* schedule.enqueue(context, `Execution result: ${command.runPath}`);
                return;
              }),
            ),
            Match.tag("SignalEdited", (command) =>
              Effect.gen(function* () {
                yield* command.replyTo.tell(
                  command.result._tag === "Success"
                    ? { value: command.result.value }
                    : { error: command.result.error.message },
                );
                return;
              }),
            ),
            Match.tag("Tool", (command) =>
              Effect.gen(function* () {
                if (
                  command.generation !== undefined &&
                  command.generation !== schedule.generation()
                ) {
                  yield* command.replyTo.tell({ error: "Goal evaluation is no longer active" });
                  return;
                }
                const req = command.request;
                if (isSignalToolRequest(req)) {
                  yield* context.pipeToSelf(
                    Effect.gen(function* () {
                      // A queued operation may begin after End or a newer evaluation.
                      if (!active()) return yield* new GoalToolError({ message: "Goal has ended" });
                      if (
                        command.generation !== undefined &&
                        command.generation !== schedule.generation()
                      )
                        return yield* new GoalToolError({
                          message: "Goal evaluation is no longer active",
                        });
                      if (!runtime.editSignal)
                        return yield* new GoalToolError({ message: "Signal tools unavailable" });
                      return yield* runtime.editSignal(definition.slug, req, context.self);
                    }).pipe(
                      signalOperations.withPermit,
                      Effect.catchTag("AskTimeoutError", Effect.die),
                    ),
                    (result) => ({ _tag: "SignalEdited", replyTo: command.replyTo, result }),
                  );
                  return;
                }
                const operation = execution.execute(context, req);
                yield* operation.pipe(
                  Effect.matchEffect({
                    onSuccess: (value) => command.replyTo.tell({ value }),
                    onFailure: (error) => command.replyTo.tell({ error: error.message }),
                  }),
                );
                return;
              }),
            ),
            Match.tag("Evaluate", (command) =>
              Effect.gen(function* () {
                yield* schedule.dequeue;
                if (!active()) return;
                yield* save({ pendingEvaluation: true });
                const started = yield* schedule.start(command.reason);
                if (!started) return;
                const { generation: currentGeneration, reason, cancelled } = started;
                yield* event(`Starting evaluation: ${reason}`);
                yield* context.pipeToSelf(
                  evaluateGoal({
                    runtime,
                    registry,
                    definition,
                    history,
                    self: context.self,
                    generation: currentGeneration,
                    reason,
                  }).pipe(Effect.raceFirst(cancelled)),
                  (result) => ({ _tag: "Planned", result, generation: currentGeneration }),
                );
                return;
              }),
            ),
            Match.tag("Planned", (command) =>
              Effect.gen(function* () {
                if (!(yield* schedule.finish(command.generation))) return;
                if (command.result._tag === "Failure") {
                  // The original partial transcript is already durable; close only missing results.
                  yield* repairGoalHistory(history, definition.slug, state().historyThrough).pipe(
                    Effect.orDie,
                  );
                  yield* save({ lastError: command.result.error.message });
                  yield* event(
                    `Evaluation failed; retry is possible: ${command.result.error.message}`,
                  );
                  if (active() && (yield* schedule.retryBudget(command.result.error.message))) {
                    yield* schedule.enqueue(context, compactionRetry);
                    return;
                  }
                } else if (active()) {
                  const plan = command.result.value,
                    completed = plan.completed && !!definition.completionCriteria;
                  yield* save({
                    lastError: undefined,
                    summary: plan.progress,
                    progress: plan.progress,
                    status: completed ? "completed" : "active",
                    pendingEvaluation: schedule.hasPending(),
                  });
                  yield* event(
                    `Evaluation conclusions: ${plan.progress}\nEvidence: ${plan.evidence.join(", ")}`,
                  );
                  if (completed) yield* execution.cancelPending(context);
                  if (completed)
                    yield* context.pipeToSelf(
                      runtime.deactivate(definition.slug).pipe(
                        signalOperations.withPermit,
                        Effect.as([]),
                        Effect.mapError(
                          (cause) =>
                            new GoalOperationError({
                              goal: definition.slug,
                              operation: "deactivate",
                              cause,
                              message: cause.message,
                            }),
                        ),
                      ),
                      (result) => ({ _tag: "Reconciled", result }),
                    );
                }
                if (schedule.hasPending() && active())
                  yield* schedule.enqueue(context, "Process changes received during evaluation");
                return;
              }),
            ),
            Match.tag("Reconciled", (command) =>
              Effect.gen(function* () {
                // Startup restoration only; tool completions never release the planning lock.
                if (command.result._tag === "Failure") yield* event(command.result.error.message);
                yield* schedule.reconciled;
                if (!schedule.isQueued() && (schedule.hasPending() || state().pendingEvaluation))
                  yield* schedule.enqueue(context, "Recover pending changes");
                return;
              }),
            ),
            Match.exhaustive,
          ),
      });
    }),
  );
}

export const GoalsRootCommand = Schema.Union([
  Schema.TaggedStruct("Route", { slug: Schema.String, command: GoalCommand }),
  Schema.TaggedStruct("Initialize", {}),
]);
export type GoalsRootCommand = typeof GoalsRootCommand.Type;
export class GoalsRootActor extends Actor.Service<
  GoalsRootActor,
  ContextRegistry | GoalRuntime | TaskPreparation | ExternalAgents
>()("goals/RootActor", { command: GoalsRootCommand }) {
  static readonly layer = Layer.effect(
    GoalsRootActor,
    Effect.gen(function* () {
      const runtime = yield* GoalRuntime;
      return GoalsRootActor.of({
        started: (context) =>
          Effect.gen(function* () {
            for (const goal of runtime.definitions)
              if (!(yield* context.child(goal.slug))) yield* context.spawn(goal.slug, GoalActor);
          }),
        receive: (command, context) =>
          Effect.gen(function* () {
            if (command._tag === "Route") {
              const configured = runtime.definitions.some((goal) => goal.slug === command.slug);
              const child = configured ? yield* context.child(command.slug) : undefined;
              if (child) yield* (child as ActorRef<GoalCommand>).tell(command.command);
              else if (
                (command.command._tag === "UserMessage" || command.command._tag === "End") &&
                command.command.replyTo
              )
                yield* command.command.replyTo.tell({
                  _tag: "Unavailable",
                  message: "Goal Actor unavailable",
                });
              return;
            }
            for (const goal of runtime.definitions) {
              const child = yield* context.child(goal.slug);
              if (child)
                yield* (child as ActorRef<GoalCommand>).tell({
                  _tag: "Evaluate",
                  reason: "Start Goal evaluation",
                });
            }
          }),
      });
    }),
  );
}
