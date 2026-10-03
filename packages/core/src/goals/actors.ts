import { goalEvaluationSchedule, compactionRetry } from "./evaluation-schedule.js";
import { planSignalChanges } from "./signal-proposals.js";
import { goalInputs, inputMessage } from "./inputs.js";
import { goalSignalOutbox } from "./signal-outbox.js";
import { goalTaskExecution } from "./task-execution.js";
import { GoalState } from "./state.js";
import { goalWorkingState } from "./working-state.js";
import { GoalOperationError } from "./errors.js";
import { createHash, randomUUID } from "node:crypto";
import { ReplyTo, Actor, type ActorRef } from "@aster/actor";
import { ContextActor } from "../context/actor.js";
import { defineContext } from "../context/model.js";
import { ContextRegistry } from "../context/registry.js";
import { DateTime, Effect, Layer, Match, Schema, Semaphore } from "effect";
import { evaluationIdentity, type GoalEvaluationRecord } from "./evaluation-record.js";
import type { AgentMessage } from "@aster/agent";
import type { GoalDefinition } from "../config/schema.js";
import { GoalRuntime } from "./runtime.js";
import { evaluateGoal } from "./evaluation.js";
import { GoalPlan } from "./plan.js";
import { FrozenGoalEvaluation } from "./frozen-evaluation.js";
import { isSignalToolRequest, GoalToolError, GoalToolRequest } from "./tasks.js";
import { contextSize, makeMemoryGoalHistory, repairGoalHistory } from "./history.js";
import { TaskPreparation, ExternalAgents } from "../tasks/model.js";
import { GoalIntentInput } from "./intent.js";
import {
  RetryGoalSignalInput,
  CommandReceipt,
  CausalChain,
  ApplicationError,
  GoalDeliveryInput,
  GoalDeliveryReceipt,
} from "@aster/api-contracts";
import { goalInbox } from "./inbox.js";
import { goalIntentInbox } from "./intent-inbox.js";

export const GoalDeliveryReply = Schema.Union([
  Schema.TaggedStruct("Accepted", { receipt: GoalDeliveryReceipt }),
  Schema.TaggedStruct("Rejected", { error: ApplicationError }),
]);
export type GoalDeliveryReply = typeof GoalDeliveryReply.Type;

export const GoalCommandReply = Schema.Union([
  Schema.TaggedStruct("Accepted", {}),
  Schema.TaggedStruct("Rejected", { error: ApplicationError }),
  Schema.TaggedStruct("Unavailable", { message: Schema.String }),
]);
export type GoalCommandReply = typeof GoalCommandReply.Type;

const ReadyCommand = Schema.TaggedStruct("Ready", { replyTo: ReplyTo<void>() });
export const GoalCommand = Schema.Union([
  ReadyCommand,
  Schema.TaggedStruct("RetrySignal", {
    input: RetryGoalSignalInput,
    replyTo: ReplyTo<GoalDeliveryReply>(),
  }),
  Schema.TaggedStruct("Deliver", {
    input: GoalDeliveryInput,
    replyTo: ReplyTo<GoalDeliveryReply>(),
  }),
  Schema.TaggedStruct("Evaluate", { reason: Schema.String }),
  Schema.TaggedStruct("Intent", {
    input: GoalIntentInput,
    replyTo: ReplyTo<GoalDeliveryReply>(),
  }),
  Schema.TaggedStruct("UserMessage", {
    requestId: Schema.optional(Schema.NonEmptyString),
    text: Schema.String,
    replyTo: Schema.optional(ReplyTo<GoalCommandReply>()),
  }),
  Schema.TaggedStruct("End", { replyTo: Schema.optional(ReplyTo<GoalCommandReply>()) }),
  Schema.TaggedStruct("Occurrence", {
    causal: Schema.optional(CausalChain),
    id: Schema.String,
    signalPath: Schema.String,
    text: Schema.String,
    replyTo: ReplyTo<{ accepted: boolean }>(),
  }),
  Schema.TaggedStruct("Execution", {
    causal: Schema.optional(CausalChain),
    runPath: Schema.String,
    evaluationId: Schema.optional(Schema.String),
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
  Schema.TaggedStruct("SignalDelivered", {
    requestId: Schema.String,
    generation: Schema.String,
    result: Schema.Union([
      Schema.TaggedStruct("Success", { value: CommandReceipt }),
      Schema.TaggedStruct("Failure", { error: ApplicationError }),
    ]),
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
    through: Schema.optional(Schema.Number),
    result: Schema.Union([
      Schema.TaggedStruct("Success", { value: GoalPlan }),
      Schema.TaggedStruct("Failure", { error: Schema.instanceOf(Error) }),
    ]),
  }),
  Schema.TaggedStruct("FreezeEvaluation", {
    generation: Schema.String,
    requestId: Schema.String,
    input: FrozenGoalEvaluation,
    replyTo: ReplyTo<FrozenGoalEvaluation | undefined>(),
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
      const agents = yield* ExternalAgents;
      const history = runtime.history ?? makeMemoryGoalHistory();
      // Preserve ordered Signal revisions without making remote asks block this mailbox.
      // End/UserMessage must remain processable while another Actor is unresponsive.
      const signalOperations = yield* Semaphore.make(1);
      let definition: GoalDefinition;
      let path = "";
      const schedule = goalEvaluationSchedule();
      const working = goalWorkingState(
        registry,
        history,
        () => definition,
        () => path,
        runtime.contextTokens ?? 48000,
      );
      const { current, state, save, append, event } = working;
      const inputs = goalInputs(working, history);
      const signalOutbox = goalSignalOutbox(working, runtime);
      let signalsRecovered = false;
      const inbox = goalInbox(registry, working, history);
      const intents = goalIntentInbox(registry, working, history);
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
              yield* registry
                .commit(
                  {
                    path,
                    description: definition.description,
                    state: {
                      ...definition,
                      status: "active",
                      summary: "Not yet evaluated",
                      progress: "Not yet evaluated",
                      tasks: [],
                      evaluations: [],
                      historyThrough: 0,
                      agentThrough: 0,
                      historyCount: 0,
                      pendingEvaluation: false,
                      receivedEvents: [],
                      intents: [],
                    },
                    messages: [],
                  },
                  { expectedRevision: 0 },
                )
                .pipe(Effect.asVoid, Effect.orDie);
            // Preserve actual recorded calls and explicitly mark results lost to interruption.
            yield* repairGoalHistory(history, slug, state().historyThrough).pipe(Effect.orDie);
            // Refresh display metadata on restart without replacing durable Goal progress.
            yield* save({ title: definition.title ?? definition.description });
            yield* inputs.project();
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
            Match.tag("RetrySignal", (command) =>
              Effect.gen(function* () {
                const result = yield* signalOutbox.retry(command.input).pipe(Effect.result);
                if (result._tag === "Failure")
                  return yield* command.replyTo.tell({ _tag: "Rejected", error: result.failure });
                yield* command.replyTo.tell({ _tag: "Accepted", receipt: result.success });
                yield* signalOutbox.dispatch(context);
              }),
            ),
            Match.tag("SignalDelivered", (command) => signalOutbox.delivered(command)),
            Match.tag("Ready", (command) => command.replyTo.tell(undefined)),
            Match.tag("Deliver", (command) =>
              Effect.gen(function* () {
                const accepted = yield* inbox.accept(command.input).pipe(Effect.result);
                if (accepted._tag === "Failure")
                  return yield* command.replyTo.tell({ _tag: "Rejected", error: accepted.failure });
                yield* command.replyTo.tell({
                  _tag: "Accepted",
                  receipt: accepted.success.receipt,
                });
                if (accepted.success.created && active())
                  yield* schedule.enqueue(context, "Personal Agent provided a message");
              }),
            ),
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
            Match.tag("FreezeEvaluation", (command) =>
              Effect.gen(function* () {
                const handoff = state().pendingHandoff;
                if (
                  command.generation !== schedule.generation() ||
                  handoff?.requestId !== command.requestId
                )
                  return yield* command.replyTo.tell(undefined);
                if (!handoff.input)
                  yield* save({
                    pendingHandoff: { ...handoff, input: command.input },
                    evaluations: state().evaluations?.map((evaluation) =>
                      evaluation.evaluationId === command.requestId &&
                      evaluation.status === "pending"
                        ? { ...evaluation, status: "running" as const }
                        : evaluation,
                    ),
                  });
                yield* command.replyTo.tell(state().pendingHandoff!.input);
              }),
            ),
            Match.tag("Intent", (command) =>
              Effect.gen(function* () {
                const accepted = yield* intents.accept(command.input).pipe(Effect.result);
                if (accepted._tag === "Failure")
                  return yield* command.replyTo.tell({ _tag: "Rejected", error: accepted.failure });
                yield* command.replyTo.tell({
                  _tag: "Accepted",
                  receipt: accepted.success.receipt,
                });
                if (accepted.success.created && active())
                  yield* schedule.enqueue(
                    context,
                    `Goal intent from ${command.input.intent.source.actorPath}`,
                  );
              }),
            ),
            Match.tag("UserMessage", (command) =>
              Effect.gen(function* () {
                const requestId = command.requestId ?? randomUUID();
                const accepted = yield* inputs
                  .accept({ _tag: "UserInput", text: command.text }, requestId, {
                    pendingEvaluation: active() || state().pendingEvaluation,
                    causal: { rootRequestId: requestId, remainingAgentTurns: 4 },
                  })
                  .pipe(Effect.result);
                if (accepted._tag === "Failure") {
                  if (command.replyTo)
                    yield* command.replyTo.tell({ _tag: "Rejected", error: accepted.failure });
                  return;
                }
                if (command.replyTo) yield* command.replyTo.tell({ _tag: "Accepted" });
                if (accepted.success && active())
                  yield* schedule.enqueue(context, "User provided additional information");
                return;
              }),
            ),
            Match.tag("End", (command) =>
              Effect.gen(function* () {
                yield* schedule.cancel();
                const observedAt = DateTime.formatIso(yield* DateTime.now);
                yield* save({
                  evaluations: state().evaluations?.map((evaluation): GoalEvaluationRecord =>
                    evaluation.status === "running" || evaluation.status === "pending"
                      ? {
                          ...evaluationIdentity(evaluation),
                          status: "reconciliation_required",
                          error:
                            "Goal ended before evaluation application; the Agent outcome is not confirmed cancelled",
                          observedAt,
                        }
                      : evaluation,
                  ),
                  status: "completed",
                  pendingEvaluation: false,
                  pendingRequestId: undefined,
                  pendingHandoff: undefined,
                });
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
                  yield* inputs.accept(
                    {
                      _tag: "SignalOccurrence",
                      occurrenceId: command.id,
                      signalPath: command.signalPath,
                      evidence: command.text,
                    },
                    command.id,
                    {
                      receivedEvents: [...state().receivedEvents, command.id],
                      causal: command.causal ?? {
                        rootRequestId: command.id,
                        remainingAgentTurns: 4,
                      },
                      pendingEvaluation: active(),
                    },
                  );
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
                yield* inputs.accept(
                  {
                    _tag: "ExecutionFeedback",
                    runPath: command.runPath,
                    taskId: command.taskId,
                    evaluationId: command.evaluationId,
                    status: command.status ?? (command.terminal ? "completed" : "running"),
                    terminal: command.terminal,
                    text: command.text,
                  },
                  eventId,
                  {
                    ...(task && (!task.execution || task.execution.runPath === command.runPath)
                      ? {
                          tasks: state().tasks.map((t) =>
                            t.id === task.id
                              ? {
                                  ...t,
                                  execution: {
                                    ...t.execution,
                                    runPath: command.runPath,
                                    status:
                                      command.status ??
                                      (command.terminal ? "completed" : "running"),
                                  },
                                  ...(command.terminal ? { result: command.text } : {}),
                                }
                              : t,
                          ),
                        }
                      : {}),
                    receivedEvents: [...state().receivedEvents, eventId],
                    causal: command.causal ?? state().causal,
                    pendingEvaluation: (command.terminal && active()) || state().pendingEvaluation,
                  },
                );
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
                  Effect.flatMap((value) => command.replyTo.tell({ value })),
                  Effect.catchTags({
                    GoalToolError: (error) => command.replyTo.tell({ error: error.message }),
                    ContextConflict: (error) =>
                      command.replyTo.tell({
                        error: `Goal revision changed from ${error.expectedRevision} to ${error.actualRevision}; read the Goal and retry the operation`,
                      }),
                  }),
                );
                return;
              }),
            ),
            Match.tag("Evaluate", (command) =>
              Effect.gen(function* () {
                yield* schedule.dequeue;
                if (!active()) return;
                const started = yield* schedule.start(command.reason);
                if (!started) {
                  yield* save({ pendingEvaluation: true });
                  return;
                }
                const { generation: currentGeneration, cancelled } = started;
                let handoff = state().pendingHandoff;
                if (
                  runtime.reasoner.durableSessions &&
                  state().pendingRequestId &&
                  (!handoff || handoff.requestId !== state().pendingRequestId)
                ) {
                  // An old request may already have reached Pi. Reconstructing a larger
                  // input prefix would let its replay consume unrelated later inputs.
                  yield* schedule.finish(currentGeneration);
                  yield* save({
                    lastError:
                      "Pending Goal handoff has no matching frozen input range; reconciliation required",
                  });
                  return;
                }
                if (!handoff) {
                  const requestId = randomUUID();
                  const causal = state().causal ?? {
                    rootRequestId: requestId,
                    remainingAgentTurns: 4,
                  };
                  const admissions = state().agentAdmissions ?? [];
                  const count =
                    admissions.find((entry) => entry.rootRequestId === causal.rootRequestId)
                      ?.count ?? 0;
                  if (causal.remainingAgentTurns === 0 || count >= 8) {
                    yield* schedule.finish(currentGeneration);
                    yield* save({
                      pendingEvaluation: false,
                      causal: { ...causal, remainingAgentTurns: 0 },
                      lastError:
                        "Automatic Goal follow-up has reached its limit. Saved evidence remains available; a new user instruction can continue the work.",
                    });
                    return;
                  }
                  yield* event(`Starting evaluation: ${started.reason}`);
                  handoff = {
                    causal,
                    requestId,
                    reason: started.reason,
                    through: yield* history.count(definition.slug).pipe(Effect.orDie),
                  };
                  const previousEvaluation = state().evaluations?.at(-1);
                  const assigned = new Set(
                    state().evaluations?.flatMap((evaluation) => evaluation.inputIds ?? []),
                  );
                  let selected =
                    state().inputs?.filter((input) => !assigned.has(input.inputId)) ?? [];
                  const retrying =
                    selected.length === 0 &&
                    previousEvaluation?.status === "failed" &&
                    !!previousEvaluation.inputIds?.length;
                  if (retrying)
                    selected =
                      state().inputs?.filter((input) =>
                        previousEvaluation.inputIds!.includes(input.inputId),
                      ) ?? [];
                  if (!selected.length) {
                    yield* inputs.accept({ _tag: "Startup", reason: started.reason }, requestId);
                    selected = [state().inputs!.at(-1)!];
                    handoff = {
                      ...handoff,
                      through: yield* history.count(definition.slug).pipe(Effect.orDie),
                    };
                  }
                  if (!retrying) {
                    const inputBudget = Math.max(
                      1,
                      Math.floor(
                        ((runtime.contextTokens ?? 48000) - (runtime.reserveTokens ?? 8192)) / 3,
                      ),
                    );
                    let size = 0;
                    selected = selected.filter((input, index) => {
                      size += contextSize(inputMessage(input));
                      return index === 0 || size <= inputBudget;
                    });
                  }
                  handoff = { ...handoff, through: selected.at(-1)!.historySequence! };
                  const evaluation: GoalEvaluationRecord = {
                    evaluationId: requestId,
                    inputIds: selected.map((input) => input.inputId),
                    ...(retrying ? { retryOf: previousEvaluation.evaluationId } : {}),
                    reason: handoff.reason,
                    historyThrough: handoff.through,
                    startedAt: DateTime.formatIso(yield* DateTime.now),
                    status: runtime.reasoner.durableSessions ? "pending" : "running",
                  };
                  yield* save({
                    evaluations: [...(state().evaluations ?? []), evaluation],
                    causal,
                    agentAdmissions: [
                      ...admissions.filter((entry) => entry.rootRequestId !== causal.rootRequestId),
                      { rootRequestId: causal.rootRequestId, count: count + 1 },
                    ],
                    pendingRequestId: handoff.requestId,
                    pendingHandoff: handoff,
                    pendingEvaluation: (state().inputs ?? []).some(
                      (input) =>
                        !assigned.has(input.inputId) &&
                        !selected.some((item) => item.inputId === input.inputId),
                    ),
                  });
                }
                yield* context.pipeToSelf(
                  evaluateGoal({
                    runtime,
                    registry,
                    definition,
                    history,
                    self: context.self,
                    generation: currentGeneration,
                    reason: handoff.reason,
                    requestId: handoff.requestId,
                    historyThrough: handoff.through,
                  }).pipe(Effect.raceFirst(cancelled)),
                  (result) => ({
                    _tag: "Planned",
                    result:
                      result._tag === "Success"
                        ? { _tag: "Success", value: result.value.plan }
                        : result,
                    through: result._tag === "Success" ? result.value.through : undefined,
                    generation: currentGeneration,
                  }),
                );
                return;
              }),
            ),
            Match.tag("Planned", (command) =>
              Effect.gen(function* () {
                if (!(yield* schedule.finish(command.generation))) return;
                const evaluation = state().evaluations?.find(
                  (item) => item.evaluationId === state().pendingRequestId,
                );
                if (command.result._tag === "Failure") {
                  // The original partial transcript is already durable; close only missing results.
                  yield* repairGoalHistory(history, definition.slug, state().historyThrough).pipe(
                    Effect.orDie,
                  );
                  const failed =
                    command.result.error instanceof GoalOperationError &&
                    command.result.error.outcome === "failed";
                  const failureRecord: GoalEvaluationRecord | undefined = evaluation && {
                    ...evaluationIdentity(evaluation),
                    status: failed ? "failed" : "reconciliation_required",
                    error: command.result.error.message,
                    observedAt: DateTime.formatIso(yield* DateTime.now),
                  };
                  yield* save({
                    lastError: command.result.error.message,
                    ...(failed ? { pendingRequestId: undefined, pendingHandoff: undefined } : {}),
                    ...(failureRecord
                      ? {
                          evaluations: state().evaluations!.map((item) =>
                            item.evaluationId === failureRecord.evaluationId ? failureRecord : item,
                          ),
                        }
                      : {}),
                  });
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
                  const appliedAt = DateTime.formatIso(yield* DateTime.now);
                  const proposals = yield* Effect.gen(function* () {
                    yield* Schema.decodeUnknownEffect(GoalPlan)(plan).pipe(
                      Effect.mapError(
                        () =>
                          new GoalToolError({
                            message: "Invalid evaluation result or disposition",
                          }),
                      ),
                    );
                    const tasks = yield* execution.planChanges(
                      plan.taskChanges ?? [],
                      state().pendingRequestId!,
                      appliedAt,
                      completed,
                    );
                    const signals = yield* planSignalChanges({
                      state: state(),
                      tasks,
                      changes: plan.signalChanges ?? [],
                      registry,
                      agents: Object.keys(agents),
                      evaluationId: state().pendingRequestId!,
                      at: appliedAt,
                      completed,
                    });
                    return { tasks, signals };
                  }).pipe(Effect.result);
                  if (proposals._tag === "Failure") {
                    const rejected: GoalEvaluationRecord | undefined = evaluation && {
                      ...evaluationIdentity(evaluation),
                      status: "failed",
                      error: proposals.failure.message,
                      result: plan,
                      observedAt: appliedAt,
                    };
                    yield* save({
                      pendingRequestId: undefined,
                      pendingHandoff: undefined,
                      lastError: `Evaluation result rejected: ${proposals.failure.message}`,
                      ...(rejected
                        ? {
                            evaluations: state().evaluations!.map((item) =>
                              item.evaluationId === rejected.evaluationId ? rejected : item,
                            ),
                          }
                        : {}),
                    });
                    if (schedule.hasPending() || state().pendingEvaluation)
                      yield* schedule.enqueue(
                        context,
                        "Re-evaluate after rejected result proposals",
                      );
                    return;
                  }
                  const resultRecord: GoalEvaluationRecord | undefined = evaluation && {
                    ...evaluationIdentity(evaluation),
                    status: proposals.success.signals.length ? "partially_applied" : "completed",
                    resultId: evaluation.evaluationId,
                    result: plan,
                    appliedAt,
                    taskOutputs: (plan.taskChanges ?? []).map((change, index) => {
                      const task = proposals.success.tasks.find((item) => item.id === change.id)!;
                      return {
                        id: `${evaluation.evaluationId}:task:${index}`,
                        taskId: change.id,
                        operation: change.operation,
                        title: task.title,
                        ...(change.operation === "task_execute" && task.execution
                          ? { runPath: task.execution.runPath }
                          : {}),
                      };
                    }),
                  };
                  yield* save({
                    tasks: proposals.success.tasks,
                    signalOutbox: [...(state().signalOutbox ?? []), ...proposals.success.signals],
                    ...(resultRecord
                      ? {
                          evaluations: state().evaluations!.map((item) =>
                            item.evaluationId === resultRecord.evaluationId ? resultRecord : item,
                          ),
                        }
                      : {}),
                    ...(runtime.reasoner.durableSessions && command.through !== undefined
                      ? { agentThrough: command.through }
                      : {}),
                    pendingRequestId: undefined,
                    pendingHandoff: undefined,
                    lastError: undefined,
                    summary: plan.progress,
                    progress: plan.progress,
                    status: completed ? "completed" : "active",
                    pendingEvaluation: state().pendingEvaluation || schedule.hasPending(),
                  });
                  yield* signalOutbox.dispatch(context);
                  // Reservations and Task revisions are durable before any Run is created or cancelled.
                  if (plan.taskChanges?.length) yield* execution.recover(context, false);
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
                if (
                  (schedule.hasPending() ||
                    (command.result._tag === "Success" && state().pendingEvaluation)) &&
                  active()
                )
                  yield* schedule.enqueue(context, "Process changes received during evaluation");
                return;
              }),
            ),
            Match.tag("Reconciled", (command) =>
              Effect.gen(function* () {
                // Startup restoration only; tool completions never release the planning lock.
                if (command.result._tag === "Failure") yield* event(command.result.error.message);
                if (!signalsRecovered) {
                  signalsRecovered = true;
                  yield* signalOutbox.recover(context);
                }
                yield* schedule.reconciled;
                if (
                  !schedule.isQueued() &&
                  (schedule.hasPending() || state().pendingEvaluation || state().pendingRequestId)
                )
                  yield* schedule.enqueue(context, "Recover pending changes");
                return;
              }),
            ),
            Match.exhaustive,
            // Persistence and ownership failures recover through Actor supervision.
            Effect.orDie,
          ),
      });
    }),
  );
}

export const GoalsRootCommand = Schema.Union([
  ReadyCommand,
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
            if (command._tag === "Ready") {
              for (const child of yield* context.children())
                yield* (child as ActorRef<GoalCommand>)
                  .ask<void>((replyTo) => ({ _tag: "Ready", replyTo }))
                  .pipe(Effect.orDie);
              return yield* command.replyTo.tell(undefined);
            }
            if (command._tag === "Route") {
              const configured = runtime.definitions.some((goal) => goal.slug === command.slug);
              const child = configured ? yield* context.child(command.slug) : undefined;
              if (child) yield* (child as ActorRef<GoalCommand>).tell(command.command);
              else if (
                command.command._tag === "Deliver" ||
                command.command._tag === "Intent" ||
                command.command._tag === "RetrySignal"
              )
                yield* command.command.replyTo.tell({
                  _tag: "Rejected",
                  error: new ApplicationError({
                    kind: "not-found",
                    message: "Goal Actor unavailable",
                  }),
                });
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
