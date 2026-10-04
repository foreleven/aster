import type { TaskExecutionServices } from "../tasks/execution.js";
import { goalEvaluationSchedule } from "./evaluation-schedule.js";
import { planSignalChanges } from "./signal-proposals.js";
import { goalInputs, inputMessage, newGoalInput } from "./inputs.js";
import { goalSignalOutbox } from "./signal-outbox.js";
import { goalTaskExecution } from "./task-execution.js";
import { GoalState } from "./state.js";
import { goalWorkingState } from "./working-state.js";
import { GoalOperationError } from "./errors.js";
import { randomUUID } from "node:crypto";
import { ReplyTo, Actor, type ActorRef } from "@aster/actor";
import { ContextActor } from "../context/actor.js";
import { defineContext } from "../context/model.js";
import { ContextRegistry } from "../context/registry.js";
import { DateTime, Deferred, Effect, Layer, Option, Match, Schema, Semaphore } from "effect";
import { evaluationIdentity, type GoalEvaluationRecord } from "./evaluation-record.js";
import type { GoalDefinition } from "../config/schema.js";
import { GoalSignals } from "./signal-coordination.js";
import { GoalSettings } from "../config/settings.js";
import { GoalHistoryStore } from "./history.js";
import { MemoryRecall } from "../context/memory.js";
import { ContextQueries } from "../context/queries.js";
import { makeGoalReasoner } from "./agent-reasoner.js";
import { evaluateGoal } from "./evaluation.js";
import { GoalPlan, StoredGoalPlan } from "./plan.js";
import { GoalToolError } from "./tasks.js";
import { contextSize } from "./history.js";
import { ExternalAgents } from "../tasks/model.js";
import { CommandReceipt, ApplicationError } from "@aster/api-contracts";
import { isDeepStrictEqual } from "node:util";
import { goalAdmission } from "./admission.js";
import { GoalCommand, GoalControl, GoalReadyReply, GoalRequestData } from "./protocol.js";
export { GoalCommand, GoalCommandReply, GoalDeliveryReply, GoalReadyReply } from "./protocol.js";

const GoalMailbox = Schema.Union([
  GoalCommand,
  GoalControl,
  Schema.TaggedStruct("RunNext", {}),
  Schema.TaggedStruct("TurnSettled", {
    turnId: Schema.String,
    resultId: Schema.String,
    generation: Schema.String,
    through: Schema.optional(Schema.Number),
    result: Schema.Union([
      Schema.TaggedStruct("Success", { value: StoredGoalPlan }),
      Schema.TaggedStruct("Failure", { error: Schema.instanceOf(Error) }),
    ]),
  }),
  Schema.TaggedStruct("SignalDeliverySettled", {
    requestId: Schema.String,
    generation: Schema.String,
    result: Schema.Union([
      Schema.TaggedStruct("Success", { value: CommandReceipt }),
      Schema.TaggedStruct("Failure", { error: ApplicationError }),
    ]),
  }),
  Schema.TaggedStruct("RecoverySettled", {
    generation: Schema.String,
    result: Schema.Union([
      Schema.TaggedStruct("Success", { value: Schema.Array(ReplyTo<unknown>()) }),
      Schema.TaggedStruct("Failure", { error: Schema.instanceOf(Error) }),
    ]),
  }),
]);
export type GoalMailbox = typeof GoalMailbox.Type;
export interface GoalMessage {
  readonly type: "user" | "assistant" | "execution" | "error";
  readonly at: string;
  readonly text: string;
  readonly references: readonly string[];
}

export class GoalActor extends ContextActor.Service<
  GoalActor,
  GoalSignals | GoalSettings | GoalHistoryStore | TaskExecutionServices
>()("goals/Actor", {
  command: GoalMailbox,
  context: defineContext({
    identity: "Ongoing work goal",
    state: GoalState,
    message: Schema.Unknown,
  }),
}) {
  static readonly layer = Layer.effect(
    GoalActor,
    Effect.gen(function* () {
      const registry = yield* ContextRegistry;
      const settings = yield* GoalSettings;
      const signals = yield* GoalSignals;
      const history = yield* GoalHistoryStore;
      const reasoner = yield* makeGoalReasoner(settings.reasoning!.model, yield* MemoryRecall, {
        ...settings.reasoning,
        queries: Option.getOrUndefined(yield* Effect.serviceOption(ContextQueries)),
      });
      const agents = yield* ExternalAgents;
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
        settings.reasoning?.contextTokens ?? 200000,
      );
      const { current, state, save, append, event } = working;
      const behaviorGeneration = randomUUID();
      let recoveryError: ApplicationError | undefined;
      const readyWaiters: { replyTo: ReplyTo<GoalReadyReply>; stage?: "restored" | "activated" }[] =
        [];
      const ready = Effect.fnUntraced(function* () {
        for (let index = readyWaiters.length - 1; index >= 0; index--) {
          const waiter = readyWaiters[index]!;
          if (
            !recoveryError &&
            !(waiter.stage === "restored" ? schedule.isRecovered() : schedule.isReady())
          )
            continue;
          readyWaiters.splice(index, 1);
          yield* waiter.replyTo.tell(
            recoveryError ? { _tag: "Failed", error: recoveryError } : { _tag: "Ready" },
          );
        }
      });
      const inputs = goalInputs(working, history);
      const signalOutbox = goalSignalOutbox(working, {
        ...signals,
        applySignal:
          signals.applySignal &&
          ((input, subscriber) =>
            signals.applySignal!(input, subscriber).pipe(signalOperations.withPermit)),
      });
      let signalsRecovered = false;
      const acceptInput = goalAdmission(registry, working, history);
      const active = () => state().status === "active";
      const execution = goalTaskExecution(
        registry,
        { current, state, save, append, event },
        () => definition,
        () => path,
      );
      const deactivate = Effect.fn("Goal.deactivate")(function* (
        context: import("./task-execution.js").GoalActorContext,
      ) {
        const operation = state().deactivation;
        if (!operation || operation.status === "delivered") return;
        yield* save({ deactivation: { ...operation, status: "sending" } });
        yield* context.pipeToSelf(
          signals.deactivate(definition.slug, context.self).pipe(
            signalOperations.withPermit,
            Effect.as({ requestId: operation.requestId, revision: current().revision ?? 0 }),
            Effect.mapError(
              (error) => new ApplicationError({ kind: "unavailable", message: error.message }),
            ),
          ),
          (result) => ({
            _tag: "SignalDeliverySettled",
            requestId: operation.requestId,
            generation: behaviorGeneration,
            result,
          }),
        );
      });
      return GoalActor.of({
        started: (context) =>
          Effect.gen(function* () {
            const slug = context.path.split("/").at(-1)!;
            definition = settings.definitions.find((g) => g.slug === slug)!;
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
                      activated: false,
                      summary: "Ready to begin",
                      progress: "Ready to begin",
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
            // Refresh display metadata on restart without replacing durable Goal progress.
            yield* save({ title: definition.title ?? definition.description });
            yield* inputs.project();
            yield* execution.recover(context);
            // Root-scoped activation survives child supervision but starts closed on process boot.
            const activation = context.metadata.goalActivation as
              Deferred.Deferred<void> | undefined;
            if (activation)
              yield* context.pipeToSelf(Deferred.await(activation), () => ({ _tag: "Activate" }));
            yield* context.pipeToSelf(
              signals.reconcile(slug, context.self).pipe(
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
              (result) => ({ _tag: "RecoverySettled", generation: behaviorGeneration, result }),
            );
          }),
        receive: (command, context) =>
          Match.value(command).pipe(
            Match.tag("SubmitInput", "End", "RetryTurn", "RetrySignalDelivery", (command) =>
              Effect.gen(function* () {
                const request = Schema.decodeUnknownSync(GoalRequestData)(command);
                const previous = state().requests?.find(
                  (item) => item.request.requestId === request.requestId,
                );
                if (previous) {
                  return yield* command.replyTo.tell(
                    isDeepStrictEqual(previous.request, request)
                      ? { _tag: "Accepted", receipt: previous.receipt }
                      : {
                          _tag: "Rejected",
                          error: new ApplicationError({
                            kind: "conflict",
                            message: "Goal request identity belongs to another payload",
                          }),
                        },
                  );
                }
                const receipt = {
                  requestId: request.requestId,
                  revision: (current().revision ?? 0) + 1,
                };
                const admission = { request, receipt };
                const requests = [...(state().requests ?? []), admission];
                const result = yield* Match.value(request).pipe(
                  Match.tag("SubmitInput", () => acceptInput(admission)),
                  Match.tag("RetrySignalDelivery", (request) =>
                    Effect.gen(function* () {
                      if (request.requestId !== request.input.requestId)
                        return yield* new ApplicationError({
                          kind: "invalid-input",
                          message: "Signal retry identity mismatch",
                        });
                      return yield* signalOutbox.retry(request.input, admission);
                    }),
                  ),
                  Match.tag("RetryTurn", (request) =>
                    Effect.gen(function* () {
                      const failed = state().evaluations?.find(
                        (item) => item.evaluationId === request.turnId,
                      );
                      if (
                        !active() ||
                        state().pendingRequestId ||
                        state().retryTurnId ||
                        failed?.status !== "failed" ||
                        !failed.inputIds?.length ||
                        !failed.inputIds.every((id) =>
                          state().inputs?.some((input) => input.inputId === id),
                        )
                      )
                        return yield* new ApplicationError({
                          kind: "conflict",
                          message:
                            "Only a definitively failed idle Goal turn can be retried; unknown outcomes need reconciliation",
                        });
                      yield* save({
                        requests,
                        retryTurnId: request.turnId,
                        pendingEvaluation: true,
                        causal: { rootRequestId: request.requestId, remainingAgentTurns: 4 },
                      }).pipe(Effect.orDie);
                      return receipt;
                    }),
                  ),
                  Match.tag("End", () =>
                    Effect.gen(function* () {
                      const observedAt = DateTime.formatIso(yield* DateTime.now);
                      yield* save({
                        requests,
                        status: "completed",
                        completionOrigin: "user",
                        pendingEvaluation: false,
                        nextStep: undefined,
                        retryTurnId: undefined,
                        deactivation: state().deactivation ?? {
                          requestId: request.requestId,
                          status: "pending",
                        },
                        evaluations: state().evaluations?.map((evaluation): GoalEvaluationRecord =>
                          evaluation.status === "running" || evaluation.status === "pending"
                            ? {
                                ...evaluationIdentity(evaluation),
                                status: "reconciliation_required",
                                error:
                                  "Goal ended before result application; external outcome is not confirmed cancelled",
                                observedAt,
                              }
                            : evaluation,
                        ),
                      }).pipe(Effect.orDie);
                      // Closure and uncertain handoff survive even if interruption or acknowledgement is lost.
                      yield* schedule.cancel();
                      return receipt;
                    }),
                  ),
                  Match.exhaustive,
                  Effect.result,
                );
                if (result._tag === "Failure")
                  return yield* command.replyTo.tell({ _tag: "Rejected", error: result.failure });
                yield* command.replyTo.tell({ _tag: "Accepted", receipt: result.success });
                if (request._tag === "End") {
                  yield* execution.cancelPending(context);
                  yield* deactivate(context);
                } else if (request._tag === "RetrySignalDelivery")
                  yield* signalOutbox.dispatch(context);
                else if (state().pendingEvaluation) yield* schedule.enqueue(context);
              }),
            ),
            Match.tag("Activate", () =>
              Effect.gen(function* () {
                if (!state().activated) {
                  // An absent marker identifies legacy state; never invent a startup for an existing pursuit.
                  if (active() && state().activated === false) {
                    yield* inputs.accept({ _tag: "GoalStarted", pursuit: "initial" }, "initial", {
                      activated: true,
                      pendingEvaluation: true,
                      causal: {
                        rootRequestId: `goal:${definition.slug}:initial`,
                        remainingAgentTurns: 4,
                      },
                    });
                  } else yield* save({ activated: true });
                }
                yield* schedule.activate;
                yield* ready();
                if (state().pendingEvaluation || state().pendingRequestId)
                  yield* schedule.enqueue(context);
              }),
            ),
            Match.tag("AwaitReady", (command) =>
              Effect.gen(function* () {
                readyWaiters.push(command);
                yield* ready();
              }),
            ),
            Match.tag("SignalDeliverySettled", (command) =>
              Effect.gen(function* () {
                if (
                  command.requestId === state().deactivation?.requestId &&
                  command.generation === behaviorGeneration
                ) {
                  yield* save({
                    deactivation: {
                      requestId: command.requestId,
                      status: command.result._tag === "Success" ? "delivered" : "unknown",
                      ...(command.result._tag === "Failure"
                        ? { error: command.result.error.message }
                        : {}),
                    },
                  });
                } else yield* signalOutbox.delivered(command);
              }),
            ),
            Match.tag("RunNext", () =>
              Effect.gen(function* () {
                yield* schedule.dequeue;
                if (!active()) return;
                const started = yield* schedule.start();
                if (!started) {
                  yield* save({ pendingEvaluation: true });
                  return;
                }
                const { generation: currentGeneration, cancelled } = started;
                let handoff = state().pendingHandoff;
                const reconcile = handoff !== undefined || state().pendingRequestId !== undefined;
                if (
                  state().pendingRequestId &&
                  (!handoff || handoff.requestId !== state().pendingRequestId)
                ) {
                  // Only the old identity is trustworthy. Replay-only inspection below
                  // cannot use current state to submit or resume this legacy request.
                  handoff = {
                    requestId: state().pendingRequestId!,
                    reason: "Inspect retained legacy session result",
                    through:
                      state().evaluations?.find(
                        (item) => item.evaluationId === state().pendingRequestId,
                      )?.historyThrough ??
                      state().agentThrough ??
                      0,
                  };
                }
                if (!handoff) {
                  const requestId = randomUUID();
                  const previousEvaluation = state().evaluations?.find(
                    (item) => item.evaluationId === state().retryTurnId,
                  );
                  const assigned = new Set(
                    state().evaluations?.flatMap((evaluation) => evaluation.inputIds ?? []),
                  );
                  const retrying = previousEvaluation?.status === "failed";
                  let selected = (state().inputs ?? []).filter((input) =>
                    retrying
                      ? previousEvaluation.inputIds?.includes(input.inputId)
                      : !assigned.has(input.inputId),
                  );
                  if (!selected.length) {
                    yield* schedule.finish(currentGeneration);
                    yield* save({ pendingEvaluation: false });
                    return;
                  }
                  if (!retrying) {
                    const inputBudget = Math.max(
                      1,
                      Math.floor(
                        ((settings.reasoning?.contextTokens ?? 200000) -
                          (settings.reasoning?.reserveTokens ?? 8192)) /
                          3,
                      ),
                    );
                    // Reserve space for an actual user instruction so exhausted older
                    // continuations cannot starve it or borrow authority without admitting it.
                    const instruction = selected.find(
                      (input) => input.payload._tag === "UserInput",
                    );
                    let size = instruction ? contextSize(inputMessage(instruction)) : 0;
                    let admitted = instruction ? 1 : 0;
                    selected = selected.filter((input) => {
                      if (input === instruction) return true;
                      const cost = contextSize(inputMessage(input));
                      if (admitted > 0 && size + cost > inputBudget) return false;
                      size += cost;
                      admitted++;
                      return true;
                    });
                  }
                  const retryRequest =
                    retrying &&
                    state().requests?.findLast(
                      (item) =>
                        item.request._tag === "RetryTurn" &&
                        item.request.turnId === previousEvaluation.evaluationId,
                    );
                  const causal = (retrying && retryRequest
                    ? { rootRequestId: retryRequest.request.requestId, remainingAgentTurns: 4 }
                    : (selected.find((input) => input.payload._tag === "UserInput")?.causal ??
                      selected[0]?.causal)) ??
                    state().causal ?? {
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

                  handoff = {
                    causal,
                    requestId,
                    reason: "Pursue the Goal using the admitted inputs",
                    through: yield* history.count(definition.slug).pipe(Effect.orDie),
                  };
                  handoff = {
                    ...handoff,
                    causal,
                    through: selected.at(-1)!.historySequence!,
                    input: {
                      inputs: selected,
                      goal: definition,
                      current: registry.project(current()),
                      contexts: registry.publicSnapshot(),
                      signals: signals.signals(definition.slug),
                      historyAfter: Math.min(
                        selected[0]!.historySequence! - 1,
                        selected.at(-1)!.historySequence!,
                      ),
                      historyThrough: selected.at(-1)!.historySequence!,
                    },
                  };
                  const evaluation: GoalEvaluationRecord = {
                    evaluationId: requestId,
                    inputIds: selected.map((input) => input.inputId),
                    ...(retrying ? { retryOf: previousEvaluation.evaluationId } : {}),
                    reason: handoff.reason,
                    historyThrough: handoff.through,
                    startedAt: DateTime.formatIso(yield* DateTime.now),
                    status: "running",
                  };
                  yield* save({
                    evaluations: [...(state().evaluations ?? []), evaluation],
                    causal: handoff.causal,
                    retryTurnId: undefined,
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
                const turnId = handoff.requestId;
                yield* context.pipeToSelf(
                  evaluateGoal({
                    reasoner,
                    history,
                    reconcile,
                    // These values are only tool/decoder context for legacy result reads.
                    // replayOnly forbids turning this fallback into a new session admission.
                    replayOnly: !handoff.input,
                    input: handoff.input ?? {
                      goal: definition,
                      current: registry.project(current()),
                      contexts: {},
                      signals: [],
                      inputs: [],
                      historyAfter: handoff.through,
                      historyThrough: handoff.through,
                    },
                    reason: handoff.reason,
                    requestId: turnId,
                  }).pipe(Effect.raceFirst(cancelled)),
                  (result) => ({
                    _tag: "TurnSettled",
                    turnId,
                    resultId: turnId,
                    result:
                      result._tag === "Success"
                        ? { _tag: "Success", value: result.value.plan }
                        : result,
                    through: result._tag === "Success" ? result.value.through : undefined,
                    generation: currentGeneration,
                  }),
                );
              }),
            ),
            Match.tag("TurnSettled", (command) =>
              Effect.gen(function* () {
                if (
                  command.turnId !== state().pendingRequestId ||
                  command.resultId !== command.turnId
                )
                  return;
                if (!(yield* schedule.finish(command.generation))) return;
                const evaluation = state().evaluations?.find(
                  (item) => item.evaluationId === state().pendingRequestId,
                );
                if (command.result._tag === "Failure") {
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
                  yield* event(`Agent turn outcome: ${command.result.error.message}`);
                } else if (active()) {
                  const original = command.result.value;
                  const plan: GoalPlan =
                    "nextStep" in original
                      ? original
                      : {
                          version: 2,
                          turnId: command.turnId,
                          resultId: command.resultId,
                          disposition: original.disposition ?? "advance",
                          progress: original.progress,
                          evidence: original.evidence,
                          taskChanges: original.taskChanges,
                          signalChanges: original.signalChanges,
                          nextStep:
                            original.completed &&
                            definition.completionCriteria &&
                            original.evidence.length
                              ? { _tag: "Complete", evidence: original.evidence }
                              : { _tag: "WaitForEvent", references: [path] },
                        };
                  const completed =
                    plan.nextStep._tag === "Complete" && !!definition.completionCriteria;
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
                    if (plan.turnId !== command.turnId || plan.resultId !== command.resultId)
                      return yield* new GoalToolError({
                        message: "Result belongs to another turn",
                      });
                    if (plan.nextStep._tag === "Complete" && !definition.completionCriteria)
                      return yield* new GoalToolError({
                        message: "Ongoing Goals cannot complete without user End",
                      });
                    if (
                      plan.nextStep._tag === "Continue" &&
                      plan.nextStep.previousResultId !== plan.resultId
                    )
                      return yield* new GoalToolError({
                        message: "Continuation must reference its preceding result",
                      });
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
                      result: original,
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
                      yield* schedule.enqueue(context);
                    return;
                  }
                  const resultRecord: GoalEvaluationRecord | undefined = evaluation && {
                    ...evaluationIdentity(evaluation),
                    status: proposals.success.signals.length ? "partially_applied" : "completed",
                    resultId: evaluation.evaluationId,
                    result: original,
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
                  const continuation =
                    plan.nextStep._tag === "Continue"
                      ? newGoalInput(
                          state(),
                          { ...plan.nextStep, _tag: "Continuation" },
                          plan.resultId,
                          appliedAt,
                        )
                      : undefined;
                  const cause = state().pendingHandoff?.causal;
                  const continuationCause = cause && {
                    ...cause,
                    remainingAgentTurns: Math.max(0, cause.remainingAgentTurns - 1),
                  };
                  yield* save({
                    ...(continuation
                      ? {
                          inputs: [
                            ...(state().inputs ?? []),
                            { ...continuation, causal: continuationCause },
                          ],
                          causal: continuationCause,
                        }
                      : {}),
                    nextStep: plan.nextStep,
                    ...(completed
                      ? {
                          completionOrigin: "criteria",
                          deactivation: { requestId: `end:${plan.resultId}`, status: "pending" },
                        }
                      : {}),
                    tasks: proposals.success.tasks,
                    signalOutbox: [...(state().signalOutbox ?? []), ...proposals.success.signals],
                    ...(resultRecord
                      ? {
                          evaluations: state().evaluations!.map((item) =>
                            item.evaluationId === resultRecord.evaluationId ? resultRecord : item,
                          ),
                        }
                      : {}),
                    ...(command.through !== undefined ? { agentThrough: command.through } : {}),
                    pendingRequestId: undefined,
                    pendingHandoff: undefined,
                    lastError: undefined,
                    summary: plan.progress,
                    progress: plan.progress,
                    status: completed ? "completed" : "active",
                    pendingEvaluation:
                      !!continuation || state().pendingEvaluation || schedule.hasPending(),
                  });
                  yield* inputs.project();
                  yield* signalOutbox.dispatch(context);
                  // Reservations and Task revisions are durable before any Run is created or cancelled.
                  if (plan.taskChanges?.length) yield* execution.recover(context, false);
                  yield* event(
                    `Evaluation conclusions: ${plan.progress}\nEvidence: ${plan.evidence.join(", ")}`,
                  );
                  if (completed) yield* execution.cancelPending(context);
                  if (completed) yield* deactivate(context);
                }
                if (
                  (schedule.hasPending() || state().pendingEvaluation) &&
                  !state().pendingRequestId &&
                  active()
                )
                  yield* schedule.enqueue(context);
                return;
              }),
            ),
            Match.tag("RecoverySettled", (command) =>
              Effect.gen(function* () {
                if (command.generation !== behaviorGeneration) return;
                if (command.result._tag === "Failure") {
                  recoveryError = new ApplicationError({
                    kind: "unavailable",
                    message: command.result.error.message,
                  });
                  yield* save({
                    lastError: `Goal recovery failed: ${command.result.error.message}`,
                  });
                  yield* ready();
                  return;
                }
                if (!signalsRecovered) {
                  signalsRecovered = true;
                  yield* signalOutbox.recover(context);
                }
                yield* schedule.reconciled;
                yield* ready();
                if (state().deactivation && state().deactivation?.status !== "delivered")
                  yield* deactivate(context);
                if (state().pendingEvaluation || state().pendingRequestId)
                  yield* schedule.enqueue(context);
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
  Schema.TaggedStruct("AwaitReady", {
    stage: Schema.optional(Schema.Literals(["restored", "activated"])),
    replyTo: ReplyTo<GoalReadyReply>(),
  }),
  Schema.TaggedStruct("Route", { slug: Schema.String, command: GoalCommand }),
  Schema.TaggedStruct("Initialize", {}),
]);
const GoalsRootMailbox = Schema.Union([
  GoalsRootCommand,
  Schema.TaggedStruct("ReadinessSettled", {
    replyTo: ReplyTo<GoalReadyReply>(),
    result: Schema.Union([
      Schema.TaggedStruct("Success", { value: GoalReadyReply }),
      Schema.TaggedStruct("Failure", { error: ApplicationError }),
    ]),
  }),
]);
export type GoalsRootCommand = typeof GoalsRootCommand.Type;
export class GoalsRootActor extends Actor.Service<
  GoalsRootActor,
  ContextRegistry | GoalSignals | GoalSettings | GoalHistoryStore | TaskExecutionServices
>()("goals/RootActor", { command: GoalsRootMailbox }) {
  static readonly layer = Layer.effect(
    GoalsRootActor,
    Effect.gen(function* () {
      const settings = yield* GoalSettings;
      const activation = yield* Deferred.make<void>();
      return GoalsRootActor.of({
        started: (context) =>
          Effect.gen(function* () {
            for (const goal of settings.definitions)
              if (!(yield* context.child(goal.slug)))
                yield* context.spawn(goal.slug, GoalActor, {
                  metadata: { goalActivation: activation },
                });
          }),
        receive: (command, context) =>
          Effect.gen(function* () {
            if (command._tag === "Route") {
              const child = settings.definitions.some((goal) => goal.slug === command.slug)
                ? yield* context.child(command.slug)
                : undefined;
              if (child) yield* (child as ActorRef<GoalMailbox>).tell(command.command);
              else
                yield* command.command.replyTo.tell({
                  _tag: "Rejected",
                  error: new ApplicationError({
                    kind: "not-found",
                    message: "Goal Actor unavailable",
                  }),
                });
              return;
            }
            if (command._tag === "ReadinessSettled") {
              yield* command.replyTo.tell(
                command.result._tag === "Success"
                  ? command.result.value
                  : { _tag: "Failed", error: command.result.error },
              );
              return;
            }
            const children = (yield* context.children()) as readonly ActorRef<GoalMailbox>[];
            if (command._tag === "Initialize") {
              yield* Deferred.succeed(activation, undefined);
              for (const goal of children) yield* goal.tell({ _tag: "Activate" });
              return;
            }
            // Child recovery must never hold the routing mailbox or block Initialize.
            yield* context.pipeToSelf(
              Effect.gen(function* () {
                for (const goal of children) {
                  const result = yield* goal.ask<GoalReadyReply>((replyTo) => ({
                    _tag: "AwaitReady",
                    stage: command.stage,
                    replyTo,
                  }));
                  if (result._tag === "Failed") return result;
                }
                return { _tag: "Ready" } as const;
              }).pipe(
                Effect.mapError(
                  (error) =>
                    new ApplicationError({
                      kind: "unavailable",
                      message: error.message,
                    }),
                ),
              ),
              (result) => ({ _tag: "ReadinessSettled", replyTo: command.replyTo, result }),
            );
          }),
      });
    }),
  );
}
