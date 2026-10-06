import { isDeepStrictEqual } from "node:util";
import { createHash, randomUUID } from "node:crypto";
import {
  ApplicationError,
  CommandReceipt,
  TaskDeliveryInput,
  FollowupTaskInput,
  PreparedTask,
} from "@aster/api-contracts";
import { AgentConversations, AgentRunner, AgentError } from "@aster/agent";
import { Clock, Effect, Match, Layer, Schema, Schedule, Option } from "effect";
import { type ActorContext, type ActorRef } from "@aster/actor";
import { ContextActor, contextPath } from "../context/actor.js";
import { defineContext } from "../context/definition.js";
import { ContextRegistry } from "../context/registry.js";
import { ContextQueries } from "../context/queries.js";
import { MemoryRecall } from "../memory/contracts.js";
import { GoalSettings } from "../config/settings.js";
import { taskPathFor, sourceTask, delegateInput } from "./admission.js";
import { ExternalAgents, taskPrompt, DEFAULT_EXECUTOR_PROMPT } from "./model.js";
import { TaskState, TaskInputRef } from "./state.js";
import { ExternalAgentError } from "./errors.js";
import { executeTask } from "./execution.js";
import { makeTaskWriteback, planWriteback } from "./writeback.js";
import { sendApproval, approvalEntries } from "../approvals/actor.js";
import type { GoalCommand, GoalCommandReply } from "../goals/actors.js";

import { TaskCommand } from "./protocol.js";
export type TaskServices =
  ExternalAgents | AgentConversations | AgentRunner | GoalSettings | ContextQueries | MemoryRecall;
const TaskOutcome = Schema.Struct({
  text: Schema.String,
  status: TaskState.fields.status,
  inputs: Schema.Array(TaskInputRef),
});
type Owner = ActorContext<TaskCommand, TaskServices | ContextRegistry>;

/** One persistent work owner; Pi owns messages and Agent execution mechanics. */
export class TaskActor extends ContextActor.Service<TaskActor, TaskServices>()("tasks/Actor", {
  command: TaskCommand,
  context: defineContext({ state: TaskState, message: Schema.Never }),
}) {
  static readonly layer = Layer.effect(
    TaskActor,
    Effect.gen(function* () {
      const registry = yield* ContextRegistry;
      const messages = yield* AgentConversations;
      const agents = yield* ExternalAgents;
      const settings = yield* GoalSettings;
      const runner = yield* AgentRunner;
      const memory = yield* MemoryRecall;
      const queries = yield* ContextQueries;
      let path = "";
      let generation = randomUUID();
      let busy = false;
      const state = () => Schema.decodeUnknownSync(TaskState)(registry.get(path)!.state);
      const save = Effect.fn("Task.save")(function* (patch: Partial<TaskState>) {
        const current = registry.get(path)!;
        yield* registry
          .commit(
            { ...current, state: { ...state(), ...patch }, messages: [] },
            { expectedRevision: current.revision ?? 0 },
          )
          .pipe(Effect.orDie);
      });
      const initial = () =>
        messages.get(path, state().inputs[0]!.entryId).pipe(
          Effect.flatMap((entry) => Schema.decodeUnknownEffect(TaskDeliveryInput)(entry.data)),
          Effect.orDie,
        );
      const preparedTask = Effect.fn("Task.prepared")(function* () {
        const admitted = yield* initial();
        const policy =
          state().admission.agent === "internal" ? "" : `${state().executorPrompt}\n\n`;
        return {
          instructions: policy + admitted.task.instructions,
          input: [
            ...admitted.task.input,
            ...(admitted.evidence
              ? [{ content: JSON.stringify(admitted.evidence), sources: [admitted.evidence.path] }]
              : []),
          ],
        } satisfies PreparedTask;
      });
      const followup = (input: TaskInputRef) =>
        messages.get(path, input.entryId).pipe(
          Effect.flatMap((entry) => Schema.decodeUnknownEffect(FollowupTaskInput)(entry.data)),
          Effect.orDie,
        );
      const mark = (requestId: string, status: TaskInputRef["status"]) =>
        save({
          inputs: state().inputs.map((input) =>
            input.requestId === requestId ? { ...input, status } : input,
          ),
        });
      const admitInputs = (inputs: readonly TaskInputRef[]) =>
        save({
          inputs,
          ...(["completed", "failed"].includes(state().status) &&
          inputs.some((input) => input.status === "pending")
            ? { status: "ready" as const }
            : {}),
        });
      const notify = Effect.fn("Task.notify")(function* (owner: Owner, text: string) {
        const saved = state();
        const requestId = createHash("sha256")
          .update(
            JSON.stringify([
              path,
              saved.status,
              saved.inputs.map((input) => input.requestId),
              text,
            ]),
          )
          .digest("hex");
        yield* owner.pipeToSelf(
          Effect.gen(function* () {
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
                  text,
                  status: saved.status,
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
          ),
          (result) => ({ _tag: "FeedbackDelivered", result }),
        );
      });
      const writeback = yield* makeTaskWriteback({ path: () => path, state });
      const finish = Effect.fn("Task.finish")(function* (
        owner: Owner,
        status: TaskState["status"],
        text: string,
        inputs: readonly TaskInputRef[] = state().inputs,
      ) {
        const entry = yield* messages
          .append(
            path,
            `outcome:${createHash("sha256")
              .update(
                JSON.stringify([
                  inputs.map((input) => [input.requestId, input.status]),
                  status,
                  text,
                ]),
              )
              .digest("hex")}`,
            "task.result",
            { text, status, inputs },
          )
          .pipe(Effect.orDie);
        const next = { ...state(), status, inputs, outcomeEntryId: entry.id };
        const publication = planWriteback(
          path,
          next,
          text,
          new Date(yield* Clock.currentTimeMillis).toISOString(),
        );
        yield* save({
          status,
          inputs,
          outcomeEntryId: entry.id,
          ...(publication ? { writeback: publication } : {}),
        });
        yield* notify(owner, text);
        yield* writeback.recover(owner);
      });
      const poll = Effect.fn("Task.observe")(function* (owner: Owner, delayed = false) {
        const saved = state();
        if (!saved.session) return;
        const token = generation;
        yield* owner.pipeToSelf(
          agents[saved.admission.agent]!.wait(saved.session).pipe(
            Effect.delay(delayed ? "1 second" : "0 seconds"),
          ),
          (result) => ({ _tag: "ExternalStatus", generation: token, result }),
        );
      });
      const confirm = Effect.fn("Task.confirm")(function* (owner: Owner) {
        const prepared = yield* preparedTask();
        yield* sendApproval(owner, {
          _tag: "Enqueue",
          entry: {
            id: `${path}:confirm`,
            target: owner.path,
            contextPath: path,
            kind: "confirmation",
            status: "pending",
            request: {
              id: `${path}:confirm`,
              kind: "approval",
              prompt: taskPrompt(prepared),
            },
          },
        });
      });
      const submitExternal = Effect.fn("Task.submitExternal")(function* (
        owner: Owner,
        inputId: string,
        prepared: PreparedTask,
      ) {
        const saved = state();
        generation = randomUUID();
        const token = generation;
        yield* save({
          status: "submitting",
          inputs: state().inputs.map((input) =>
            input.requestId === inputId ? { ...input, status: "sending" } : input,
          ),
        });
        const agent = agents[saved.admission.agent]!;
        const execution = saved.session
          ? agent.followUp(saved.session, { requestId: inputId, text: prepared.instructions })
          : agent.submit(prepared, { requestId: path });
        yield* owner.pipeToSelf(execution, (result) => ({
          _tag: "ExternalSubmitted",
          generation: token,
          inputId,
          result,
        }));
      });
      const advance = Effect.fn("Task.advance")(function* (owner: Owner) {
        if (
          busy ||
          ["awaiting-confirmation", "rejected", "cancelled", "uncertain"].includes(state().status)
        )
          return;
        const input = state().inputs.find(
          (input) => input.status === "pending" || input.status === "sending",
        );
        if (!input) return;
        const saved = state();
        if (
          (registry.get(saved.admission.replyTo)?.state as { status?: string } | undefined)
            ?.status !== "active"
        ) {
          return yield* finish(owner, "cancelled", "The owning Goal has ended");
        }
        const prepared =
          input.requestId === saved.inputs[0]!.requestId
            ? yield* preparedTask()
            : { instructions: (yield* followup(input)).text, input: [] };
        busy = true;
        if (saved.admission.agent === "internal") {
          generation = randomUUID();
          const token = generation;
          yield* save({
            status: "running",
            inputs: state().inputs.map((item) =>
              item.requestId === input.requestId ? { ...item, status: "sending" } : item,
            ),
          });
          yield* owner.pipeToSelf(
            executeTask({
              path,
              requestId: input.requestId,
              model: settings.reasoning!.model,
              task: prepared,
              reconcile: input.status === "sending",
            }).pipe(
              Effect.provideService(AgentRunner, runner),
              Effect.provideService(ContextRegistry, registry),
              Effect.provideService(ContextQueries, queries),
              Effect.provideService(MemoryRecall, memory),
            ),
            (result) => ({
              _tag: "InternalSettled",
              generation: token,
              inputId: input.requestId,
              result,
            }),
          );
        } else {
          // Never repeat an external submission whose outcome was not committed.
          if (input.status === "sending") {
            busy = false;
            yield* finish(
              owner,
              "uncertain",
              "Execution input was interrupted during delivery; reconcile its original identity before continuing.",
            );
            return;
          }
          yield* submitExternal(owner, input.requestId, prepared);
        }
      });
      const settleResumption = (
        requestId: string,
        status: "pending" | "resuming" | "done" | "unknown",
      ) =>
        save({
          resumptions: state().resumptions?.map((item) =>
            item.input.requestId === requestId ? { ...item, status } : item,
          ),
        });
      const reconcile = Effect.fn("Task.reconcile")(function* (owner: Owner, requestId: string) {
        const saved = state();
        if (saved.admission.agent === "internal") {
          const input = saved.inputs.find((input) => ["unknown", "sending"].includes(input.status));
          if (!input) {
            yield* settleResumption(requestId, "done");
            return;
          }
          yield* mark(input.requestId, "sending");
          yield* save({ status: "ready" });
          return yield* advance(owner);
        }
        // An older provider handle cannot prove that a later instruction was accepted.
        if (
          saved.inputs.some(
            (input) =>
              input.requestId !== saved.inputs[0]!.requestId &&
              ["sending", "unknown"].includes(input.status),
          )
        ) {
          yield* settleResumption(requestId, "unknown");
          return yield* finish(
            owner,
            "uncertain",
            "Follow-up delivery cannot be verified from the preceding execution. Reconcile it with the external executor before continuing.",
          );
        }
        const agent = agents[saved.admission.agent];
        if (!agent) {
          yield* settleResumption(requestId, "unknown");
          return;
        }
        if (saved.session)
          return yield* owner.pipeToSelf(agent.status(saved.session), (result) => ({
            _tag: "ResumeObserved",
            requestId,
            result,
          }));
        if (!agent.lookupSubmission) {
          yield* settleResumption(requestId, "unknown");
          return;
        }
        const prepared = yield* preparedTask();
        yield* owner.pipeToSelf(
          agent.lookupSubmission(prepared, { requestId: path }),
          (result) => ({ _tag: "SubmissionLocated", requestId, result }),
        );
      });
      const recover = Effect.fn("Task.recover")(function* (owner: Owner) {
        if (busy) return;
        const entries = yield* messages.read(path).pipe(Effect.orDie);
        const inputs = [...state().inputs];
        for (const entry of entries) {
          if (
            entry.kind !== "task.input" ||
            inputs.some((input) => input.requestId === entry.requestId)
          )
            continue;
          inputs.push({
            requestId: entry.requestId,
            entryId: entry.id,
            receipt: Schema.decodeUnknownSync(Schema.Struct({ receipt: CommandReceipt }))(
              entry.data,
            ).receipt,
            status: "pending",
          });
        }
        for (const entry of entries) {
          if (entry.kind !== "task.steer") continue;
          const data = Schema.decodeUnknownSync(Schema.Struct({ requestId: Schema.String }))(
            entry.data,
          );
          const index = inputs.findIndex(
            (input) => input.requestId === data.requestId && input.status === "pending",
          );
          if (index >= 0) inputs[index] = { ...inputs[index]!, status: "accepted" };
        }
        if (!isDeepStrictEqual(inputs, state().inputs)) yield* admitInputs(inputs);
        if (state().responses?.some((response) => response.status === "sending")) {
          yield* save({
            responses: state().responses?.map((response) =>
              response.status === "sending" ? { ...response, status: "unknown" } : response,
            ),
          });
          return yield* finish(
            owner,
            "uncertain",
            "The external answer's delivery outcome is unknown; it will not be sent again automatically.",
          );
        }
        for (const response of state().responses ?? [])
          if (response.status === "sent")
            yield* sendApproval(owner, {
              _tag: "Acknowledge",
              id: response.requestId,
              target: owner.path,
            });
        const lastOutcome = entries.findLast((entry) => entry.kind === "task.result");
        if (lastOutcome && lastOutcome.id !== state().outcomeEntryId) {
          const outcome = Schema.decodeUnknownSync(TaskOutcome)(lastOutcome.data);
          if (
            isDeepStrictEqual(
              outcome.inputs.map((input) => input.requestId),
              state().inputs.map((input) => input.requestId),
            )
          ) {
            const next = {
              ...state(),
              status: outcome.status,
              inputs: outcome.inputs,
              outcomeEntryId: lastOutcome.id,
            };
            const publication = planWriteback(path, next, outcome.text, lastOutcome.at);
            yield* save({ ...next, ...(publication ? { writeback: publication } : {}) });
          }
        }
        yield* writeback.recover(owner);
        const resumption = state().resumptions?.find(
          (item) => item.status === "pending" || item.status === "resuming",
        );
        if (resumption?.status === "pending")
          return yield* reconcile(owner, resumption.input.requestId);
        if (resumption?.status === "resuming") {
          yield* settleResumption(resumption.input.requestId, "unknown");
          return yield* finish(owner, "uncertain", "External resumption outcome is unknown");
        }
        if (
          state().admission.agent !== "internal" &&
          state().inputs.some((input) => input.status === "sending")
        ) {
          return yield* finish(
            owner,
            "uncertain",
            "Execution input was interrupted during delivery; its outcome must be reconciled.",
            state().inputs.map((input) =>
              input.status === "sending" ? { ...input, status: "unknown" as const } : input,
            ),
          );
        }
        if (state().status === "awaiting-confirmation") return yield* confirm(owner);
        if (
          state().admission.agent !== "internal" &&
          state().session &&
          ["running", "waiting_input"].includes(state().status)
        ) {
          busy = true;
          return yield* poll(owner);
        }
        if (
          !["completed", "failed", "cancelled", "rejected", "uncertain"].includes(state().status) &&
          state().inputs.some((input) => input.status === "pending" || input.status === "sending")
        )
          return yield* advance(owner);
        const result = state().outcomeEntryId;
        if (result !== undefined) {
          const entry = yield* messages.get(path, result).pipe(Effect.orDie);
          const { text } = Schema.decodeUnknownSync(Schema.Struct({ text: Schema.String }))(
            entry.data,
          );
          yield* notify(owner, text);
        }
      });
      return TaskActor.of({
        started: (owner) =>
          Effect.gen(function* () {
            path = contextPath(owner);
            if (registry.get(path)) yield* recover(owner);
          }),
        receive: (command, owner) =>
          Match.value(command).pipe(
            Match.tag("Ready", ({ replyTo }) => replyTo.tell(undefined)),
            Match.tag("Resume", () => recover(owner)),
            Match.tag("FeedbackDelivered", ({ result }) =>
              result._tag === "Failure" ? Effect.logWarning(result.error) : Effect.void,
            ),
            Match.tag("StartTask", ({ input, replyTo }) =>
              Effect.gen(function* () {
                const accepted = yield* Effect.gen(function* () {
                  const decoded = yield* Schema.decodeUnknownEffect(TaskDeliveryInput)(input).pipe(
                    Effect.mapError(
                      () =>
                        new ApplicationError({ kind: "invalid-input", message: "Invalid Task" }),
                    ),
                  );
                  path = contextPath(owner);
                  if (
                    decoded.target !== path ||
                    path !== taskPathFor(decoded.source, decoded.requestId)
                  )
                    return yield* new ApplicationError({
                      kind: "invalid-input",
                      message: "Task identity mismatch",
                    });
                  if (registry.get(path)) {
                    yield* messages
                      .append(path, input.requestId, "task.admission", decoded)
                      .pipe(
                        Effect.mapError(
                          (error) =>
                            new ApplicationError({ kind: error.kind, message: error.message }),
                        ),
                      );
                    return state().inputs[0]!.receipt;
                  }
                  const source = yield* sourceTask(registry, input.source, input.requestId);
                  if (
                    source &&
                    (source.task._tag === "Goal" ||
                      !isDeepStrictEqual(delegateInput(source, source.task), input))
                  )
                    return yield* new ApplicationError({
                      kind: "conflict",
                      message: "Task differs from its Signal occurrence",
                    });
                  if (input.agent !== "internal" && !agents[input.agent])
                    return yield* new ApplicationError({
                      kind: "invalid-input",
                      message: "Task executor is not configured",
                    });
                  if (
                    (registry.get(input.replyTo)?.state as { status?: string } | undefined)
                      ?.status !== "active"
                  )
                    return yield* new ApplicationError({
                      kind: "conflict",
                      message: "Reply Goal is missing or ended",
                    });
                  const entry = yield* messages
                    .append(path, input.requestId, "task.admission", decoded)
                    .pipe(Effect.orDie);
                  const { source: sourcePath, replyTo: replyGoal, agent, causal, action } = decoded;
                  const receipt = { requestId: input.requestId, revision: 1 };
                  yield* registry
                    .commit(
                      {
                        path,
                        description: "Task",
                        messages: [],
                        state: {
                          admission: {
                            source: sourcePath,
                            replyTo: replyGoal,
                            agent,
                            causal,
                            action,
                          },
                          executorPrompt:
                            agents[input.agent]?.executorPrompt ?? DEFAULT_EXECUTOR_PROMPT,
                          status: input.agent === "internal" ? "ready" : "awaiting-confirmation",
                          inputs: [
                            {
                              requestId: input.requestId,
                              entryId: entry.id,
                              receipt,
                              status: "pending",
                            },
                          ],
                        },
                      },
                      { expectedRevision: 0 },
                    )
                    .pipe(Effect.orDie);
                  return receipt;
                }).pipe(Effect.result);
                if (accepted._tag === "Failure")
                  return yield* replyTo.tell({ _tag: "Rejected", error: accepted.failure });
                yield* replyTo.tell({ _tag: "Accepted", receipt: accepted.success });
                if (state().status === "awaiting-confirmation") yield* confirm(owner);
                else yield* advance(owner);
              }),
            ),
            Match.tag("FollowupTask", ({ input, replyTo }) =>
              Effect.gen(function* () {
                const previous = state().inputs.find((item) => item.requestId === input.requestId);
                if (previous) {
                  if (previous.requestId === state().inputs[0]!.requestId)
                    return yield* replyTo.tell({
                      _tag: "Rejected",
                      error: new ApplicationError({
                        kind: "conflict",
                        message: "Input identity belongs to the original Task",
                      }),
                    });
                  const stored = yield* followup(previous);
                  return yield* replyTo.tell(
                    isDeepStrictEqual(stored, input)
                      ? { _tag: "Accepted", receipt: previous.receipt }
                      : {
                          _tag: "Rejected",
                          error: new ApplicationError({
                            kind: "conflict",
                            message: "Input identity belongs to another payload",
                          }),
                        },
                  );
                }
                if (
                  input.target !== path ||
                  input.source !== state().admission.replyTo ||
                  (registry.get(input.source)?.state as { status?: string } | undefined)?.status !==
                    "active" ||
                  ["rejected", "cancelled", "uncertain"].includes(state().status)
                )
                  return yield* replyTo.tell({
                    _tag: "Rejected",
                    error: new ApplicationError({
                      kind: "conflict",
                      message: "Task cannot accept this follow-up",
                    }),
                  });
                const receipt = {
                  requestId: input.requestId,
                  revision: (registry.get(path)!.revision ?? 0) + 1,
                };
                const entry = yield* messages
                  .append(path, input.requestId, "task.input", { ...input, receipt })
                  .pipe(Effect.result);
                if (entry._tag === "Failure")
                  return yield* replyTo.tell({
                    _tag: "Rejected",
                    error: new ApplicationError({
                      kind: entry.failure.kind,
                      message: entry.failure.message,
                    }),
                  });
                yield* admitInputs([
                  ...state().inputs,
                  {
                    requestId: input.requestId,
                    entryId: entry.success.id,
                    receipt,
                    status: "pending",
                  },
                ]);
                yield* replyTo.tell({ _tag: "Accepted", receipt });
                if (busy && state().admission.agent === "internal") {
                  const steered = yield* messages
                    .steer(path, input.requestId, input.text)
                    .pipe(Effect.orDie);
                  if (steered) yield* mark(input.requestId, "accepted");
                } else if (
                  busy &&
                  state().session &&
                  !state().inputs.some((item) => item.status === "sending")
                ) {
                  yield* submitExternal(owner, input.requestId, {
                    instructions: input.text,
                    input: [],
                  });
                } else yield* advance(owner);
              }),
            ),
            Match.tag("InternalSettled", ({ generation: token, inputId, result }) =>
              Effect.gen(function* () {
                if (token !== generation) return;
                busy = false;
                for (const resumption of state().resumptions ?? [])
                  if (resumption.status === "pending")
                    yield* settleResumption(
                      resumption.input.requestId,
                      result._tag === "Success" ? "done" : "unknown",
                    );
                if (result._tag === "Failure") {
                  const known =
                    result.error instanceof AgentError && result.error.outcome === "failed";
                  const inputs = state().inputs.map((input) =>
                    input.requestId === inputId || input.status === "accepted"
                      ? { ...input, status: known ? ("completed" as const) : ("unknown" as const) }
                      : input,
                  );
                  return yield* finish(
                    owner,
                    known ? "failed" : "uncertain",
                    result.error.message,
                    inputs,
                  );
                }
                const inputs = state().inputs.map((input) =>
                  input.requestId === inputId || input.status === "accepted"
                    ? { ...input, status: "completed" as const }
                    : input,
                );
                if (inputs.some((input) => input.status === "pending")) {
                  yield* save({ inputs });
                  return yield* advance(owner);
                }
                yield* finish(owner, "completed", result.value, inputs);
              }),
            ),
            Match.tag("ExternalSubmitted", ({ generation: token, inputId, result }) =>
              Effect.gen(function* () {
                if (token !== generation) return;
                if (result._tag === "Failure") {
                  const rejected =
                    result.error instanceof ExternalAgentError &&
                    result.error.outcome === "rejected";
                  const inputs = state().inputs.map((input) =>
                    input.requestId === inputId
                      ? {
                          ...input,
                          status: rejected ? ("rejected" as const) : ("unknown" as const),
                        }
                      : input,
                  );
                  if (
                    rejected &&
                    state().session &&
                    inputs.some((input) => input.status === "accepted")
                  ) {
                    yield* save({ inputs, status: "running" });
                    yield* notify(
                      owner,
                      `The follow-up could not be delivered: ${result.error.message}`,
                    );
                    return yield* poll(owner);
                  }
                  busy = false;
                  return yield* finish(
                    owner,
                    rejected ? "failed" : "uncertain",
                    result.error.message,
                    inputs,
                  );
                }
                yield* save({
                  session: result.value,
                  status: "running",
                  inputs: state().inputs.map((input) =>
                    input.requestId === inputId ? { ...input, status: "accepted" } : input,
                  ),
                });
                const pending = state().inputs.find((input) => input.status === "pending");
                if (pending) {
                  const input = yield* followup(pending);
                  yield* submitExternal(owner, input.requestId, {
                    instructions: input.text,
                    input: [],
                  });
                } else yield* poll(owner);
              }),
            ),
            Match.tag("ExternalStatus", ({ generation: token, result }) =>
              Effect.gen(function* () {
                if (token !== generation) return;
                if (result._tag === "Failure") {
                  busy = false;
                  return yield* finish(owner, "uncertain", result.error.message);
                }
                const observed = result.value;
                if (observed.state === "running") {
                  return yield* poll(owner, true);
                }
                if (observed.state === "waiting_input") {
                  yield* save({ status: "waiting_input" });
                  for (const request of observed.requests ?? [])
                    yield* sendApproval(owner, {
                      _tag: "Enqueue",
                      entry: {
                        id: `${path}:input:${request.id}`,
                        contextPath: path,
                        target: owner.path,
                        kind: request.kind,
                        status: "pending",
                        request,
                      },
                    });
                  yield* notify(
                    owner,
                    observed.requests?.map((request) => request.prompt).join("\n") ??
                      "More information is needed",
                  );
                  return;
                }
                busy = false;
                const inputs = state().inputs.map((input) =>
                  input.status === "accepted" ? { ...input, status: "completed" as const } : input,
                );
                if (inputs.some((input) => input.status === "pending")) {
                  yield* save({ inputs });
                  return yield* advance(owner);
                }
                yield* finish(
                  owner,
                  observed.state === "unknown" ? "uncertain" : observed.state,
                  observed.result?.text ?? observed.error ?? "Task finished",
                  inputs,
                );
              }),
            ),
            Match.tag("ApprovalResolved", ({ requestId, response }) =>
              Effect.gen(function* () {
                if (yield* writeback.resolve(requestId, owner)) return;
                const entry = approvalEntries(registry).find(
                  (entry) =>
                    entry.id === requestId &&
                    entry.target === owner.path &&
                    ["resolved", "acknowledged"].includes(entry.status),
                );
                if (!entry || !isDeepStrictEqual(entry.response, response)) return;
                if (requestId === `${path}:confirm`) {
                  if (state().status === "awaiting-confirmation") {
                    yield* save({
                      status: response.decision === "approve" ? "ready" : "rejected",
                    });
                    if (response.decision === "approve") yield* advance(owner);
                    else yield* finish(owner, "rejected", "The user rejected this execution");
                  }
                  return yield* sendApproval(owner, {
                    _tag: "Acknowledge",
                    id: requestId,
                    target: owner.path,
                  });
                }
                if (
                  !state().session ||
                  state().responses?.some((item) => item.requestId === requestId)
                )
                  return;
                // Durable sending marker prevents repeating an external answer after interruption.
                yield* save({
                  responses: [...(state().responses ?? []), { requestId, status: "sending" }],
                });
                const token = generation;
                yield* owner.pipeToSelf(
                  agents[state().admission.agent]!.respond(
                    state().session!,
                    entry.request,
                    response,
                  ),
                  (result) => ({ _tag: "Responded", generation: token, requestId, result }),
                );
              }),
            ),
            Match.tag("Responded", ({ generation: token, requestId, result }) =>
              Effect.gen(function* () {
                // Answer delivery belongs to its request, even when a follow-up starts another round.
                if (
                  !state().responses?.some(
                    (item) => item.requestId === requestId && item.status === "sending",
                  )
                )
                  return;
                yield* save({
                  responses: state().responses?.map((item) =>
                    item.requestId === requestId
                      ? { ...item, status: result._tag === "Failure" ? "unknown" : "sent" }
                      : item,
                  ),
                });
                if (result._tag === "Failure") {
                  if (token !== generation)
                    return yield* notify(
                      owner,
                      `Answer delivery is uncertain: ${result.error.message}`,
                    );
                  busy = false;
                  return yield* finish(owner, "uncertain", result.error.message);
                }
                yield* sendApproval(owner, {
                  _tag: "Acknowledge",
                  id: requestId,
                  target: owner.path,
                });
                if (token !== generation) return;
                yield* save({ status: "running" });
                yield* poll(owner);
              }),
            ),
            Match.tag("ResumeTask", ({ input, replyTo }) =>
              Effect.gen(function* () {
                const saved = state();
                const previous = saved.resumptions?.find(
                  (item) => item.input.requestId === input.requestId,
                );
                if (previous)
                  return yield* replyTo.tell(
                    isDeepStrictEqual(previous.input, input)
                      ? { _tag: "Accepted", receipt: previous.receipt }
                      : {
                          _tag: "Rejected",
                          error: new ApplicationError({
                            kind: "conflict",
                            message: "Resumption identity belongs to another command",
                          }),
                        },
                  );
                if (
                  input.target !== path ||
                  input.expectedRevision !== registry.get(path)!.revision ||
                  !["failed", "uncertain"].includes(saved.status) ||
                  saved.resumptions?.some((item) =>
                    ["pending", "resuming", "unknown"].includes(item.status),
                  )
                )
                  return yield* replyTo.tell({
                    _tag: "Rejected",
                    error: new ApplicationError({
                      kind: "conflict",
                      message:
                        "Task revision changed or an execution still requires reconciliation",
                    }),
                  });
                const receipt = {
                  requestId: input.requestId,
                  revision: input.expectedRevision + 1,
                };
                yield* save({
                  resumptions: [
                    ...(saved.resumptions ?? []),
                    { input, receipt, status: "pending" },
                  ],
                });
                yield* replyTo.tell({ _tag: "Accepted", receipt });
                yield* reconcile(owner, input.requestId);
              }),
            ),
            Match.tag("SubmissionLocated", ({ requestId, result }) =>
              Effect.gen(function* () {
                if (result._tag === "Failure" || Option.isNone(result.value)) {
                  yield* settleResumption(requestId, "unknown");
                  return yield* finish(
                    owner,
                    "uncertain",
                    "Original submission could not be located; no replacement was submitted",
                  );
                }
                yield* save({ session: result.value.value });
                yield* reconcile(owner, requestId);
              }),
            ),
            Match.tag("ResumeObserved", ({ requestId, result }) =>
              Effect.gen(function* () {
                if (
                  !state().resumptions?.some(
                    (item) => item.input.requestId === requestId && item.status === "pending",
                  )
                )
                  return;
                if (result._tag === "Failure") {
                  yield* settleResumption(requestId, "unknown");
                  return yield* finish(owner, "uncertain", result.error.message);
                }
                const status = result.value;
                if (status.state === "failed" && status.resumable) {
                  yield* settleResumption(requestId, "resuming");
                  yield* owner.pipeToSelf(
                    agents[state().admission.agent]!.resume(state().session!),
                    (result) => ({ _tag: "Resumed", requestId, result }),
                  );
                } else if (["running", "waiting_input", "completed"].includes(status.state)) {
                  yield* settleResumption(requestId, "done");
                  yield* save({
                    inputs: state().inputs.map((input) =>
                      input.status === "unknown" || input.status === "sending"
                        ? { ...input, status: "accepted" }
                        : input,
                    ),
                  });
                  yield* owner.self.tell({
                    _tag: "ExternalStatus",
                    generation,
                    result: { _tag: "Success", value: status },
                  });
                } else {
                  yield* settleResumption(requestId, "unknown");
                  yield* finish(
                    owner,
                    "uncertain",
                    status.error ?? "Executor cannot authoritatively resume this work",
                  );
                }
              }),
            ),
            Match.tag("Resumed", ({ requestId, result }) =>
              Effect.gen(function* () {
                if (
                  !state().resumptions?.some(
                    (item) => item.input.requestId === requestId && item.status === "resuming",
                  )
                )
                  return;
                if (result._tag === "Failure") {
                  yield* settleResumption(requestId, "unknown");
                  return yield* finish(owner, "uncertain", result.error.message);
                }
                yield* settleResumption(requestId, "done");
                yield* save({
                  session: result.value,
                  status: "running",
                  inputs: state().inputs.map((input) =>
                    input.status === "unknown" || input.status === "sending"
                      ? { ...input, status: "accepted" }
                      : input,
                  ),
                });
                busy = true;
                yield* poll(owner);
              }),
            ),
            Match.tag("Cancel", ({ reason, replyTo }) =>
              Effect.gen(function* () {
                if (["ready", "awaiting-confirmation"].includes(state().status)) {
                  yield* finish(owner, "cancelled", reason);
                  yield* sendApproval(owner, { _tag: "Revoke", id: `${path}:confirm` });
                }
                if (replyTo) yield* replyTo.tell(undefined);
              }),
            ),
            Match.tag("WritebackFinished", writeback.finish),
            Match.exhaustive,
          ),
      });
    }),
  );
}
