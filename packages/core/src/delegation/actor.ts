import { ApplicationError, CommandReceipt, ResumeRunDeliveryInput } from "@aster/api-contracts";
import { isDeepStrictEqual } from "node:util";
import { transitionDelegation, type DelegationTransition } from "./transition.js";
import { ExecutionOutcome } from "../tasks/outcome.js";
import { DelegationRequest, DelegationState } from "./state.js";
import { DelegationError } from "./errors.js";
import { Clock, Effect, Match, Layer, Schema, Option } from "effect";
import { ReplyTo, type ActorRef, type ActorContext } from "@aster/actor";
import { ContextActor, contextPath } from "../context/actor.js";
import { defineContext } from "../context/definition.js";
import { ContextRegistry } from "../context/registry.js";
import type { ExternalAgentError } from "../tasks/errors.js";
import { ExecutionSession, ExecutionStatus, ExternalAgents } from "../tasks/model.js";
import { ApprovalResolved, sendApproval } from "../approvals/actor.js";

const UpdateResult = Schema.Union([
  Schema.TaggedStruct("Success", { value: Schema.String }),
  Schema.TaggedStruct("Failure", { error: Schema.instanceOf(Error) }),
]);
export const DelegationUpdate = Schema.Union([
  Schema.TaggedStruct("Submitted", { result: UpdateResult }),
  Schema.TaggedStruct("Progress", { result: UpdateResult }),
  Schema.TaggedStruct("Finished", { outcome: ExecutionOutcome }),
]);
export type DelegationUpdate = typeof DelegationUpdate.Type;
const Outcome = <A extends Schema.Constraint>(value: A) =>
  Schema.Union([
    Schema.TaggedStruct("Success", { value }),
    Schema.TaggedStruct("Failure", { error: Schema.instanceOf(Error) }),
  ]);
export const ResumeExecutionReply = Schema.Union([
  Schema.TaggedStruct("Accepted", { receipt: CommandReceipt }),
  Schema.TaggedStruct("Rejected", { error: ApplicationError }),
]);
export type ResumeExecutionReply = typeof ResumeExecutionReply.Type;
const Command = Schema.Union([
  Schema.TaggedStruct("ResumeExecution", {
    input: ResumeRunDeliveryInput,
    replyTo: ReplyTo<ResumeExecutionReply>(),
  }),
  Schema.TaggedStruct("ResumeStatus", {
    requestId: Schema.String,
    result: Outcome(ExecutionStatus),
  }),
  Schema.TaggedStruct("ResumeSession", {
    requestId: Schema.String,
    result: Outcome(ExecutionSession),
  }),
  Schema.TaggedStruct("Start", {
    request: DelegationRequest,
    replyTo: ReplyTo<DelegationUpdate>(),
    recovering: Schema.optional(Schema.Boolean),
    resumeOnly: Schema.optional(Schema.Boolean),
  }),
  Schema.TaggedStruct("Session", { result: Outcome(ExecutionSession) }),
  Schema.TaggedStruct("Admission", { result: Outcome(Schema.Option(ExecutionSession)) }),
  Schema.TaggedStruct("Status", { result: Outcome(ExecutionStatus) }),
  Schema.TaggedStruct("Responded", {
    id: Schema.String,
    result: Outcome(Schema.Void),
  }),
  Schema.TaggedStruct("Poll", {}),
  ApprovalResolved,
]);
type Command = typeof Command.Type;
const terminalFailure = (
  status: "failed" | "cancelled" | "unknown",
  text: string,
): ExecutionOutcome =>
  Match.value(status).pipe(
    Match.when("failed", () => ({ _tag: "Failed" as const, text })),
    Match.when("cancelled", () => ({ _tag: "Cancelled" as const, text })),
    Match.when("unknown", () => ({ _tag: "Uncertain" as const, text })),
    Match.exhaustive,
  );

export class DelegationActor extends ContextActor.Service<DelegationActor, ExternalAgents>()(
  "signals/DelegationActor",
  {
    command: Command,
    context: defineContext({
      state: DelegationState,
      message: Schema.Unknown,
    }),
  },
) {
  static readonly layer = Layer.effect(
    DelegationActor,
    Effect.gen(function* () {
      const registry = yield* ContextRegistry;
      const agents = yield* ExternalAgents;
      let path = "";
      let replyTo: ActorRef<DelegationUpdate>;
      let busy = false;
      let recovering = false;
      let lastStatus = "";
      const state = () => Schema.decodeUnknownSync(DelegationState)(registry.get(path)!.state);
      const transition = Effect.fn("Delegation.transition")(function* (
        change: DelegationTransition,
      ) {
        const current = registry.get(path)!;
        const next = transitionDelegation(state(), change);
        const at = new Date(yield* Clock.currentTimeMillis).toISOString();
        return yield* registry
          .commit(
            {
              ...current,
              state: next.state,
              messages: next.event
                ? [...current.messages, { ...next.event, at }]
                : current.messages,
            },
            { expectedRevision: current.revision ?? 0 },
          )
          .pipe(Effect.asVoid, Effect.orDie);
      });
      const failure = (error: Error) =>
        Effect.gen(function* () {
          yield* transition({ type: "Uncertain", text: error.message });
          yield* replyTo.tell({
            _tag: "Finished",
            outcome: { _tag: "Uncertain", text: error.message },
          });
        });
      const operation = <A>(effect: Effect.Effect<A, ExternalAgentError>) =>
        effect.pipe(
          Effect.mapError((cause) => new DelegationError({ path, cause, message: cause.message })),
        );
      const lookupSubmission = Effect.fn("Delegation.lookupSubmission")(function* (
        context: ActorContext<Command, ExternalAgents | ContextRegistry>,
        fallback: Error,
      ) {
        const saved = state();
        const lookup = agents[saved.request.agent]?.lookupSubmission;
        if (!lookup) return yield* failure(fallback);
        busy = true;
        // The adapter only looks up durable admission. Absence must never call submit again.
        yield* context.pipeToSelf(
          operation(lookup(saved.request.task, { requestId: path })),
          (result) => ({ _tag: "Admission", result }),
        );
      });
      const markResumption = Effect.fn("Delegation.markResumption")(function* (
        requestId: string,
        status: "pending" | "resuming" | "done" | "unknown",
        change?: DelegationTransition,
      ) {
        const current = registry.get(path)!;
        const next = change
          ? transitionDelegation(state(), change)
          : { state: state(), event: undefined };
        const at = new Date(yield* Clock.currentTimeMillis).toISOString();
        const reconcilesUnknown = change?.type === "Submitted" || change?.type === "Completed";
        yield* registry
          .commit(
            {
              ...current,
              state: {
                ...next.state,
                resumptions: next.state.resumptions?.map((item) => {
                  if (item.input.requestId === requestId) return { ...item, status };
                  if (reconcilesUnknown && item.status === "unknown")
                    return { ...item, status: "done" };
                  return item;
                }),
              },
              messages: [
                ...current.messages,
                { type: "ResumptionChanged", requestId, status, at },
                ...(next.event ? [{ ...next.event, at }] : []),
              ],
            },
            { expectedRevision: current.revision ?? 0 },
          )
          .pipe(Effect.orDie);
      });
      const observeResumption = Effect.fn("Delegation.observeResumption")(function* (
        context: ActorContext<Command, ExternalAgents | ContextRegistry>,
        requestId: string,
      ) {
        const saved = state();
        recovering = false;
        if (saved.status === "completed" || saved.status === "cancelled") {
          yield* markResumption(requestId, "done");
          yield* replayTerminal(saved);
          return;
        }
        if (!agents[saved.request.agent]) {
          yield* markResumption(requestId, "done");
          return yield* failure(new Error("Executor unavailable for this retained session"));
        }
        if (!saved.session) {
          yield* markResumption(requestId, "done");
          return yield* lookupSubmission(
            context,
            new Error("Submission remains unknown; no session was created"),
          );
        }
        busy = true;
        yield* context.pipeToSelf(
          operation(agents[saved.request.agent]!.status(saved.session)),
          (result) => ({ _tag: "ResumeStatus", requestId, result }),
        );
      });
      const admitResumption = Effect.fn("Delegation.admitResumption")(function* (
        raw: ResumeRunDeliveryInput,
      ) {
        const input = yield* Schema.decodeUnknownEffect(ResumeRunDeliveryInput)(raw).pipe(
          Effect.mapError(
            () =>
              new ApplicationError({
                kind: "invalid-input",
                message: "Invalid resumption command",
              }),
          ),
        );
        if (!path || input.target !== state().request.runPath)
          return yield* new ApplicationError({
            kind: "invalid-input",
            message: "Resumption targets another Run",
          });
        const current = registry.get(path)!;
        const saved = state();
        const previous = saved.resumptions?.find(
          (item) => item.input.requestId === input.requestId,
        );
        if (previous) {
          if (!isDeepStrictEqual(previous.input, input))
            return yield* new ApplicationError({
              kind: "conflict",
              message: "Resumption identity belongs to another command",
            });
          return { receipt: previous.receipt, created: false };
        }
        if (busy)
          return yield* new ApplicationError({
            kind: "unavailable",
            message: "Execution observation is in progress; reconcile the same resumption later",
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
            { expectedRevision: current.revision ?? 0 },
          )
          .pipe(Effect.orDie);
        return { receipt, created: true };
      });
      // Durable terminal outcomes must be replayed before looking up a live adapter.
      // Poll deliberately skips finished work and cannot deliver these acknowledgements.
      const replayTerminal = (saved: DelegationState) =>
        Effect.gen(function* () {
          if (saved.status === "completed") {
            yield* replyTo.tell({
              _tag: "Finished",
              outcome: { _tag: "Completed", text: saved.result },
            });
            return true;
          }
          if (
            saved.status === "failed" ||
            saved.status === "cancelled" ||
            saved.status === "unknown"
          ) {
            yield* replyTo.tell({
              _tag: "Finished",
              outcome: terminalFailure(saved.status, saved.error),
            });
            return true;
          }
          return false;
        });
      return DelegationActor.of({
        started: (context) =>
          Effect.gen(function* () {
            const record = registry.get(contextPath(context));
            const saved = record && Schema.decodeUnknownSync(DelegationState)(record.state);
            if (!saved?.request || !saved.replyPath) return;
            // An explicit Run command must reach durable admission before this child
            // can observe or resume the executor. Already-admitted work still recovers.
            if (
              context.metadata.resumeOnly === true &&
              !saved.resumptions?.some((item) =>
                ["pending", "resuming", "unknown"].includes(item.status),
              )
            )
              return;
            const reply = yield* context.select(saved.replyPath).resolve().pipe(Effect.option);
            if (reply._tag === "Some")
              yield* context.self.tell({
                _tag: "Start",
                request: saved.request,
                replyTo: reply.value,
                recovering: true,
              });
          }),
        receive: (command, context) =>
          Match.value(command).pipe(
            Match.tag("ResumeExecution", ({ input, replyTo: receiver }) =>
              Effect.gen(function* () {
                const result = yield* admitResumption(input).pipe(Effect.result);
                if (result._tag === "Failure")
                  return yield* receiver.tell({ _tag: "Rejected", error: result.failure });
                yield* receiver.tell({ _tag: "Accepted", receipt: result.success.receipt });
                if (result.success.created) yield* observeResumption(context, input.requestId);
                else if (!busy) {
                  // The receiver's command receipt can outlive the parent's lost outcome.
                  // Duplicate admission must also replay durable completion, without an SDK call.
                  if (yield* replayTerminal(state())) return;
                  if (state().status === "uncertain")
                    yield* replyTo.tell({
                      _tag: "Finished",
                      outcome: {
                        _tag: "Uncertain",
                        text: state().error ?? "External outcome unknown",
                      },
                    });
                  else if (state().session) {
                    recovering = false;
                    yield* replyTo.tell({
                      _tag: "Submitted",
                      result: { _tag: "Success", value: state().session!.sessionId },
                    });
                    yield* context.self.tell({ _tag: "Poll" });
                  }
                }
              }),
            ),
            Match.tag("ResumeStatus", ({ requestId, result }) =>
              Effect.gen(function* () {
                busy = false;
                const attempt = state().resumptions!.find(
                  (item) => item.input.requestId === requestId,
                )!;
                if (result._tag === "Failure") {
                  yield* markResumption(
                    requestId,
                    attempt.status === "pending" ? "done" : "unknown",
                  );
                  return yield* failure(result.error);
                }
                const status = result.value;
                if (status.state === "failed" && status.resumable) {
                  // A durable marker precedes the external operation. Recovery may observe but
                  // cannot repeat a resume whose outcome was lost, even under a new request ID.
                  if (
                    attempt.status !== "pending" ||
                    state().resumptions?.some((item) => item.status === "unknown")
                  ) {
                    yield* markResumption(requestId, "unknown");
                    return yield* failure(
                      new Error(
                        "Prior resume outcome is unknown; external reconciliation is required",
                      ),
                    );
                  }
                  yield* markResumption(requestId, "resuming");
                  busy = true;
                  yield* context.pipeToSelf(
                    operation(agents[state().request.agent]!.resume(state().session!)),
                    (result) => ({ _tag: "ResumeSession", requestId, result }),
                  );
                  return;
                }
                if (status.state === "completed") {
                  if (!status.result) {
                    yield* markResumption(requestId, "done");
                    return yield* failure(new Error("Executor completed without a result"));
                  }
                  yield* markResumption(requestId, "done", {
                    type: "Completed",
                    text: status.result.text,
                  });
                  return yield* replyTo.tell({
                    _tag: "Finished",
                    outcome: { _tag: "Completed", text: status.result.text },
                  });
                }
                if (
                  status.state === "failed" ||
                  status.state === "cancelled" ||
                  status.state === "unknown"
                ) {
                  const text = status.error ?? `External execution ${status.state}`;
                  yield* markResumption(requestId, "done", {
                    type: "Failed",
                    status: status.state,
                    text,
                  });
                  return yield* replyTo.tell({
                    _tag: "Finished",
                    outcome: terminalFailure(status.state, text),
                  });
                }
                yield* markResumption(requestId, "done", {
                  type: "Submitted",
                  session: state().session!,
                });
                yield* replyTo.tell({
                  _tag: "Submitted",
                  result: { _tag: "Success", value: state().session!.sessionId },
                });
                busy = true;
                yield* context.self.tell({ _tag: "Status", result });
              }),
            ),
            Match.tag("ResumeSession", ({ requestId, result }) =>
              Effect.gen(function* () {
                busy = false;
                if (
                  result._tag === "Success" &&
                  result.value.sessionId !== state().session!.sessionId
                ) {
                  yield* markResumption(requestId, "unknown");
                  return yield* failure(
                    new Error(
                      "Executor returned another session during resume; original execution retained",
                    ),
                  );
                }
                if (result._tag === "Failure") {
                  yield* markResumption(requestId, "unknown", {
                    type: "Uncertain",
                    text: result.error.message,
                  });
                  return yield* replyTo.tell({
                    _tag: "Finished",
                    outcome: { _tag: "Uncertain", text: result.error.message },
                  });
                }
                // The returned handle and completion marker share one commit. A crash cannot
                // retain a successful resume while losing the provider's updated run ID.
                yield* markResumption(requestId, "done", {
                  type: "Submitted",
                  session: result.value,
                });
                yield* replyTo.tell({
                  _tag: "Submitted",
                  result: { _tag: "Success", value: result.value.sessionId },
                });
                yield* context.self.tell({ _tag: "Poll" });
              }),
            ),
            Match.tag("Start", (command) =>
              Effect.gen(function* () {
                if (path) {
                  // A parent behavior restarted; reattach without repeating external submission.
                  replyTo = command.replyTo;
                  if (command.resumeOnly) return;
                  const saved = state();
                  if (yield* replayTerminal(saved)) return;
                  if (saved.session)
                    yield* replyTo.tell({
                      _tag: "Submitted",
                      result: { _tag: "Success", value: saved.session.sessionId },
                    });
                  if (saved.status === "uncertain")
                    yield* replyTo.tell({
                      _tag: "Finished",
                      outcome: {
                        _tag: "Uncertain",
                        text: saved.error ?? "External outcome unknown",
                      },
                    });
                  else if (saved.status === "waiting_input")
                    yield* replyTo.tell({
                      _tag: "Progress",
                      result: { _tag: "Success", value: "Waiting for external agent input" },
                    });
                  return;
                }
                path = `/delegations/${command.request.runPath.split("/").at(-1)!}`;
                replyTo = command.replyTo;
                recovering = !!command.recovering;
                const previous = registry.get(path);
                if (!previous)
                  yield* registry
                    .commit(
                      {
                        path,
                        description: `Agent execution: ${command.request.task.instructions}`,
                        state: {
                          request: command.request,
                          replyPath: command.replyTo.path,
                          status: "submitting",
                          requests: {},
                          responses: {},
                        },
                        messages: [{ type: "Requested", task: command.request.task }],
                      },
                      { expectedRevision: 0 },
                    )
                    .pipe(Effect.asVoid, Effect.orDie);
                if (!state().replyPath)
                  yield* transition({ type: "Attach", replyPath: command.replyTo.path });
                if (command.resumeOnly) return;
                if (state().resumptions?.length) recovering = false;
                const resumption = state().resumptions?.findLast(
                  (item) =>
                    item.status === "pending" ||
                    item.status === "resuming" ||
                    item.status === "unknown",
                );
                if (resumption) {
                  if (resumption.status === "resuming")
                    yield* markResumption(resumption.input.requestId, "unknown");
                  yield* observeResumption(context, resumption.input.requestId);
                  return;
                }
                if (yield* replayTerminal(state())) return;
                if (!agents[command.request.agent]) {
                  yield* failure(new Error(`Unknown external Agent: ${command.request.agent}`));
                  return;
                }
                if (state().session) {
                  yield* context.self.tell({ _tag: "Poll" });
                  return;
                }
                if (previous || recovering) {
                  yield* lookupSubmission(
                    context,
                    new Error("Submission outcome unknown; no replacement session created"),
                  );
                  return;
                }
                busy = true;
                yield* context.pipeToSelf(
                  operation(
                    agents[state().request.agent]!.submit(state().request.task, {
                      requestId: path,
                    }),
                  ),
                  (result) => ({ _tag: "Session", result }),
                );
              }),
            ),
            Match.tag("Session", ({ result }) =>
              Effect.gen(function* () {
                busy = false;
                yield* Match.value(result).pipe(
                  Match.tag("Failure", ({ error }) => lookupSubmission(context, error)),
                  Match.tag("Success", ({ value }) =>
                    Effect.gen(function* () {
                      yield* transition({ type: "Submitted", session: value });
                      yield* Effect.logInfo(
                        JSON.stringify({
                          event: "delegation.submitted",
                          path,
                          agent: state().request.agent,
                          sessionId: value.sessionId,
                          runId: value.runId,
                        }),
                      );
                      yield* replyTo.tell({
                        _tag: "Submitted",
                        result: { _tag: "Success", value: value.sessionId },
                      });
                      yield* context.self.tell({ _tag: "Poll" });
                    }),
                  ),
                  Match.exhaustive,
                );
              }),
            ),
            Match.tag("Admission", ({ result }) =>
              Effect.gen(function* () {
                busy = false;
                if (result._tag === "Failure") return yield* failure(result.error);
                if (Option.isNone(result.value))
                  return yield* failure(
                    new Error(
                      "No retained admission found; submission remains unknown and no replacement was created",
                    ),
                  );
                // Finding a handle authorizes observation, not restarting a failed execution.
                recovering = false;
                yield* context.self.tell({
                  _tag: "Session",
                  result: { _tag: "Success", value: result.value.value },
                });
              }),
            ),
            Match.tag("Poll", () =>
              Effect.gen(function* () {
                if (
                  busy ||
                  !path ||
                  !state().session ||
                  ["completed", "failed", "cancelled", "unknown"].includes(state().status)
                )
                  return;
                busy = true;
                yield* context.pipeToSelf(
                  operation(agents[state().request.agent]!.status(state().session!)),
                  (result) => ({ _tag: "Status", result }),
                );
              }),
            ),
            Match.tag("Status", ({ result }) =>
              Effect.gen(function* () {
                busy = false;
                if (result._tag === "Failure") {
                  yield* failure(result.error);
                  return;
                }
                const status = result.value;
                if (lastStatus !== status.state) {
                  yield* Effect.logInfo(
                    JSON.stringify({ event: "delegation.status", path, state: status.state }),
                  );
                  lastStatus = status.state;
                }
                if (status.state === "completed") {
                  if (!status.result) {
                    yield* failure(new Error("Executor completed without a result"));
                    return;
                  }
                  yield* transition({ type: "Completed", text: status.result.text });
                  yield* replyTo.tell({
                    _tag: "Finished",
                    outcome: { _tag: "Completed", text: status.result.text },
                  });
                  return;
                }
                if (recovering && status.resumable && status.state === "failed") {
                  recovering = false;
                  busy = true;
                  yield* context.pipeToSelf(
                    operation(agents[state().request.agent]!.resume(state().session!)),
                    (result) => ({ _tag: "Session", result }),
                  );
                  return;
                }
                recovering = false;
                if (
                  status.state === "failed" ||
                  status.state === "cancelled" ||
                  status.state === "unknown"
                ) {
                  const error = new Error(status.error ?? `External execution ${status.state}`);
                  // Commit the authoritative outcome before notifying the parent so recovery
                  // never resumes a run whose terminal acknowledgement was already delivered.
                  yield* transition({ type: "Failed", status: status.state, text: error.message });
                  yield* replyTo.tell({
                    _tag: "Finished",
                    outcome: terminalFailure(status.state, error.message),
                  });
                  return;
                }
                if (status.state === "waiting_input") {
                  for (const request of status.requests ?? []) {
                    const id = `${path}:${state().session!.runId ?? state().session!.sessionId}:${request.id}`;
                    yield* transition({ type: "WaitingInput", id, request });
                    yield* sendApproval(context, {
                      _tag: "Enqueue",
                      entry: {
                        id,
                        target: context.path,
                        contextPath: path,
                        kind: request.kind,
                        request,
                        status: "pending",
                      },
                    });
                    const response = state().responses[id];
                    if (response?.status === "received")
                      yield* context.self.tell({
                        _tag: "ApprovalResolved",
                        requestId: id,
                        response: response.response,
                      });
                    if (response?.status === "sending")
                      yield* transition({ type: "ResponseUncertain", requestId: id });
                  }
                  yield* replyTo.tell({
                    _tag: "Progress",
                    result: {
                      _tag: "Success",
                      value: "External agent awaiting approval or additional information",
                    },
                  });
                  yield* context.pipeToSelf(Effect.sleep("3 seconds"), () => ({ _tag: "Poll" }));
                  return;
                }
                yield* transition({ type: "Running" });
                busy = true;
                yield* context.pipeToSelf(
                  operation(agents[state().request.agent]!.wait(state().session!)),
                  (result) => ({ _tag: "Status", result }),
                );
              }),
            ),
            Match.tag("ApprovalResolved", ({ requestId, response }) =>
              Effect.gen(function* () {
                if (!path || !state().session || !state().requests[requestId]) return;
                if (["completed", "failed", "cancelled", "unknown"].includes(state().status)) {
                  yield* sendApproval(context, {
                    _tag: "Acknowledge",
                    id: requestId,
                    target: context.path,
                  });
                  return;
                }
                const existing = state().responses[requestId];
                if (!existing) yield* transition({ type: "ApprovalReceived", requestId, response });
                yield* sendApproval(context, {
                  _tag: "Acknowledge",
                  id: requestId,
                  target: context.path,
                });
                if ((existing && existing.status !== "received") || busy) return;
                const item = state().responses[requestId]!;
                yield* transition({ type: "ResponseSending", requestId });
                busy = true;
                yield* context.pipeToSelf(
                  operation(
                    agents[state().request.agent]!.respond(
                      state().session!,
                      item.request,
                      item.response,
                    ),
                  ),
                  (result) => ({ _tag: "Responded", id: requestId, result }),
                );
              }),
            ),
            Match.tag("Responded", ({ id, result }) =>
              Effect.gen(function* () {
                busy = false;
                yield* transition({ type: "ResponseDelivered", requestId: id, result });
                if (result._tag === "Failure") yield* failure(result.error);
                else yield* context.self.tell({ _tag: "Poll" });
              }),
            ),
            Match.exhaustive,
          ),
      });
    }),
  );
}
