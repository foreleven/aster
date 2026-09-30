import { transitionDelegation, type DelegationTransition } from "./transition.js";
import { ExecutionOutcome } from "../tasks/outcome.js";
import { DelegationRequest, DelegationState } from "./state.js";
import { DelegationError } from "./errors.js";
import { Clock, Effect, Match, Layer, Schema } from "effect";
import { ReplyTo, type ActorRef } from "@aster/actor";
import { ContextActor, contextPath } from "../context/actor.js";
import { defineContext } from "../context/model.js";
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
const Command = Schema.Union([
  Schema.TaggedStruct("Start", {
    request: DelegationRequest,
    replyTo: ReplyTo<DelegationUpdate>(),
    recovering: Schema.optional(Schema.Boolean),
  }),
  Schema.TaggedStruct("Session", { result: Outcome(ExecutionSession) }),
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
      identity: "Task delegated to an external agent",
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
        return yield* registry.set({
          ...current,
          state: next.state,
          messages: next.event ? [...current.messages, { ...next.event, at }] : current.messages,
        });
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
            Match.tag("Start", (command) =>
              Effect.gen(function* () {
                if (path) {
                  // A parent behavior restarted; reattach without repeating external submission.
                  replyTo = command.replyTo;
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
                  yield* registry.set({
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
                  });
                if (!state().replyPath)
                  yield* transition({ type: "Attach", replyPath: command.replyTo.path });
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
                  yield* failure(
                    new Error("Submission outcome unknown; no replacement session created"),
                  );
                  return;
                }
                busy = true;
                yield* context.pipeToSelf(
                  operation(agents[state().request.agent]!.submit(state().request.task)),
                  (result) => ({ _tag: "Session", result }),
                );
              }),
            ),
            Match.tag("Session", ({ result }) =>
              Effect.gen(function* () {
                busy = false;
                yield* Match.value(result).pipe(
                  Match.tag("Failure", ({ error }) => failure(error)),
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
