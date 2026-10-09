import type { ActorContext } from "@aster/actor";
import { AgentConversations } from "@aster/agent/harness";
import { Context, Deferred, Effect, Fiber, Layer, Match, Option, Ref, Schema, Scope } from "effect";
import { randomUUID } from "node:crypto";
import { approvalEntries, sendApproval } from "../approvals/actor.js";
import { ContextActor, contextPath } from "../context/actor.js";
import { defineContext } from "../context/definition.js";
import { ContextRegistry } from "../context/registry.js";
import { CurrentActors } from "../services/actors.js";
import { deliverTaskFeedback } from "./delivery.js";
import { TaskExecution } from "./execution/service.js";
import { TaskCommands, TaskInternal, type TaskCommand } from "./protocol.js";
import { TaskState } from "./state/model.js";
import { TaskOutcome, TaskSnapshot, type TaskInput } from "./state/snapshot.js";

type Owner = ActorContext<TaskCommand>;
const makeHandlers = Effect.gen(function* () {
  const scope = yield* Effect.scope;
  const state = yield* TaskState;
  const execution = yield* TaskExecution;
  const messages = yield* AgentConversations;
  const registry = yield* ContextRegistry;
  const running = yield* Ref.make<
    { generation: string; inputs: readonly string[]; fiber: Fiber.Fiber<TaskOutcome> } | undefined
  >(undefined);
  const notify = Effect.fnUntraced(function* (owner: Owner, outcome: TaskOutcome) {
    yield* owner.pipeToSelf(
      deliverTaskFeedback(owner, state.path, yield* state.snapshot, outcome),
      (result) => ({
        _tag: "DeliverySettled",
        ...(result._tag === "Failure" ? { error: result.error.message } : {}),
      }),
    );
  });
  const reportOutcome = Effect.fnUntraced(function* (owner: Owner) {
    const snapshot = yield* state.snapshot;
    if (snapshot.outcomeEntryId === undefined) return;
    const entry = yield* messages.get(state.path, snapshot.outcomeEntryId).pipe(Effect.orDie);
    const result = Schema.decodeUnknownSync(TaskOutcome)(entry.data);
    if (!result.requests?.some((request) => request.kind === "confirmation"))
      yield* notify(owner, result);
    if (result.status === "cancelled")
      for (const request of approvalEntries(registry))
        if (
          request.contextPath === state.path &&
          request.target === owner.path &&
          request.status === "pending"
        )
          yield* sendApproval(owner, { _tag: "Revoke", id: request.id });
    for (const request of snapshot.waiting ?? [])
      yield* sendApproval(owner, {
        _tag: "Enqueue",
        entry: { ...request, contextPath: state.path, target: owner.path, status: "pending" },
      });
  });
  const drive = Effect.fnUntraced(function* (owner: Owner) {
    if (yield* Ref.get(running)) return;
    const snapshot = yield* state.snapshot;
    if (
      (registry.get(snapshot.admission.replyTo)?.state as { status?: string } | undefined)
        ?.status !== "active" &&
      snapshot.status === "ready"
    ) {
      if (yield* state.cancel("The owning Goal has ended")) yield* reportOutcome(owner);
      return;
    }
    const work = yield* state.start;
    if (Option.isNone(work)) return;
    const generation = randomUUID();
    const fiber = yield* execution.run(work.value).pipe(Effect.forkIn(scope));
    yield* Ref.set(running, {
      generation,
      inputs: work.value.inputs.map((input) => input.requestId),
      fiber,
    });
    yield* owner.pipeToSelf(Fiber.join(fiber), (result) => {
      if (result._tag === "Failure") throw result.error;
      return { _tag: "ExecutionSettled", generation, outcome: result.value };
    });
  });
  const receiveInput = Effect.fnUntraced(function* (
    owner: Owner,
    input: TaskInput,
    replyTo?: import("@aster/actor").ReplyTo<import("./protocol.js").TaskAdmissionReply>,
  ) {
    const accepted = yield* state.accept(input).pipe(Effect.result);
    if (accepted._tag === "Failure") {
      if (replyTo) yield* replyTo.tell({ _tag: "Rejected", error: accepted.failure });
      return;
    }
    if (replyTo) yield* replyTo.tell({ _tag: "Accepted", receipt: accepted.success.receipt });
    if (accepted.success.replayed && input._tag !== "Answer") return;
    if (accepted.success.replayed || (yield* Ref.get(running))) {
      const ref = (yield* state.snapshot).inputs.find(
        (item) => item.requestId === accepted.success.receipt.requestId,
      )!;
      yield* owner.pipeToSelf(execution.send(ref), (result) => ({
        _tag: "DeliverySettled",
        ...(result._tag === "Failure" ? { error: result.error.message } : {}),
      }));
    } else yield* drive(owner);
  });
  return {
    restore: Effect.fnUntraced(function* (owner: Owner) {
      if (!(yield* state.exists)) return;
      yield* reportOutcome(owner);
      if (["ready", "running"].includes((yield* state.snapshot).status)) yield* drive(owner);
    }),
    receive: (command: TaskCommand, owner: Owner) =>
      Match.value(command).pipe(
        Match.tag("StartTask", ({ input, replyTo }) =>
          receiveInput(owner, { _tag: "Initial", input }, replyTo),
        ),
        Match.tag("Input", ({ input, replyTo }) =>
          receiveInput(owner, { _tag: "Message", input }, replyTo),
        ),
        Match.tag("CheckTask", ({ input, replyTo }) =>
          receiveInput(owner, { _tag: "Check", input }, replyTo),
        ),
        Match.tag("RetryTask", ({ input, replyTo }) =>
          receiveInput(owner, { _tag: "Retry", input }, replyTo),
        ),
        Match.tag("ApprovalResolved", ({ requestId, response }) =>
          receiveInput(owner, { _tag: "Answer", requestId, response }),
        ),
        Match.tag("ExecutionSettled", ({ generation, outcome }) =>
          Effect.gen(function* () {
            const completed = yield* Ref.get(running);
            if (generation !== completed?.generation) return;
            yield* Ref.set(running, undefined);
            if (!(yield* state.settle(outcome))) return;
            yield* reportOutcome(owner);
            const snapshot = yield* state.snapshot;
            if (
              snapshot.status === "ready" ||
              (snapshot.status === "waiting_input" &&
                snapshot.inputs.some(
                  (input) =>
                    input.status === "pending" && !completed.inputs.includes(input.requestId),
                ))
            )
              yield* drive(owner);
          }),
        ),
        Match.tag("DeliverySettled", ({ error }) =>
          error ? Effect.logWarning(error) : Effect.void,
        ),
        Match.tag("Cancel", ({ reason, replyTo }) =>
          Effect.gen(function* () {
            const active = yield* Ref.get(running);
            if (!active) {
              if (yield* state.cancel(reason)) yield* reportOutcome(owner);
              if (replyTo) yield* replyTo.tell(undefined);
              return;
            }
            yield* owner.pipeToSelf(execution.cancel(), (result) => ({
              _tag: "CancellationChecked",
              generation: active.generation,
              reason,
              replyTo,
              confirmed: result._tag === "Success" && result.value,
            }));
          }),
        ),
        Match.tag("CancellationChecked", ({ generation, reason, replyTo, confirmed }) =>
          Effect.gen(function* () {
            const active = yield* Ref.get(running);
            if (confirmed && generation === active?.generation) {
              yield* Fiber.interrupt(active.fiber);
              yield* Ref.set(running, undefined);
              const snapshot = yield* state.snapshot;
              yield* state.settle({
                roundId: snapshot.roundId!,
                status: "cancelled",
                text: reason,
                covered: snapshot.inputs.map((input) => input.requestId),
              });
              yield* reportOutcome(owner);
            }
            if (replyTo) yield* replyTo.tell(undefined);
          }),
        ),
        Match.exhaustive,
      ),
  };
});
export const TaskActor = ContextActor.define("tasks/Actor", {
  commands: TaskCommands,
  internal: TaskInternal,
  context: defineContext({ state: TaskSnapshot, message: Schema.Never }),
})(
  Effect.gen(function* () {
    const scope = yield* Effect.scope;
    const initialized = yield* Deferred.make<Effect.Success<typeof makeHandlers>>();
    return {
      started: (owner) =>
        Effect.gen(function* () {
          const path = contextPath(owner);
          const services = yield* Layer.buildWithScope(
            Layer.merge(TaskState.layer(path), TaskExecution.layer(path)),
            scope,
          ).pipe(Effect.provideService(CurrentActors, owner));
          const handlers = yield* makeHandlers.pipe(
            Effect.provideService(TaskState, Context.get(services, TaskState)),
            Effect.provideService(TaskExecution, Context.get(services, TaskExecution)),
            Effect.provideService(Scope.Scope, scope),
          );
          yield* Deferred.succeed(initialized, handlers);
          yield* handlers.restore(owner);
        }),
      receive: (command, owner) =>
        Effect.flatMap(Deferred.await(initialized), (handlers) => handlers.receive(command, owner)),
    };
  }),
);
