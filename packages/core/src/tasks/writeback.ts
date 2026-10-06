import { createHash, randomUUID } from "node:crypto";
import { isDeepStrictEqual } from "node:util";
import { AgentConversations } from "@aster/agent";
import { Context, Clock, Effect, Option, Schema } from "effect";
import {
  WritebackRequest,
  WritebackOperation,
  WritebackAuthorization,
  writebackApprovalId,
  writebackPrompt,
} from "@aster/api-contracts";
import { approvalEntries, sendApproval } from "../approvals/actor.js";
import { ContextRegistry } from "../context/registry.js";
import type { TaskState } from "./state.js";
import type { ActorContext } from "@aster/actor";
import type { TaskCommand } from "./protocol.js";

export class ChannelWriteError extends Schema.TaggedError<ChannelWriteError>()(
  "ChannelWriteError",
  {
    outcome: Schema.Literals(["rejected", "unknown"]),
    message: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {}

/** Concrete Channel adapters own credentials. Rejection proves no write was
 * accepted; transport failures after submission must be reported as unknown. */
export class ChannelWrites extends Context.Service<
  ChannelWrites,
  {
    readonly publish: (
      request: WritebackRequest,
      authorization: typeof WritebackAuthorization.Type,
    ) => Effect.Effect<{ readonly externalId: string }, ChannelWriteError>;
  }
>()("tasks/ChannelWrites") {}

export const WritebackFinished = Schema.TaggedStruct("WritebackFinished", {
  generation: Schema.String,
  requestId: Schema.String,
  result: Schema.Union([
    Schema.TaggedStruct("Success", { value: Schema.Struct({ externalId: Schema.NonEmptyString }) }),
    Schema.TaggedStruct("Failure", { error: Schema.instanceOf(ChannelWriteError) }),
  ]),
});

const publicationId = (source: string) =>
  createHash("sha256")
    .update(JSON.stringify(["writeback.v1", source]))
    .digest("hex")
    .slice(0, 48);

/** This intent is committed with the execution result, before any approval or I/O. */
export const planWriteback = (
  source: string,
  state: TaskState,
  text: string,
  at: string,
): WritebackOperation | undefined => {
  if (state.writeback) return state.writeback;
  if (state.status !== "completed" || !state.admission.action || !text.trim()) return undefined;
  const requestId = publicationId(source);
  return {
    status: "waiting-approval",
    request: {
      requestId,
      source,
      taskSource: state.admission.source,
      causationId: state.inputs[0]!.requestId,
      createdAt: at,
      action: state.admission.action,
      content: text,
      // Publication is the end of this automatic chain. Channel echo must not
      // manufacture fresh authorization or replenish the causal budget.
      causal: { rootRequestId: state.admission.causal.rootRequestId, remainingAgentTurns: 0 },
    },
  };
};

/** All methods run on the Task mailbox. Only publish leaves it via pipeToSelf. */
export const makeTaskWriteback = Effect.fn("Task.writeback")(function* (options: {
  path: () => string;
  state: () => TaskState;
}) {
  const registry = yield* ContextRegistry;
  const messages = yield* AgentConversations;
  const channel = yield* Effect.serviceOption(ChannelWrites);
  const generation = yield* Effect.sync(randomUUID);
  const at = Clock.currentTimeMillis.pipe(Effect.map((now) => new Date(now).toISOString()));
  const save = Effect.fn("Task.saveWriteback")(function* (operation: WritebackOperation) {
    const current = registry.get(options.path())!;
    const previous = options.state();
    const text = `External publication ${operation.status}: ${operation.request.action.channelPath}${operation.error ? `. ${operation.error}` : ""}`;
    yield* messages
      .append(
        options.path(),
        `publication:${operation.request.requestId}:${operation.status}`,
        "task.publication",
        { text },
      )
      .pipe(Effect.orDie);
    yield* registry
      .commit(
        {
          ...current,
          state: {
            ...previous,
            writeback: operation,
          },
          messages: [],
        },
        { expectedRevision: current.revision ?? 0 },
      )
      .pipe(Effect.orDie);
  });
  const approval = (operation: WritebackOperation, actor: ActorContext<TaskCommand>) => {
    const id = writebackApprovalId(operation.request);
    return {
      id,
      contextPath: options.path(),
      target: actor.path,
      kind: "approval" as const,
      status: "pending" as const,
      request: { id, kind: "approval" as const, prompt: writebackPrompt(operation.request) },
    };
  };
  const dispatch = Effect.fn("Task.dispatchWriteback")(function* (
    actor: ActorContext<TaskCommand>,
  ) {
    const operation = options.state().writeback;
    if (!operation || operation.status !== "authorized" || !operation.authorization) return;
    yield* save({ ...operation, status: "sending", submittedAt: yield* at });
    const publish = Option.isSome(channel)
      ? channel.value.publish(operation.request, operation.authorization)
      : Effect.fail(
          new ChannelWriteError({
            outcome: "rejected",
            message: "No Channel write adapter is installed",
          }),
        );
    yield* actor.pipeToSelf(publish, (result) => ({
      _tag: "WritebackFinished",
      generation,
      requestId: operation.request.requestId,
      result,
    }));
  });
  const recover = Effect.fn("Task.recoverWriteback")(function* (actor: ActorContext<TaskCommand>) {
    const state = options.state();
    const operation = state.writeback;
    if (!operation) return;
    if (
      operation.request.requestId !== publicationId(options.path()) ||
      operation.request.taskSource !== state.admission.source ||
      operation.request.source !== options.path() ||
      !isDeepStrictEqual(operation.request.action, state.admission.action)
    )
      return yield* Effect.die(
        new Error("Writeback intent disagrees with its committed Task result"),
      );
    if (operation.status === "sending") {
      yield* save({
        ...operation,
        status: "unknown",
        error:
          "Publication was interrupted after durable submission intent; inspect the destination before any further action",
      });
      return;
    }
    if (operation.status === "waiting-approval")
      yield* sendApproval(actor, { _tag: "Enqueue", entry: approval(operation, actor) });
    if (operation.authorization || operation.status === "rejected")
      yield* sendApproval(actor, {
        _tag: "Acknowledge",
        id: writebackApprovalId(operation.request),
        target: actor.path,
      });
    yield* dispatch(actor);
  });
  const resolve = Effect.fn("Task.authorizeWriteback")(function* (
    requestId: string,
    actor: ActorContext<TaskCommand>,
  ) {
    const operation = options.state().writeback;
    if (!operation || requestId !== writebackApprovalId(operation.request)) return false;
    // Never trust a naked mailbox ApprovalResolved message as authority. Require
    // the queue's persisted decision bound to the exact displayed payload.
    const entry = approvalEntries(registry).find((entry) => entry.id === requestId);
    const expected = approval(operation, actor);
    if (
      !entry ||
      !["resolved", "acknowledged"].includes(entry.status) ||
      entry.target !== expected.target ||
      entry.contextPath !== expected.contextPath ||
      !isDeepStrictEqual(entry.request, expected.request) ||
      !entry.response?.decision
    )
      return true;
    if (operation.status === "waiting-approval") {
      yield* save(
        entry.response.decision === "approve"
          ? {
              ...operation,
              status: "authorized",
              authorization: {
                approvalId: requestId,
                approvalsRevision: registry.get("/approvals")!.revision!,
                approvedAt: yield* at,
              },
            }
          : { ...operation, status: "rejected", error: "Publication rejected by the user" },
      );
    }
    yield* sendApproval(actor, { _tag: "Acknowledge", id: requestId, target: actor.path });
    yield* dispatch(actor);
    return true;
  });
  const finish = Effect.fn("Task.finishWriteback")(function* (
    command: typeof WritebackFinished.Type,
  ) {
    const operation = options.state().writeback;
    if (
      command.generation !== generation ||
      !operation ||
      operation.status !== "sending" ||
      command.requestId !== operation.request.requestId
    )
      return;
    yield* save(
      command.result._tag === "Success"
        ? { ...operation, status: "published", externalId: command.result.value.externalId }
        : {
            ...operation,
            status: command.result.error.outcome,
            error: command.result.error.message,
          },
    );
  });
  return { recover, resolve, finish };
});
