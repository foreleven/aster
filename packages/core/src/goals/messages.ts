import { AgentConversations } from "../agent/runtime/session.js";
import { Effect, Schema } from "effect";
import { isDeepStrictEqual } from "node:util";
import { DurableContext } from "../context/store.js";
import { ApplicationError, CommandIdentifier, CommandReceipt } from "../operations.js";
import { type TaskOrigin } from "../tasks/contracts.js";
import { signalDelivery } from "../signals/state/store.js";
import { askOwner } from "../runtime/addressing.js";
import { GoalMessageDelivery, SendGoalMessageInput } from "./message-contracts.js";
import { type GoalMailbox, type GoalCommandReply, goalRequestFingerprint } from "./protocol.js";
import { loadGoalStatus } from "./queries.js";
import { GoalInputPayload } from "./contracts.js";
import { GoalSnapshot } from "./state/snapshot.js";

/** A receiver receipt is authoritative only for an input from this exact source. */
export const receivedGoalMessageReceipt: (
  registry: DurableContext["Service"],
  messages: AgentConversations["Service"],
  source: string,
  operationId: string,
) => Effect.Effect<CommandReceipt | undefined> = Effect.fn("Goals.receivedMessageReceipt")(
  function* (
    registry: DurableContext["Service"],
    messages: AgentConversations["Service"],
    source: string,
    operationId: string,
  ) {
    for (const entry of registry.directory()) {
      if (!Schema.is(SendGoalMessageInput.fields.goalPath)(entry.path)) continue;
      const state = yield* registry.readState(entry.path, GoalSnapshot).pipe(Effect.orDie);
      const receipt = state.receipts.find((item) => item.requestId === operationId);
      if (!receipt) continue;
      for (const input of state.inputs.filter((input) => input.kind === "GoalMessage")) {
        const body = yield* messages.get(entry.path, input.entryId).pipe(Effect.orDie);
        const saved = yield* Schema.decodeUnknownEffect(
          Schema.Struct({ payload: GoalInputPayload.cases.GoalMessage }),
        )(body.data).pipe(Effect.orDie);
        if (saved.payload.source === source && saved.payload.requestId === operationId)
          return receipt.receipt;
      }
    }
    return undefined;
  },
);

export const validateGoalMessage: (
  registry: DurableContext["Service"],
  messages: AgentConversations["Service"],
  delivery: GoalMessageDelivery,
) => Effect.Effect<void, ApplicationError> = Effect.fn("Goals.validateMessage")(function* (
  registry: DurableContext["Service"],
  messages: AgentConversations["Service"],
  delivery: GoalMessageDelivery,
) {
  if (delivery.source.startsWith("/goals/")) {
    if ((yield* loadGoalStatus(registry, delivery.source)).status === "active") return;
  } else if (delivery.source.startsWith("/signals/") && registry.get(delivery.source)) {
    const saved = yield* signalDelivery(messages, delivery.source, delivery.requestId);
    if (saved && isDeepStrictEqual(saved, delivery)) return;
  }
  return yield* new ApplicationError({
    kind: "conflict",
    message: "Goal message has no active source or differs from its frozen occurrence",
  });
});

export const sendGoalMessage = Effect.fn("Goals.sendMessage")(function* (raw: GoalMessageDelivery) {
  const delivery = yield* Schema.decodeUnknownEffect(GoalMessageDelivery)(raw).pipe(
    Effect.mapError(
      () => new ApplicationError({ kind: "invalid-input", message: "Invalid Goal message" }),
    ),
  );
  const reply = yield* askOwner<GoalMailbox, GoalCommandReply>(
    `/user${delivery.goalPath}`,
    (replyTo) => ({
      _tag: "SubmitInput",
      requestId: delivery.requestId,
      input: { _tag: "GoalMessage", delivery },
      replyTo,
    }),
    delivery.requestId,
  );
  if (reply._tag === "Rejected") return yield* reply.error;
  return reply.receipt;
});

export const messageGoal = Effect.fn("Goals.message")(function* (
  origin: TaskOrigin,
  input: typeof SendGoalMessageInput.Type,
) {
  const receipt = yield* sendGoalMessage({ _tag: "GoalMessageDelivery", ...origin, ...input });
  return { status: "accepted" as const, receipt };
});

const MessageReceipts = Schema.Struct({
  receipts: Schema.Array(
    Schema.Struct({
      requestId: CommandIdentifier,
      payloadFingerprint: Schema.String,
      receipt: CommandReceipt,
    }),
  ),
});
export const goalMessageReceipt = Effect.fn("Goals.messageReceipt")(function* (
  registry: DurableContext["Service"],
  delivery: GoalMessageDelivery,
) {
  if (!registry.get(delivery.goalPath)) return undefined;
  const state = yield* registry.readState(delivery.goalPath, MessageReceipts).pipe(Effect.orDie);
  const saved = state.receipts.find((item) => item.requestId === delivery.requestId);
  if (!saved) return undefined;
  const fingerprint = goalRequestFingerprint({
    _tag: "SubmitInput",
    requestId: delivery.requestId,
    input: { _tag: "GoalMessage", delivery },
  });
  if (saved.payloadFingerprint !== fingerprint)
    return yield* new ApplicationError({
      kind: "conflict",
      message: "Goal message identity belongs to different content",
    });
  return saved.receipt;
});
