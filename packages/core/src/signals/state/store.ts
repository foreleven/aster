import { isDeepStrictEqual } from "node:util";
import { AgentConversations } from "@aster/agent/harness";
import { RemainingAgentTurns, TaskMessage } from "../../tasks/contracts.js";
import { CommandReceipt } from "../../operations.js";

import { Effect, Match, Ref, Schema } from "effect";
import { ContextRegistry } from "../../context/registry.js";
import { SignalChangeInput, SignalReactionInput } from "../protocol.js";
import { SignalSnapshot, SignalTime } from "./snapshot.js";

const { sourceContext: _evidence, ...reactionIdentity } = SignalReactionInput.fields;
const SignalReceipt = Schema.TaggedUnion({
  Command: { input: SignalChangeInput, receipt: CommandReceipt },
  Reaction: {
    input: Schema.Struct(reactionIdentity),
    receipt: CommandReceipt,
  },
});
const DeliveryStatus = Schema.Literals(["pending", "sending", "delivered", "failed", "cancelled"]);
export const SignalDelivery = Schema.Struct({
  message: TaskMessage,
  status: DeliveryStatus,
  error: Schema.optional(Schema.String),
});
export type SignalDelivery = typeof SignalDelivery.Type;
const Event = Schema.TaggedUnion({
  Changed: {
    snapshot: SignalSnapshot,
    remainingAgentTurns: Schema.optional(RemainingAgentTurns),
    receipt: Schema.optional(SignalReceipt),
  },
  Triggered: {
    message: TaskMessage,
    nextDue: Schema.optional(Schema.NullOr(SignalTime)),
    receipt: Schema.optional(SignalReceipt),
  },
  DeliveryChanged: {
    requestId: Schema.String,
    status: Schema.Literals(["sending", "delivered", "failed"]),
    error: Schema.optional(Schema.String),
  },
});
type Event = typeof Event.Type;
interface History {
  readonly sequence: number;
  readonly snapshot?: SignalSnapshot;
  readonly remainingAgentTurns?: RemainingAgentTurns;
  readonly receipts: readonly (typeof SignalReceipt.Type)[];
  readonly deliveries: readonly SignalDelivery[];
}
const empty: History = { sequence: 0, receipts: [], deliveries: [] };
const apply = (history: History, event: Event): History =>
  Match.value(event).pipe(
    Match.tag("Changed", ({ snapshot, remainingAgentTurns, receipt }) => {
      if (snapshot.version !== (history.snapshot?.version ?? 0) + 1)
        throw new Error("Signal definition versions must be consecutive");
      return {
        ...history,
        snapshot,
        remainingAgentTurns,
        receipts: receipt ? [...history.receipts, receipt] : history.receipts,
        deliveries:
          snapshot.status === "deleted"
            ? history.deliveries.map((delivery) =>
                delivery.status === "pending"
                  ? { ...delivery, status: "cancelled" as const }
                  : delivery,
              )
            : history.deliveries,
      };
    }),
    Match.tag("Triggered", ({ message, nextDue, receipt }) => {
      if (
        !history.snapshot ||
        history.snapshot.status !== "active" ||
        history.deliveries.some((item) => item.message.requestId === message.requestId)
      )
        throw new Error("Signal trigger requires an active definition and a new delivery identity");
      return {
        ...history,
        snapshot: Schema.decodeUnknownSync(SignalSnapshot)({
          ...history.snapshot,
          ...(nextDue === undefined ? {} : { nextDue }),
        }),
        receipts: receipt ? [...history.receipts, receipt] : history.receipts,
        deliveries: [...history.deliveries, { message, status: "pending" as const }],
      };
    }),
    Match.tag("DeliveryChanged", ({ requestId, status, error }) => {
      const current = history.deliveries.find((item) => item.message.requestId === requestId);
      if (!current || current.status !== (status === "sending" ? "pending" : "sending"))
        throw new Error("Signal delivery transition has no matching predecessor");
      return {
        ...history,
        deliveries: history.deliveries.map((delivery) =>
          delivery.message.requestId === requestId
            ? { ...delivery, status, ...(error ? { error } : {}) }
            : delivery,
        ),
      };
    }),
    Match.exhaustive,
  );
export const readSignalHistory: (
  messages: AgentConversations["Service"],
  path: string,
) => Effect.Effect<History> = Effect.fn("Signal.readHistory")(function* (
  messages: AgentConversations["Service"],
  path: string,
): Effect.fn.Return<History> {
  const entries = yield* messages.read(path).pipe(Effect.orDie);
  let history = empty;
  for (const entry of entries) {
    if (entry.kind !== "signal.event") continue;
    const event = Schema.decodeUnknownSync(Event)(entry.data);
    history = { ...apply(history, event), sequence: history.sequence + 1 };
    if (history.snapshot) Schema.decodeUnknownSync(SignalSnapshot)(history.snapshot);
  }
  return history;
});

/** The mailbox writes Pi first, then its Context projection, then the committed Ref. */
export const makeSignalStore = Effect.fn("SignalStore.make")(function* (path: string) {
  const registry = yield* ContextRegistry;
  const messages = yield* AgentConversations;
  const record = registry.get(path);
  if (record) Schema.decodeUnknownSync(SignalSnapshot)(record.state);
  const restored = yield* readSignalHistory(messages, path);
  if (record && !restored.snapshot)
    return yield* Effect.die(new Error("Signal journal is missing"));
  const project = Effect.fnUntraced(function* (snapshot: SignalSnapshot) {
    const current = registry.get(path);
    yield* registry
      .commit(
        {
          path,
          description: current?.description ?? `Signal: ${path.split("/").at(-1)}`,
          state: snapshot,
          messages: [],
        },
        { expectedRevision: current?.revision ?? 0 },
      )
      .pipe(Effect.orDie);
  });
  if (restored.snapshot && !isDeepStrictEqual(record?.state, restored.snapshot))
    yield* project(restored.snapshot);
  const ref = yield* Ref.make(restored);
  const read = Ref.get(ref);
  const append = Effect.fn("SignalStore.commit")(function* (event: Event) {
    const previous = yield* read;
    Schema.decodeUnknownSync(Event)(event);
    const next = { ...apply(previous, event), sequence: previous.sequence + 1 };
    yield* messages
      .append(path, `event:${next.sequence}`, "signal.event", event)
      .pipe(Effect.orDie);
    // Delivery phases are private; they must not invalidate the rule screened by System One.
    if (event._tag !== "DeliveryChanged") yield* project(next.snapshot!);
    yield* Ref.set(ref, next);
  }, Effect.uninterruptible);
  return { read, append };
});

/** Only a durably started delivery may authorize Task admission, including after deletion. */
export const signalMessage: (
  messages: AgentConversations["Service"],
  path: string,
  requestId: string,
) => Effect.Effect<TaskMessage | undefined> = Effect.fn("Signal.message")(function* (
  messages: AgentConversations["Service"],
  path: string,
  requestId: string,
): Effect.fn.Return<TaskMessage | undefined> {
  const history = yield* readSignalHistory(messages, path);
  return history.deliveries.find(
    (delivery) =>
      delivery.message.requestId === requestId &&
      ["sending", "delivered"].includes(delivery.status),
  )?.message;
});
