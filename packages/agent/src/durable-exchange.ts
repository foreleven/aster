import {
  configure,
  defineDoc,
  type ConversationId,
  type Extension,
  type Harness,
} from "@earendil-works/pi-durable";
import type { Context } from "@earendil-works/chord";
import { Schema } from "effect";

const Exchange = Schema.Struct({
  requestId: Schema.NonEmptyString,
  input: Schema.NonEmptyString,
  error: Schema.NullOr(Schema.String),
  resultEntries: Schema.NullOr(Schema.Array(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)))),
});
const State = Schema.Struct({
  identity: Schema.String,
  pending: Schema.NullOr(Schema.String),
  exchanges: Schema.Array(Exchange),
}).check(
  Schema.makeFilter(
    (state) =>
      new Set(state.exchanges.map((exchange) => exchange.requestId)).size ===
        state.exchanges.length &&
      state.exchanges.every((exchange) =>
        exchange.resultEntries === null
          ? exchange.requestId === state.pending && exchange.error === null
          : new Set(exchange.resultEntries).size === exchange.resultEntries.length,
      ) &&
      (state.identity !== "" || state.exchanges.length === 0) &&
      (state.pending === null ||
        state.exchanges.some(
          (exchange) => exchange.requestId === state.pending && exchange.resultEntries === null,
        )),
    { expected: "Unique durable exchanges and an existing unfinished pending exchange" },
  ),
);

/** SDK transaction boundary. Aster's identity and result references share Pi's mutation line. */
export const DurableExchanges = defineDoc<{
  identity: string;
  pending: string | null;
  exchanges: {
    requestId: string;
    input: string;
    error: string | null;
    resultEntries: number[] | null;
  }[];
}>({
  kind: "app.aster.agent.exchanges",
  version: 1,
  scope: "session",
  initial: () => ({ identity: "", pending: null, exchanges: [] }),
});

export const admitExchange = async (
  harness: Harness,
  options: {
    conversationId: ConversationId;
    identity: string;
    requestId: string;
    input: string;
    instructions: string;
    model: { provider: string; modelId: string };
    extension: Extension;
  },
  context: Context,
) =>
  harness.commit(async (tx) => {
    const doc = await tx.doc(DurableExchanges);
    const state = Schema.decodeUnknownSync(State)(doc);
    if (state.identity !== "" && state.identity !== options.identity)
      throw new Error("Durable conversation storage belongs to another Aster owner");
    const existing = state.exchanges.find((exchange) => exchange.requestId === options.requestId);
    if (existing) {
      if (existing.input !== options.input)
        throw new Error(
          "Durable request identity conflicts with its frozen input or configuration",
        );
      return { entryIds: existing.resultEntries, error: existing.error };
    }
    if (state.pending !== null)
      throw new Error("Another durable exchange requires recovery before accepting new input");
    doc.identity = options.identity;
    doc.pending = options.requestId;
    doc.exchanges.push({
      requestId: options.requestId,
      input: options.input,
      error: null,
      resultEntries: null,
    });
    await configure(tx, options.conversationId, {
      model: options.model,
      instructions: options.instructions,
      extensions: [options.extension],
    });
    await tx.appendEntry(options.conversationId, {
      kind: "app.aster.agent.accepted",
      data: { requestId: options.requestId, input: options.input },
    });
    return { entryIds: null, error: null };
  }, context);

export const completeExchange = async (
  harness: Harness,
  options: {
    conversationId: ConversationId;
    requestId: string;
    entryIds: readonly number[];
    error?: string;
  },
  context: Context,
) =>
  harness.commit(async (tx) => {
    const doc = await tx.doc(DurableExchanges);
    Schema.decodeUnknownSync(State)(doc);
    const exchange = doc.exchanges.find((exchange) => exchange.requestId === options.requestId);
    if (!exchange || doc.pending !== options.requestId || exchange.resultEntries !== null)
      throw new Error("Durable result has no matching admitted exchange");
    exchange.resultEntries = [...options.entryIds];
    exchange.error = options.error ?? null;
    doc.pending = null;
    await tx.appendEntry(options.conversationId, {
      kind: "app.aster.agent.result",
      data: {
        requestId: options.requestId,
        entryIds: [...options.entryIds],
        error: options.error ?? null,
      },
    });
  }, context);
