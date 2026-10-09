import { isDeepStrictEqual } from "node:util";
import { ContextSession } from "@aster/core";
import { Clock, Context, Effect, Layer, Struct } from "effect";
import { publicChatMessage, type ChatBatch } from "../service/model.js";
import { formatDate, dayStart } from "../service/dates.js";
import {
  ChatContext,
  messageFingerprint,
  type ChatWork,
  type ChatSnapshot,
  type SummaryCommit,
} from "./snapshot.js";

const retainReceipts = (state: ChatSnapshot, pending: ReadonlySet<string>): ChatSnapshot => ({
  ...state,
  seen: Object.fromEntries(
    Object.entries(state.seen).filter(
      ([id, receipt]) =>
        pending.has(id) ||
        state.replayFrom === undefined ||
        Date.parse(receipt.at) >= Date.parse(state.replayFrom),
    ),
  ),
});

/** The mailbox owns every mutation; model workers receive detached evidence. */
const makeChatState = Effect.fn("ChatState.make")(function* (path: string) {
  const id = path.slice(path.lastIndexOf("/") + 1);
  const session = yield* ContextSession.open({
    path,
    definition: ChatContext,
    initial: {
      description: `Work Lark conversation: ${id}`,
      state: { chat: { id, name: "", mode: "", description: "" }, seen: {} },
      messages: [],
    },
  }).pipe(Effect.orDie);
  const work = session.snapshot.pipe(
    Effect.map(({ state, messages }): ChatWork => ({
      ...state,
      pending: Object.values(messages).sort(ChatContext.compareMessages),
    })),
    Effect.orDie,
  );
  const flush = Effect.fn("ChatState.flush")(function* (date: string) {
    dayStart(date);
    let obligated = false;
    yield* session
      .commit(
        (current) => {
          obligated = Object.values(current.messages).some(
            (message) => formatDate(message.at) <= date,
          );
          if (!obligated) return {};
          const flushThrough =
            current.state.flushThrough && current.state.flushThrough > date
              ? current.state.flushThrough
              : date;
          return { state: { ...current.state, flushThrough } };
        },
        { mode: "bootstrap" },
      )
      .pipe(Effect.orDie);
    return obligated;
  });
  return {
    work,
    flush,
    restore: Effect.gen(function* () {
      const today = formatDate(yield* Clock.currentTimeMillis);
      const dates = (yield* work).pending
        .map((message) => formatDate(message.at))
        .filter((date) => date < today)
        .sort();
      const latest = dates.at(-1);
      if (latest) yield* flush(latest);
    }),
    accept: Effect.fn("ChatState.accept")(function* (batch: ChatBatch) {
      let changed = false;
      yield* session
        .commit(
          (current) => {
            const incoming = new Map(
              batch.messages.map((message) => [message.id, publicChatMessage(message)]),
            );
            const upsert = [...incoming.values()].filter(
              (message) =>
                current.state.seen[message.id]?.fingerprint !== messageFingerprint(message),
            );
            changed = upsert.length > 0;
            return {
              state: {
                ...current.state,
                chat: batch.chat,
                seen: {
                  ...current.state.seen,
                  ...Object.fromEntries(
                    upsert.map((message) => [
                      message.id,
                      { fingerprint: messageFingerprint(message), at: message.at },
                    ]),
                  ),
                },
              },
              messages: { upsert },
            };
          },
          {
            mode: "bootstrap",
            description: `Work Lark conversation: ${batch.chat.name || batch.chat.id}`,
          },
        )
        .pipe(Effect.orDie);
      yield* Effect.logInfo({ event: "chat.messages", path, received: batch.messages.length });
      return changed;
    }),
    retain: Effect.fn("ChatState.retain")(function* (from: string) {
      yield* session
        .commit(
          (current) => ({
            state: retainReceipts(
              { ...current.state, replayFrom: from },
              new Set(Object.keys(current.messages)),
            ),
          }),
          { mode: "bootstrap" },
        )
        .pipe(Effect.orDie);
    }),
    commit: Effect.fn("ChatState.commit")(function* (result: SummaryCommit) {
      const previous = yield* session.state.get.pipe(Effect.orDie);
      const covered = new Map(result.batch.map((message) => [message.id, message]));
      yield* session
        .commit(
          (current) => {
            const pending = Object.values(current.messages).filter(
              (message) => !isDeepStrictEqual(covered.get(message.id), message),
            );
            const flushThrough = current.state.flushThrough;
            return {
              state: retainReceipts(
                {
                  ...Struct.omit(current.state, ["flushThrough"]),
                  ...(flushThrough &&
                  pending.some((message) => formatDate(message.at) <= flushThrough)
                    ? { flushThrough }
                    : {}),
                  summary: result.rolling,
                },
                new Set(pending.map((message) => message.id)),
              ),
              messages: { removeUnchanged: result.batch },
            };
          },
          { mode: isDeepStrictEqual(previous.summary, result.rolling) ? "bootstrap" : "update" },
        )
        .pipe(Effect.orDie);
      yield* Effect.logInfo({ event: "chat.summary.saved", path, covered: result.batch.length });
      return yield* work;
    }),
  };
});
export class ChatState extends Context.Service<
  ChatState,
  Effect.Success<ReturnType<typeof makeChatState>>
>()("lark/im/ChatState") {
  static readonly layer = (path: string) => Layer.effect(ChatState, makeChatState(path));
}
