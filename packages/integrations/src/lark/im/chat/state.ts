import { isDeepStrictEqual } from "node:util";
import type { ContextHandle } from "@aster/core";
import { Clock, Effect, Struct } from "effect";
import { publicChatMessage, ChatMessage, type ChatBatch } from "../service/model.js";
import { formatDate, dayStart } from "../service/dates.js";
import { type ChatWork, ChatSnapshot, type SummaryCommit } from "./snapshot.js";

/** The mailbox owns every mutation; model workers receive detached evidence. */
export const makeChatState = (session: ContextHandle<ChatSnapshot, ChatMessage>) => {
  const { path } = session;
  const work = session.snapshot.pipe(
    Effect.map(({ state, messages }): ChatWork => ({
      ...state,
      pending: [...messages].sort(
        (a, b) => Date.parse(a.at) - Date.parse(b.at) || a.id.localeCompare(b.id),
      ),
    })),
    Effect.orDie,
  );
  const flush = Effect.fn("ChatState.flush")(function* (date: string) {
    dayStart(date);
    let obligated = false;
    yield* session
      .commit(
        (current) => {
          obligated = current.messages.some((message) => formatDate(message.at) <= date);
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
            const retained = new Set(current.messages.map((message) => message.id));
            const append = [...incoming.values()].filter((message) => !retained.has(message.id));
            changed = append.length > 0;
            return {
              state: { ...current.state, chat: batch.chat },
              messages: { append },
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
    commit: Effect.fn("ChatState.commit")(function* (result: SummaryCommit) {
      const previous = yield* session.state.get.pipe(Effect.orDie);
      const covered = new Set(result.batch.map((message) => message.id));
      yield* session
        .commit(
          (current) => {
            const pending = current.messages.filter((message) => !covered.has(message.id));
            const flushThrough = current.state.flushThrough;
            return {
              state: {
                ...Struct.omit(current.state, ["flushThrough"]),
                ...(flushThrough &&
                pending.some((message) => formatDate(message.at) <= flushThrough)
                  ? { flushThrough }
                  : {}),
                summary: result.rolling,
              },
              messages: { remove: [...covered] },
            };
          },
          { mode: isDeepStrictEqual(previous.summary, result.rolling) ? "bootstrap" : "update" },
        )
        .pipe(Effect.orDie);
      yield* Effect.logInfo({ event: "chat.summary.saved", path, covered: result.batch.length });
      return yield* work;
    }),
  };
};
