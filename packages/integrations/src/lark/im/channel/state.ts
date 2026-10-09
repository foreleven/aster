import { ContextRegistry, ContextSession, validateConfig } from "@aster/core";
import { Clock, Effect, Ref, Schema, Struct } from "effect";
import { LarkConfig } from "../../config.js";
import { ChatPollError } from "../../shared/errors.js";
import { LarkChatService } from "../service/chat-service.js";
import { DEFAULT_TIME_ZONE, formatDate, dayStart, nextDay } from "../service/dates.js";
import { ChatPollingConfig } from "../config.js";
import { pollChats } from "../service/poll.js";
import { RetrievalProgressSchema, extendRetrieval } from "./retrieval.js";
import { ImSnapshot, type ImPollResult } from "./snapshot.js";

/** Cursor advancement follows durable acknowledgements from every affected Chat. */
export const makeImState = Effect.fn("ImState.make")(function* (
  session: ContextSession<ImSnapshot, never>,
) {
  const registry = yield* ContextRegistry;
  const config = yield* LarkConfig;
  const cli = yield* LarkChatService;
  const entry = config.im ?? {};
  const settings = yield* validateConfig("Lark IM polling", () =>
    Schema.decodeUnknownSync(ChatPollingConfig)(entry.config ?? {}),
  );
  const sessionStart = dayStart(formatDate(yield* Clock.currentTimeMillis));
  const startup = yield* Ref.make(true);
  const markRetrieved = Effect.fn("ImState.markRetrieved")(function* (
    from: string,
    through: string,
  ) {
    const end = Date.parse(through);
    for (let start = Date.parse(from); start < end;) {
      const date = formatDate(start);
      const next = Math.min(dayStart(nextDay(date)), end);
      yield* Effect.scoped(
        Effect.gen(function* () {
          const progress = yield* ContextSession.make({
            path: `/lark/im/retrieval/days/${date}`,
            state: RetrievalProgressSchema,
            message: Schema.Never,
            persistence: { layout: "daily", date, timeZone: DEFAULT_TIME_ZONE },
            initial: {
              state: { version: 1, date, timeZone: DEFAULT_TIME_ZONE, intervals: [] },
              messages: [],
              description: `Private IM retrieval coverage for ${date}`,
            },
          });
          yield* progress.state.update((state) => extendRetrieval(state, start, next), {
            mode: "bootstrap",
          });
        }),
      ).pipe(Effect.orDie);
      start = next;
    }
  });
  return {
    interval: settings.pollIntervalMs,
    restore: Effect.gen(function* () {
      const chats = registry.reader
        .directory()
        .filter(({ path }) => /^\/lark\/im\/chats\/[^/]+$/.test(path))
        .map(({ path }) => path.slice(path.lastIndexOf("/") + 1));
      yield* session.state
        .update((state) => ({ ...state, ready: false }), { mode: "bootstrap" })
        .pipe(Effect.orDie);
      return chats;
    }),
    poll: Effect.gen(function* () {
      const current = yield* session.state.get.pipe(Effect.orDie);
      const initial = yield* Ref.get(startup);
      const now = yield* Clock.currentTimeMillis;
      return yield* pollChats(
        cli,
        current.through,
        now,
        initial,
        sessionStart,
        settings.catchUpWindowMs,
      );
    }),
    accept: Effect.fn("ImState.accept")(function* (result: ImPollResult) {
      // Caller has durably handed every fetched batch to its Chat before entering here.
      yield* markRetrieved(result.start, result.through);
      yield* session.state
        .update((state) => ({ ...state, through: result.through }), { mode: "bootstrap" })
        .pipe(Effect.orDie);
      yield* Ref.set(startup, false);
      // Every Chat checks its own pending messages. The Channel supplies completed coverage only.
      const flushThrough = formatDate(dayStart(formatDate(Date.parse(result.through))) - 1);
      // The durable cursor has advanced. Keep its maximum overlap, even for smaller catch-up windows.
      const from = new Date(
        Math.max(sessionStart, Date.parse(result.through) - 60_000),
      ).toISOString();
      return { flushThrough, retainFrom: from };
    }),
    completed: Effect.fn("ImState.completed")(function* (result: ImPollResult) {
      yield* session.state
        .update(
          (state) => ({
            ...Struct.omit(state, ["lastError"]),
            ready: result.caughtUp,
            chats: result.batches.length,
          }),
          { mode: "bootstrap" },
        )
        .pipe(Effect.orDie);
      yield* Effect.logInfo({
        event: "lark.im.poll.completed",
        chats: result.batches.length,
        messages: result.batches.reduce((sum, batch) => sum + batch.messages.length, 0),
      });
    }),
    failed: Effect.fn("ImState.failed")(function* (error: ChatPollError) {
      yield* Effect.logWarning(error.message);
      yield* session.state
        .update((state) => ({ ...state, ready: false, lastError: error.message }), {
          mode: "bootstrap",
        })
        .pipe(Effect.orDie);
    }),
  };
});
