import { ContextRegistry, ContextSession, validateConfig } from "@aster/core";
import { Clock, Context, Effect, Layer, Ref, Schema, Struct } from "effect";
import { LarkConfig } from "../../config.js";
import { ChatPollError } from "../../shared/errors.js";
import { string } from "../../shared/response.js";
import { LarkChatService } from "../service/chat-service.js";
import { DEFAULT_TIME_ZONE, formatDate, dayStart, nextDay } from "../service/dates.js";
import { ChatSnapshot } from "../chat/snapshot.js";
import { ChatMessage } from "../service/model.js";
import { ChatPollingConfig } from "../config.js";
import { pollChats } from "../service/poll.js";
import { RetrievalProgressSchema, extendRetrieval } from "./retrieval.js";
import { ImContext, type ImPollResult } from "./snapshot.js";

const RetrievalContext = ContextSession.define({
  state: RetrievalProgressSchema,
  message: Schema.Never,
  messageKey: (_message: never) => "",
  compareMessages: () => 0,
});

/** Cursor advancement follows durable acknowledgements from every affected Chat. */
const makeImState = Effect.gen(function* () {
  const registry = yield* ContextRegistry;
  const config = yield* LarkConfig;
  const cli = yield* LarkChatService;
  const entry = config.im ?? {};
  const settings = yield* validateConfig("Lark IM polling", () =>
    Schema.decodeUnknownSync(ChatPollingConfig)(entry.config ?? {}),
  );
  const session = yield* ContextSession.open({
    path: "/lark/im",
    definition: ImContext,
    initial: {
      state: { ready: false, chats: 0 },
      messages: [],
      description: string(entry.description) || "My work Lark messages",
    },
  }).pipe(Effect.orDie);
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
          const progress = yield* ContextSession.open({
            path: `/lark/im/retrieval/days/${date}`,
            definition: RetrievalContext,
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
      const chats: string[] = [];
      for (const record of Object.values(registry.snapshot())) {
        if (!/^\/lark\/im\/chats\/[^/]+$/.test(record.path)) continue;
        const state = yield* Schema.decodeUnknownEffect(ChatSnapshot)(record.state).pipe(
          Effect.orDie,
        );
        if (record.messages.length) chats.push(state.chat.id);
      }
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
      const flushes: Array<{ chat: string; date: string }> = [];
      const chats: string[] = [];
      for (const record of Object.values(registry.snapshot())) {
        if (!/^\/lark\/im\/chats\/[^/]+$/.test(record.path)) continue;
        chats.push(record.path.slice(record.path.lastIndexOf("/") + 1));
        const messages = yield* Schema.decodeUnknownEffect(Schema.Array(ChatMessage))(
          record.messages,
        ).pipe(Effect.orDie);
        const dates = new Set(messages.map((message) => formatDate(message.at)));
        for (const date of dates)
          if (dayStart(nextDay(date)) <= Date.parse(result.through))
            flushes.push({ chat: record.path.slice(record.path.lastIndexOf("/") + 1), date });
      }
      // The durable cursor has advanced. Keep its maximum overlap, even for smaller catch-up windows.
      const from = new Date(
        Math.max(sessionStart, Date.parse(result.through) - 60_000),
      ).toISOString();
      return { flushes, retention: { from, chats } };
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
export class ImState extends Context.Service<ImState, Effect.Success<typeof makeImState>>()(
  "lark/im/ImState",
) {
  static readonly layer = Layer.effect(ImState, makeImState);
}
