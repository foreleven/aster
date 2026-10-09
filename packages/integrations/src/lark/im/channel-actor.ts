import { type ActorContext, type ActorRef } from "@aster/actor";
import {
  childActorName,
  ContextActor,
  ContextRegistry,
  defineContext,
  spawnContextChild,
} from "@aster/core";
import { Clock, Effect, Match, Ref, Schema } from "effect";
import { LarkConfig } from "../config.js";
import { imChannelView } from "../public-views.js";
import { LarkImCommands, makeLarkImQuery } from "../queries.js";
import { ImPollError } from "../shared/errors.js";
import { object, string } from "../shared/response.js";
import { LarkChatActor, type ChatCommand } from "./chat-actor.js";
import { ImSearch } from "./client.js";
import { imDate, imDayStart, nextImDay } from "./dates.js";
import { ImChat, ImMessage } from "./model.js";
import { parseImPolicy } from "./policy.js";
import { pollIm } from "./poll.js";
import { ImStorage } from "./storage.js";
const ImCommand = Schema.TaggedUnion({
  Poll: {},
  Polled: {
    result: Schema.TaggedUnion({
      Success: {
        value: Schema.Struct({
          start: Schema.String,
          through: Schema.String,
          caughtUp: Schema.Boolean,
          batches: Schema.Array(Schema.Struct({ chat: ImChat, messages: Schema.Array(ImMessage) })),
        }),
      },
      Failure: {
        error: Schema.instanceOf(ImPollError),
      },
    }),
  },
});
type ImCommand = typeof ImCommand.Type;

// This channel owns every child at this name; the generic Actor lookup cannot
// recover its command type. Keep that assertion at the lookup boundary.
const chatActor = Effect.fnUntraced(function* (context: ActorContext<ImCommand>, id: string) {
  const relative = `chats/${id}`;
  const existing = yield* context.child(childActorName(relative));
  if (existing) return existing as ActorRef<ChatCommand>;
  return yield* spawnContextChild(context, relative, LarkChatActor).pipe(Effect.orDie);
});

export const LarkImActor = ContextActor.define("lark/ImActor", {
  commands: LarkImCommands,
  internal: ImCommand,
  context: defineContext({
    view: imChannelView,
    state: Schema.Struct({
      ready: Schema.Boolean,
      chats: Schema.Number,
      lastError: Schema.optional(Schema.String),
    }),
    message: Schema.Never,
  }),
})(
  Effect.gen(function* () {
    const query = yield* makeLarkImQuery;
    const registry = yield* ContextRegistry;
    const config = yield* LarkConfig;
    const storage = yield* ImStorage;
    const entry = config.im ?? {};
    const policy = parseImPolicy(entry);
    const interval = policy.pollIntervalMs;
    const cli = yield* ImSearch;
    const sessionTime = yield* Clock.currentTimeMillis;
    const sessionStart = imDayStart(imDate(sessionTime));
    // Only mailbox handlers update this state. Each worker receives one snapshot.
    const polling = yield* Ref.make({
      busy: false,
      startup: true,
      cursor: storage.progress(imDate(sessionTime))?.through,
    });
    const poll = Effect.fn("LarkImActor.poll")(function* (progress: {
      readonly cursor: string | undefined;
      readonly startup: boolean;
    }) {
      const now = yield* Clock.currentTimeMillis;
      return yield* Effect.tryPromise({
        try: (signal) =>
          pollIm(
            cli,
            progress.cursor,
            now,
            signal,
            progress.startup,
            sessionStart,
            policy.catchUpWindowMs,
          ),
        catch: (cause) => new ImPollError({ cause, message: String(cause) }),
      });
    });
    return {
      query,
      started: (context) =>
        Effect.gen(function* () {
          // Import only today's legacy pending messages. Historical Contexts stay untouched.
          const today = imDate(yield* Clock.currentTimeMillis);
          for (const record of Object.values(registry.snapshot())) {
            if (!record.path.startsWith("/lark/im/chats/")) continue;
            const restored = yield* Schema.decodeUnknownEffect(Schema.Array(ImMessage))(
              record.messages,
            ).pipe(Effect.orDie);
            const messages = restored.filter((message) => imDate(message.at) === today);
            if (messages.length)
              storage.ingest(
                { chat: Schema.decodeUnknownSync(ImChat)(object(record.state).chat), messages },
                true,
              );
          }
          for (const day of storage.list(today)) {
            if (!day.pending.length && !day.commit) continue;
            yield* chatActor(context, day.chat.id);
          }
          const previous = registry.get("/lark/im");
          yield* registry
            .commit(
              {
                path: "/lark/im",
                description: string(entry.description) || "My work Lark messages",
                state: { ready: false, chats: Number(object(previous?.state).chats ?? 0) },
                messages: [],
              },
              { mode: "bootstrap", expectedRevision: previous?.revision ?? 0 },
            )
            .pipe(Effect.asVoid, Effect.orDie);
          yield* context.self.tell({ _tag: "Poll" });
        }),
      receive: (command, context) =>
        Match.value(command).pipe(
          Match.tag("Poll", (_command) =>
            Effect.gen(function* () {
              const previous = yield* Ref.getAndUpdate(polling, (state) => ({
                ...state,
                busy: true,
              }));
              if (previous.busy) return;
              yield* Effect.logInfo(JSON.stringify({ event: "lark.im.poll.started" }));
              yield* context.pipeToSelf(poll(previous), (result) => ({
                _tag: "Polled",
                result,
              }));
            }),
          ),
          Match.tag("Polled", (command) =>
            Effect.gen(function* () {
              yield* Ref.update(polling, (state) => ({ ...state, busy: false }));
              const continueCatchUp = yield* Match.value(command.result).pipe(
                Match.tag("Failure", (result) =>
                  Effect.gen(function* () {
                    yield* Effect.logWarning(result.error.message);
                    const previous = registry.get("/lark/im");
                    yield* registry
                      .commit(
                        {
                          path: "/lark/im",
                          description: string(entry.description) || "My work Lark messages",
                          state: {
                            chats: 0,
                            ...previous?.state,
                            ready: false,
                            lastError: result.error.message,
                          },
                          messages: [],
                        },
                        { expectedRevision: previous?.revision ?? 0 },
                      )
                      .pipe(Effect.asVoid, Effect.orDie);
                    return false;
                  }),
                ),
                Match.tag("Success", (result) =>
                  Effect.gen(function* () {
                    // Durable handoff precedes the cursor: tell() only enqueues a command.
                    for (const batch of result.value.batches) storage.ingest(batch);
                    storage.markRetrieved(result.value.start, result.value.through);
                    yield* Ref.update(polling, (state) => ({
                      ...state,
                      cursor: result.value.through,
                      startup: false,
                    }));
                    for (const batch of result.value.batches) {
                      const update = {
                        _tag: "Update" as const,
                        ...batch,
                      };
                      const child = yield* chatActor(context, update.chat.id);
                      yield* child.tell(update);
                    }
                    // Only close days covered during this process lifetime; never revive old backlogs on restart.
                    for (
                      let date = imDate(sessionStart);
                      imDayStart(nextImDay(date)) <= Date.parse(result.value.through);
                      date = nextImDay(date)
                    ) {
                      if (
                        storage.progress(date)?.through !==
                        new Date(imDayStart(nextImDay(date))).toISOString()
                      )
                        continue;
                      for (const day of storage.list(date)) {
                        if (!day.pending.length && !day.stage && !day.commit) continue;
                        const child = yield* chatActor(context, day.chat.id);
                        yield* child.tell({ _tag: "Flush", date });
                      }
                    }
                    yield* Effect.logInfo(
                      JSON.stringify({
                        event: "lark.im.poll.completed",
                        chats: result.value.batches.length,
                        messages: result.value.batches.reduce(
                          (sum, batch) => sum + batch.messages.length,
                          0,
                        ),
                      }),
                    );
                    yield* registry
                      .commit(
                        {
                          path: "/lark/im",
                          description: string(entry.description) || "My work Lark messages",
                          state: {
                            ready: result.value.caughtUp,
                            chats: result.value.batches.length,
                          },
                          messages: [],
                        },
                        { expectedRevision: registry.get("/lark/im")?.revision ?? 0 },
                      )
                      .pipe(Effect.asVoid, Effect.orDie);
                    return !result.value.caughtUp;
                  }),
                ),
                Match.exhaustive,
              );
              if (continueCatchUp) {
                yield* context.self.tell({ _tag: "Poll" });
                return;
              }
              yield* context.pipeToSelf(Effect.sleep(interval), () => ({
                _tag: "Poll",
              }));
            }),
          ),
          Match.exhaustive,
        ),
    };
  }),
);
