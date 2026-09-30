import { ImPollError } from "../shared/errors.js";
import { LarkConfig } from "../config.js";
import { Clock, Effect, Match, Layer, Schema } from "effect";
import { type ActorRef } from "@aster/actor";
import {
  ContextActor,
  childActorName,
  ContextRegistry,
  defineContext,
  spawnContextChild,
} from "@aster/core";
import { object, string } from "../shared/response.js";
import { ImChat, ImMessage } from "./model.js";
import { LarkChatActor, type ChatCommand } from "./chat-actor.js";
import { ChatSummarizer } from "./summarizer.js";
import { ImSearch } from "./client.js";
import { ImStorage } from "./storage.js";
import { imDate, imDayStart, nextImDay } from "./dates.js";
import { ImAgentQueue } from "./agent-queue.js";
import { ImSummaryGate } from "./summary-gate.js";
import { parseImPolicy } from "./policy.js";
import { pollIm } from "./poll.js";
const ImCommand = Schema.Union([
  Schema.TaggedStruct("Poll", {}),
  Schema.TaggedStruct("Polled", {
    result: Schema.Union([
      Schema.TaggedStruct("Success", {
        value: Schema.Struct({
          start: Schema.String,
          through: Schema.String,
          caughtUp: Schema.Boolean,
          batches: Schema.Array(Schema.Struct({ chat: ImChat, messages: Schema.Array(ImMessage) })),
        }),
      }),
      Schema.TaggedStruct("Failure", {
        error: Schema.instanceOf(ImPollError),
      }),
    ]),
  }),
]);
export class LarkImActor extends ContextActor.Service<
  LarkImActor,
  LarkConfig | ChatSummarizer | ImStorage | ImSearch | ImAgentQueue | ImSummaryGate
>()("lark/ImActor", {
  command: ImCommand,
  context: defineContext({
    identity: "Work Lark IM integration",
    state: Schema.Struct({
      ready: Schema.Boolean,
      chats: Schema.Number,
      lastError: Schema.optional(Schema.String),
    }),
    message: Schema.Never,
  }),
}) {
  static readonly layer = Layer.effect(
    LarkImActor,
    Effect.gen(function* () {
      const registry = yield* ContextRegistry;
      const config = yield* LarkConfig;
      const storage = yield* ImStorage;
      const entry = config.im ?? {};
      const policy = parseImPolicy(entry);
      const interval = policy.pollIntervalMs;
      const cli = yield* ImSearch;
      let busy = false;
      let startup = true;
      const sessionTime = yield* Clock.currentTimeMillis;
      const sessionStart = imDayStart(imDate(sessionTime));
      let cursor = storage.progress(imDate(sessionTime))?.through;
      const poll = Clock.currentTimeMillis.pipe(
        Effect.flatMap((now) =>
          Effect.tryPromise({
            try: (signal) =>
              pollIm(cli, cursor, now, signal, startup, sessionStart, policy.catchUpWindowMs),
            catch: (cause) => new ImPollError({ cause, message: String(cause) }),
          }),
        ),
      );
      return LarkImActor.of({
        started: (context) =>
          Effect.gen(function* () {
            // Import only today's legacy pending messages. Historical Contexts stay untouched.
            const today = imDate(yield* Clock.currentTimeMillis);
            for (const record of Object.values(registry.snapshot())) {
              if (!record.path.startsWith("/lark/im/chats/")) continue;
              const messages = (record.messages as readonly ImMessage[]).filter(
                (message) => imDate(message.at) === today,
              );
              if (messages.length)
                storage.ingest(
                  { chat: Schema.decodeUnknownSync(ImChat)(object(record.state).chat), messages },
                  true,
                );
            }
            for (const day of storage.list(today)) {
              if (!day.pending.length && !day.commit) continue;
              if (yield* context.child(childActorName(`chats/${day.chat.id}`))) continue;
              yield* spawnContextChild(context, `chats/${day.chat.id}`, LarkChatActor).pipe(
                Effect.orDie,
              );
            }
            const previous = registry.get("/lark/im");
            yield* registry.set(
              {
                path: "/lark/im",
                description: string(entry.description) || "My work Lark messages",
                state: { ready: false, chats: Number(object(previous?.state).chats ?? 0) },
                messages: [],
              },
              { evaluate: false },
            );
            yield* context.self.tell({ _tag: "Poll" });
          }),
        receive: (command, context) =>
          Match.value(command).pipe(
            Match.tag("Poll", (_command) =>
              Effect.gen(function* () {
                if (busy) return;
                busy = true;
                yield* Effect.logInfo(JSON.stringify({ event: "lark.im.poll.started" }));
                yield* context.pipeToSelf(poll, (result) => ({
                  _tag: "Polled",
                  result,
                }));
              }),
            ),
            Match.tag("Polled", (command) =>
              Effect.gen(function* () {
                busy = false;
                let continueCatchUp = false;
                yield* Match.value(command.result).pipe(
                  Match.tag("Failure", (result) =>
                    Effect.gen(function* () {
                      yield* Effect.logWarning(result.error.message);
                      const previous = registry.get("/lark/im");
                      yield* registry.set({
                        path: "/lark/im",
                        description: string(entry.description) || "My work Lark messages",
                        state: {
                          chats: 0,
                          ...previous?.state,
                          ready: false,
                          lastError: result.error.message,
                        },
                        messages: [],
                      });
                    }),
                  ),
                  Match.tag("Success", (result) =>
                    Effect.gen(function* () {
                      // Durable handoff precedes the cursor: tell() only enqueues a command.
                      for (const batch of result.value.batches) storage.ingest(batch);
                      storage.markRetrieved(result.value.start, result.value.through);
                      cursor = result.value.through;
                      startup = false;
                      continueCatchUp = !result.value.caughtUp;
                      for (const batch of result.value.batches) {
                        const update = {
                          _tag: "Update" as const,
                          ...batch,
                        };
                        const relative = `chats/${update.chat.id}`;
                        const existing = yield* context.child(childActorName(relative));
                        const child =
                          (existing as ActorRef<ChatCommand> | undefined) ??
                          (yield* spawnContextChild(context, relative, LarkChatActor).pipe(
                            Effect.orDie,
                          ));
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
                          const relative = `chats/${day.chat.id}`;
                          const existing = yield* context.child(childActorName(relative));
                          const child =
                            (existing as ActorRef<ChatCommand> | undefined) ??
                            (yield* spawnContextChild(context, relative, LarkChatActor).pipe(
                              Effect.orDie,
                            ));
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
                      yield* registry.set({
                        path: "/lark/im",
                        description: string(entry.description) || "My work Lark messages",
                        state: {
                          ready: result.value.caughtUp,
                          chats: result.value.batches.length,
                        },
                        messages: [],
                      });
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
      });
    }),
  );
}
