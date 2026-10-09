import { imChannelView } from "../public-views.js";
import { LarkConfig } from "../config.js";
import { ImSnapshot } from "./channel/snapshot.js";
import { LarkChatService } from "./service/chat-service.js";
import { CommandProcessor, type ActorContext, type ActorRef } from "@aster/actor";
import {
  childActorName,
  ContextActor,
  ContextSession,
  contextPath,
  spawnContextChild,
} from "@aster/core";
import { Effect, Match, Ref, Schema } from "effect";
import { LarkImCommands, listChats, readChatSummary } from "./queries.js";
import { LarkChatActor } from "./chat-actor.js";
import { Update, Flush } from "./chat/protocol.js";
import type { ChatCommand } from "./chat/protocol.js";
import { ImInternal, type ImCommand } from "./channel/protocol.js";
import { makeImState } from "./channel/state.js";

import { ChatPollError } from "../shared/errors.js";

// This channel owns every child at this name; the generic Actor lookup cannot
// recover its command type. Keep that assertion at the lookup boundary.
const chatActor = Effect.fnUntraced(function* (context: ActorContext<ImCommand>, id: string) {
  const relative = `chats/${id}`;
  const existing = yield* context.child(childActorName(relative));
  if (existing) return existing as ActorRef<ChatCommand>;
  return (yield* spawnContextChild(context, relative, LarkChatActor).pipe(
    Effect.orDie,
  )) as ActorRef<ChatCommand>;
});

export const LarkImActor = ContextActor.define("lark/ImActor", {
  commands: LarkImCommands,
  internal: ImInternal,
})((owner) =>
  Effect.gen(function* () {
    const cli = yield* LarkChatService;
    const processor = yield* CommandProcessor.make({ concurrency: 2 });
    const config = yield* LarkConfig;
    const session = yield* ContextSession.make({
      path: contextPath(owner),
      state: ImSnapshot,
      message: Schema.Never,
      view: imChannelView,
      initial: {
        description: config.im?.description || "My work Lark messages",
        state: { ready: false, chats: 0 },
      },
    }).pipe(Effect.orDie);
    const state = yield* makeImState(session);
    const busy = yield* Ref.make(false);
    const schedule = (context: ActorContext<ImCommand>) =>
      context.pipeToSelf(Effect.sleep(state.interval), () => ({ _tag: "Poll" }));
    return {
      started: (context) =>
        Effect.gen(function* () {
          for (const id of yield* state.restore) yield* chatActor(context, id);
          yield* context.self.tell({ _tag: "Poll" });
        }),
      receive: (command, context) =>
        Match.value(command).pipe(
          Match.tag("list_chats", (request) =>
            processor.submit(request, context, listChats(request, context)),
          ),
          Match.tag("summary", (request) =>
            processor.submit(request, context, readChatSummary(request, context)),
          ),
          Match.tag("messages", (request) =>
            processor.submit(request, context, cli.listMessages(request)),
          ),
          Match.tag("Poll", () =>
            Effect.gen(function* () {
              if (yield* Ref.getAndSet(busy, true)) return;
              yield* Effect.logInfo({ event: "lark.im.poll.started" });
              yield* context.pipeToSelf(state.poll, (result) => ({ _tag: "Polled", result }));
            }),
          ),
          Match.tag("Polled", ({ result }) =>
            Match.value(result).pipe(
              Match.tag("Failure", ({ error }) =>
                Effect.gen(function* () {
                  yield* Ref.set(busy, false);
                  yield* state.failed(error);
                  yield* schedule(context);
                }),
              ),
              Match.tag("Success", ({ value }) =>
                Effect.gen(function* () {
                  const targets = yield* Effect.forEach(value.batches, (batch) =>
                    chatActor(context, batch.chat.id).pipe(Effect.map((ref) => ({ ref, batch }))),
                  );
                  // Await durable acceptance in a scoped worker, keeping the channel mailbox free.
                  yield* context.pipeToSelf(
                    Effect.forEach(
                      targets,
                      ({ ref, batch }) =>
                        ref.ask<void>((replyTo) => new Update({ ...batch, replyTo })),
                      { discard: true },
                    ).pipe(
                      Effect.mapError(
                        (cause) => new ChatPollError({ cause, message: cause.message }),
                      ),
                    ),
                    (result) => ({ _tag: "HandedOff", value, result }),
                  );
                }),
              ),
              Match.exhaustive,
            ),
          ),
          Match.tag("HandedOff", ({ value, result }) =>
            Effect.gen(function* () {
              yield* Ref.set(busy, false);
              const continueCatchUp = yield* Match.value(result).pipe(
                Match.tag("Failure", ({ error }) => state.failed(error).pipe(Effect.as(false))),
                Match.tag("Success", () =>
                  Effect.gen(function* () {
                    const { flushThrough, retainFrom } = yield* state.accept(value);
                    for (const child of yield* context.children()) {
                      const chat = child as ActorRef<ChatCommand>;
                      yield* chat.tell({ _tag: "RetainReceipts", from: retainFrom });
                      yield* chat.tell(new Flush({ date: flushThrough }));
                    }
                    yield* state.completed(value);
                    return !value.caughtUp;
                  }),
                ),
                Match.exhaustive,
              );
              if (continueCatchUp) return yield* context.self.tell({ _tag: "Poll" });
              yield* schedule(context);
            }),
          ),
          Match.exhaustive,
        ),
    };
  }),
);
