import { type ActorContext, type ActorRef } from "@aster/actor";
import { childActorName, ContextActor, spawnContextChild } from "@aster/core";
import { Effect, Match, Ref } from "effect";
import { LarkImCommands, makeLarkImQuery } from "../queries.js";
import { LarkChatActor } from "./chat-actor.js";
import { Update, Flush } from "./chat/protocol.js";
import type { ChatCommand } from "./chat/protocol.js";
import { ImInternal, type ImCommand } from "./channel/protocol.js";
import { ImState } from "./channel/state.js";
import { ImContext } from "./channel/snapshot.js";
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
  context: ImContext,
})(
  Effect.gen(function* () {
    const query = yield* makeLarkImQuery;
    const state = yield* ImState;
    const busy = yield* Ref.make(false);
    const schedule = (context: ActorContext<ImCommand>) =>
      context.pipeToSelf(Effect.sleep(state.interval), () => ({ _tag: "Poll" }));
    return {
      query,
      started: (context) =>
        Effect.gen(function* () {
          for (const id of yield* state.restore) yield* chatActor(context, id);
          yield* context.self.tell({ _tag: "Poll" });
        }),
      receive: (command, context) =>
        Match.value(command).pipe(
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
                    const { flushes, retention } = yield* state.accept(value);
                    for (const chat of retention.chats) {
                      const child = yield* chatActor(context, chat);
                      yield* child.tell({ _tag: "RetainReceipts", from: retention.from });
                    }
                    for (const { chat, date } of flushes) {
                      const child = yield* chatActor(context, chat);
                      yield* child.tell(new Flush({ date }));
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
).pipe(ContextActor.provide(ImState.layer));
