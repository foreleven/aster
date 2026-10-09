import { chatView } from "../public-views.js";
import { ChatMessage } from "./service/model.js";
import type { ActorContext } from "@aster/actor";
import { ContextActor, contextPath, ContextSession } from "@aster/core";
import { Effect, Match, Ref } from "effect";
import { randomUUID } from "node:crypto";
import { ChatSummaryError } from "../shared/errors.js";
import { ChatCommands, ChatInternal, type ChatCommand } from "./chat/protocol.js";
import { ChatSnapshot, type ChatWork, type SummaryCommit } from "./chat/snapshot.js";
import { makeChatState } from "./chat/state.js";
import { ChatSummaryWork } from "./summary/work.js";

type Owner = ActorContext<ChatCommand>;

export const LarkChatActor = ContextActor.define("lark/ChatActor", {
  commands: ChatCommands,
  internal: ChatInternal,
})((owner) =>
  Effect.gen(function* () {
    const work = yield* ChatSummaryWork;
    const generation = randomUUID();
    const inFlight = yield* Ref.make(false);
    const changed = yield* Ref.make(false);
    const summarize = Effect.fn("LarkChatActor.summarize")(function* (context: Owner) {
      if (yield* Ref.getAndSet(inFlight, true)) return;
      yield* Ref.set(changed, false);
      const access = (value?: SummaryCommit) =>
        context.self
          .ask<ChatWork | undefined>((replyTo) =>
            value
              ? { _tag: "ApplySummary", generation, value, replyTo }
              : { _tag: "GetSummaryMessages", generation, replyTo },
          )
          .pipe(
            Effect.mapError((cause) => new ChatSummaryError({ cause, message: cause.message })),
            Effect.flatMap((snapshot) => (snapshot ? Effect.succeed(snapshot) : Effect.interrupt)),
          );
      yield* context.pipeToSelf(
        work.run({ path: contextPath(context), read: access(), commit: access }),
        (result) => ({ _tag: "Summarized", generation, result }),
      );
    });

    const path = contextPath(owner);
    const id = path.slice(path.lastIndexOf("/") + 1);
    const session = yield* ContextSession.make({
      path,
      state: ChatSnapshot,
      message: ChatMessage,
      view: chatView,
      changes: "durable-state",
      messageKey: (message) => message.id,
      compareMessages: (a, b) => Date.parse(a.at) - Date.parse(b.at) || a.id.localeCompare(b.id),
      initial: {
        description: `Work Lark conversation: ${id}`,
        state: { chat: { id, name: "", mode: "", description: "" }, seen: {} },
      },
    }).pipe(Effect.orDie);
    const state = makeChatState(session);
    yield* state.restore;

    return {
      started: (context) => summarize(context),

      receive: (command, context) =>
        Effect.gen(function* () {
          yield* Match.value(command).pipe(
            Match.tag("GetChatInfo", ({ replyTo }) =>
              state.work.pipe(Effect.flatMap((snapshot) => replyTo.tell(snapshot.chat))),
            ),
            Match.tag("GetChatSummary", ({ replyTo }) =>
              state.work.pipe(
                Effect.flatMap((snapshot) =>
                  replyTo.tell({ chat: snapshot.chat, summary: snapshot.summary ?? null }),
                ),
              ),
            ),
            Match.tag("GetSummaryMessages", (command) =>
              Effect.gen(function* () {
                if (command.generation !== generation)
                  return yield* command.replyTo.tell(undefined);
                const snapshot = yield* state.work;
                yield* command.replyTo.tell(snapshot);
              }),
            ),
            Match.tag("ApplySummary", (command) =>
              Effect.gen(function* () {
                if (command.generation !== generation)
                  return yield* command.replyTo.tell(undefined);
                const snapshot = yield* state.commit(command.value);
                yield* command.replyTo.tell(snapshot);
              }),
            ),
            Match.tag("RetainReceipts", ({ from }) => state.retain(from)),
            Match.tag("Update", (command) =>
              Effect.gen(function* () {
                const accepted = yield* state.accept(command);
                if (command.replyTo) yield* command.replyTo.tell(undefined);
                if (accepted) {
                  yield* Ref.set(changed, true);
                  yield* summarize(context);
                }
              }),
            ),
            Match.tag("Flush", ({ date }) =>
              Effect.gen(function* () {
                if (yield* state.flush(date)) {
                  yield* Ref.set(changed, true);
                  yield* summarize(context);
                }
              }),
            ),
            Match.tag("Summarized", (command) =>
              Effect.gen(function* () {
                if (command.generation !== generation) return;
                const recheck = yield* Match.value(command.result).pipe(
                  Match.tag("Failure", ({ error }) =>
                    Effect.logError({
                      event: "chat.summary.failed",
                      path: contextPath(context),
                      error: error.message,
                    }).pipe(Effect.as(false)),
                  ),
                  Match.tag("Success", ({ value }) => Effect.succeed(value)),
                  Match.exhaustive,
                );
                yield* Ref.set(inFlight, false);
                if (recheck || (yield* Ref.get(changed))) yield* summarize(context);
              }),
            ),
            Match.exhaustive,
          );
        }),
    };
  }),
).pipe(ContextActor.provide(ChatSummaryWork.layer));
