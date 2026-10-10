import { Command, ReplyTo, type ActorContext, type MailboxOf } from "@aster/actor";
import { ContextActor, contextPath, DurableContext } from "@aster/core";
import { Clock, Effect, Match, Schema } from "effect";
import { randomUUID } from "node:crypto";
import { ChatInfo, ChatMessage } from "../model.js";
import { CalendarDate, formatDate } from "../../dates.js";
import { ChatSummaryError } from "../summary/errors.js";
import { makeChatSummaryWork } from "../summary/work.js";
import {
  ChatState,
  SummaryCommit,
  acceptChat,
  applySummary,
  chatWork,
  flushChat,
  serializeChat,
  type ChatWork,
} from "./state.js";

export class Update extends Command.Class<Update>()("Update", {
  payload: { chat: ChatInfo, messages: Schema.Array(ChatMessage) },
  reply: Schema.Void,
}) {}
export class Flush extends Command.Class<Flush>()("Flush", { payload: { date: CalendarDate } }) {}
const ChatCommands = [Update, Flush] as const;
const ChatInternal = Schema.TaggedUnion({
  ApplySummary: {
    generation: Schema.String,
    value: SummaryCommit,
    replyTo: ReplyTo<ChatWork | undefined>(),
  },
  Summarized: {
    generation: Schema.String,
    result: Schema.TaggedUnion({
      Success: { value: Schema.Boolean },
      Failure: { error: Schema.instanceOf(ChatSummaryError) },
    }),
  },
});
export type ChatCommand = MailboxOf<typeof ChatCommands, typeof ChatInternal>;
type Owner = ActorContext<ChatCommand>;

export const LarkChatActor = ContextActor.define("lark/ChatActor", {
  commands: ChatCommands,
  internal: ChatInternal,
})((owner) =>
  Effect.gen(function* () {
    const path = contextPath(owner);
    const id = path.slice(path.lastIndexOf("/") + 1);
    const session = yield* (yield* DurableContext)
      .open({
        path,
        state: ChatState,
        message: ChatMessage,
        keyOf: (message) => message.id,
        serialize: serializeChat,
        events: "state",
        initial: {
          description: `Work Lark conversation: ${id}`,
          state: { chat: { id, name: "", mode: "", description: "" } },
        },
      })
      .pipe(Effect.orDie);
    const work = yield* makeChatSummaryWork;
    const generation = randomUUID();
    let inFlight = false;
    let changed = false;
    const read = session.snapshot.pipe(
      Effect.map(chatWork),
      Effect.mapError(
        (cause) => new ChatSummaryError({ message: "Chat snapshot unavailable", cause }),
      ),
    );
    const idle = Effect.fn("LarkChatActor.idle")(function* (context: Owner) {
      const pending = (yield* session.snapshot.pipe(Effect.orDie)).messages.length > 0;
      yield* context.receiveTimeout(inFlight || pending ? undefined : "5 minutes");
    });
    const summarize = Effect.fn("LarkChatActor.summarize")(function* (context: Owner) {
      if (inFlight) return;
      inFlight = true;
      changed = false;
      yield* context.receiveTimeout(undefined);
      yield* context.pipeToSelf(
        work.run({
          path,
          read,
          commit: (value) =>
            context.self
              .ask<ChatWork | undefined>((replyTo) => ({
                _tag: "ApplySummary",
                generation,
                value,
                replyTo,
              }))
              .pipe(
                Effect.mapError((cause) => new ChatSummaryError({ message: cause.message, cause })),
                Effect.flatMap((snapshot) =>
                  snapshot ? Effect.succeed(snapshot) : Effect.interrupt,
                ),
              ),
        }),
        (result) => ({ _tag: "Summarized", generation, result }),
      );
    });
    return {
      started: (context) =>
        Effect.gen(function* () {
          const today = formatDate(yield* Clock.currentTimeMillis);
          const old = (yield* session.snapshot.pipe(Effect.orDie)).messages
            .map((m) => formatDate(m.at))
            .filter((date) => date < today)
            .sort()
            .at(-1);
          if (old)
            yield* session
              .commit((current) => flushChat(current, old), { mode: "bootstrap" })
              .pipe(Effect.orDie);
          yield* summarize(context);
        }),
      receive: (command, context) =>
        Match.value(command).pipe(
          Match.tag("Update", (command) =>
            Effect.gen(function* () {
              let accepted = false;
              yield* session
                .commit(
                  (current) => {
                    const plan = acceptChat(current, command);
                    accepted = (plan.messages?.append?.length ?? 0) > 0;
                    return plan;
                  },
                  {
                    mode: "bootstrap",
                    description: `Work Lark conversation: ${command.chat.name || command.chat.id}`,
                  },
                )
                .pipe(Effect.orDie);
              yield* command.replyTo.tell(undefined);
              if (accepted) {
                changed = true;
                yield* summarize(context);
              }
              yield* idle(context);
            }),
          ),
          Match.tag("Flush", ({ date }) =>
            Effect.gen(function* () {
              const before = yield* session.snapshot.pipe(Effect.orDie);
              const after = yield* session
                .commit((current) => flushChat(current, date), { mode: "bootstrap" })
                .pipe(Effect.orDie);
              if (after.revision !== before.revision) {
                changed = true;
                yield* summarize(context);
              }
              yield* idle(context);
            }),
          ),
          Match.tag("ApplySummary", ({ generation: expected, value, replyTo }) =>
            Effect.gen(function* () {
              if (expected !== generation) return yield* replyTo.tell(undefined);
              const snapshot = yield* session
                .commit((current) => applySummary(current, value))
                .pipe(Effect.orDie);
              yield* replyTo.tell(chatWork(snapshot));
            }),
          ),
          Match.tag("Summarized", (command) =>
            Effect.gen(function* () {
              if (command.generation !== generation) return;
              const again = yield* Match.value(command.result).pipe(
                Match.tag("Failure", ({ error }) =>
                  Effect.logError({
                    event: "chat.summary.failed",
                    path,
                    error: error.message,
                  }).pipe(Effect.as(false)),
                ),
                Match.tag("Success", ({ value }) => Effect.succeed(value)),
                Match.exhaustive,
              );
              inFlight = false;
              if (again || changed) yield* summarize(context);
              yield* idle(context);
            }),
          ),
          Match.exhaustive,
        ),
    };
  }),
);
