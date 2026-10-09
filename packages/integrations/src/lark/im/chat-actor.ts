import type { ActorContext } from "@aster/actor";
import { ContextActor, contextPath } from "@aster/core";
import { Context, Deferred, Effect, Layer, Match, Ref } from "effect";
import { randomUUID } from "node:crypto";
import { ChatSummaryError } from "../shared/errors.js";
import { ChatCommands, ChatInternal, type ChatCommand } from "./chat/protocol.js";
import { ChatContext, type ChatWork, type SummaryCommit } from "./chat/snapshot.js";
import { ChatState } from "./chat/state.js";
import { ChatSummaryWork } from "./summary/work.js";

type Owner = ActorContext<ChatCommand>;

export const LarkChatActor = ContextActor.define("lark/ChatActor", {
  commands: ChatCommands,
  internal: ChatInternal,
  context: ChatContext,
})(
  Effect.gen(function* () {
    const work = yield* ChatSummaryWork;
    const scope = yield* Effect.scope;
    const initialized = yield* Deferred.make<ChatState["Service"]>();
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
              : { _tag: "ReadInput", generation, replyTo },
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
    return {
      started: (context) =>
        Effect.gen(function* () {
          const services = yield* Layer.buildWithScope(
            ChatState.layer(contextPath(context)),
            scope,
          );
          const state = Context.get(services, ChatState);
          yield* state.restore;
          yield* Deferred.succeed(initialized, state);
          yield* summarize(context);
        }),
      receive: (command, context) =>
        Effect.gen(function* () {
          const state = yield* Deferred.await(initialized);
          yield* Match.value(command).pipe(
            Match.tag("ReadInput", "ApplySummary", (command) =>
              Effect.gen(function* () {
                if (command.generation !== generation)
                  return yield* command.replyTo.tell(undefined);
                const snapshot =
                  command._tag === "ApplySummary"
                    ? yield* state.commit(command.value)
                    : yield* state.work;
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
