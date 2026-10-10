import { randomUUID } from "node:crypto";
import { CommandProcessor, type ActorContext, type ActorRef } from "@aster/actor";
import {
  childActorName,
  ContextActor,
  DurableContext,
  spawnContextChild,
  RuntimeConfigurationError,
} from "@aster/core";
import { Clock, Effect, Match, Schema } from "effect";
import { LarkSettings } from "../settings.js";
import { ChatPollError, queryError } from "../errors.js";
import { LarkImClient } from "./client.js";
import { ImState, ImPollResult, ChatMessage } from "./model.js";
import { LarkImCommands, listChats, readChatSummary } from "./queries.js";
import { LarkChatActor, Update, Flush, type ChatCommand } from "./chat/actor.js";
import { ChatState } from "./chat/state.js";
import { pollChats } from "./polling.js";
import { formatDate, dayStart } from "../dates.js";
import { chatPath, isChatPath } from "../paths.js";
const Internal = Schema.TaggedUnion({
  Poll: { generation: Schema.String },
  Polled: {
    generation: Schema.String,
    result: Schema.TaggedUnion({
      Success: { value: ImPollResult },
      Failure: { error: Schema.instanceOf(ChatPollError) },
    }),
  },
  HandedOff: {
    generation: Schema.String,
    value: ImPollResult,
    result: Schema.TaggedUnion({
      Success: { value: Schema.Void },
      Failure: { error: Schema.instanceOf(ChatPollError) },
    }),
  },
});
type ImCommand = typeof Internal.Type | (typeof LarkImCommands)[number]["Type"];
const chatActor = Effect.fnUntraced(function* (context: ActorContext<ImCommand>, id: string) {
  const relative = `chats/${id}`;
  const existing = yield* context.child(childActorName(relative));
  // This channel is the only creator at these names. Failed/stopping deliveries never acknowledge.
  if (existing) return existing as ActorRef<ChatCommand>;
  return yield* spawnContextChild(context, relative, LarkChatActor).pipe(Effect.orDie);
});
export const LarkImActor = ContextActor.define("lark/ImActor", {
  commands: LarkImCommands,
  internal: Internal,
})(
  Effect.gen(function* () {
    const generation = randomUUID();
    const registry = yield* DurableContext;
    const cli = yield* LarkImClient;
    const processor = yield* CommandProcessor.make({ concurrency: 2 });
    const settings = (yield* LarkSettings).im;
    if (!settings)
      return yield* new RuntimeConfigurationError({ message: "Lark IM is not configured" });
    const session = yield* registry
      .open({
        path: "/lark/im",
        state: ImState,
        message: Schema.Never,
        serialize: JSON.stringify,
        initial: { description: settings.description ?? "My work Lark messages", state: {} },
      })
      .pipe(Effect.orDie);
    const sessionStart = dayStart(formatDate(yield* Clock.currentTimeMillis));
    let startup = true;
    let busy = false;
    return {
      started: (context) =>
        Effect.gen(function* () {
          for (const { path } of registry.directory().filter(({ path }) => isChatPath(path))) {
            const retained = yield* registry
              .readOwnerSnapshot(path, { state: ChatState, message: ChatMessage })
              .pipe(Effect.orDie);
            if (retained && chatPath(retained.state.chat.id) !== path)
              return yield* Effect.die(new Error("Retained Chat identity mismatch"));
            if (retained && (retained.messages.length || retained.state.flushThrough))
              yield* chatActor(context, retained.state.chat.id);
          }
          yield* context.self.tell({ _tag: "Poll", generation });
        }),
      receive: (command, context) => {
        if ("generation" in command && command.generation !== generation) return Effect.void;
        return Match.value(command).pipe(
          Match.tag("list_chats", (request) =>
            processor.submit(request, context, listChats(request)),
          ),
          Match.tag("summary", (request) =>
            processor.submit(request, context, readChatSummary(request)),
          ),
          Match.tag("messages", (request) =>
            processor.submit(
              request,
              context,
              cli.listMessages(request).pipe(Effect.mapError(queryError)),
            ),
          ),
          Match.tag("Poll", () =>
            Effect.gen(function* () {
              if (busy) return;
              busy = true;
              const state = yield* session.state.get.pipe(Effect.orDie);
              const now = yield* Clock.currentTimeMillis;
              yield* context.pipeToSelf(
                pollChats(
                  cli,
                  state.through,
                  now,
                  startup,
                  sessionStart,
                  settings.config.catchUpWindowMs,
                ),
                (result) => ({ _tag: "Polled", generation, result }),
              );
            }),
          ),
          Match.tag("Polled", ({ result }) =>
            Match.value(result).pipe(
              Match.tag("Failure", ({ error }) =>
                Effect.gen(function* () {
                  busy = false;
                  yield* Effect.logWarning(error.message);
                  yield* context.pipeToSelf(Effect.sleep(settings.config.pollIntervalMs), () => ({
                    _tag: "Poll",
                    generation,
                  }));
                }),
              ),
              Match.tag("Success", ({ value }) =>
                Effect.gen(function* () {
                  const targets = yield* Effect.forEach(value.batches, (batch) =>
                    chatActor(context, batch.chat.id).pipe(Effect.map((ref) => ({ ref, batch }))),
                  );
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
                    (result) => ({ _tag: "HandedOff", generation, value, result }),
                  );
                }),
              ),
              Match.exhaustive,
            ),
          ),
          Match.tag("HandedOff", ({ value, result }) =>
            Effect.gen(function* () {
              busy = false;
              yield* Match.value(result).pipe(
                Match.tag("Failure", ({ error }) => Effect.logWarning(error.message)),
                Match.tag("Success", () =>
                  Effect.gen(function* () {
                    // Acceptance is acknowledged only after every affected Chat commits its batch.
                    yield* session.state
                      .update((state) => ({ ...state, through: value.through }), {
                        mode: "bootstrap",
                      })
                      .pipe(Effect.orDie);
                    startup = false;
                    const date = formatDate(dayStart(formatDate(value.through)) - 1);
                    for (const child of yield* context.children())
                      yield* (child as ActorRef<ChatCommand>).tell(new Flush({ date }));
                    yield* Effect.logInfo({
                      event: "lark.im.poll.completed",
                      chats: value.batches.length,
                      messages: value.batches.reduce(
                        (sum, batch) => sum + batch.messages.length,
                        0,
                      ),
                    });
                  }),
                ),
                Match.exhaustive,
              );
              if (result._tag === "Success" && !value.caughtUp)
                return yield* context.self.tell({ _tag: "Poll", generation });
              yield* context.pipeToSelf(Effect.sleep(settings.config.pollIntervalMs), () => ({
                _tag: "Poll",
                generation,
              }));
            }),
          ),
          Match.exhaustive,
        );
      },
    };
  }),
);
