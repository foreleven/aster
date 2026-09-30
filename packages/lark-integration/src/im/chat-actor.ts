import { randomUUID } from "node:crypto";
import { ReplyTo } from "@aster/actor";
import { ImSummaryError } from "../shared/errors.js";
import { Clock, Effect, Layer, Match, Schema } from "effect";
import { ContextActor, contextPath, ContextRegistry, defineContext } from "@aster/core";
import { ImAgentQueue } from "./agent-queue.js";
import { ImSummaryGate } from "./summary-gate.js";
import { batchFingerprint, prepareChatSummary, SummaryCheckpoint } from "./summary-work.js";
import { ChatSummary, ChatSummarizer } from "./summarizer.js";
import { ImChat, ImMessage } from "./model.js";
import { object } from "../shared/response.js";
import { imDate } from "./dates.js";
import { ImStorage, messageFingerprint, type SummaryCommit, type ChatDay } from "./storage.js";

const Commit = Schema.Struct({
  batch: Schema.Array(ImMessage),
  daily: ChatSummary,
  rolling: ChatSummary,
  evaluate: Schema.Boolean,
  updatedAt: Schema.String,
});
const ChatCommand = Schema.Union([
  Schema.TaggedStruct("Update", {
    chat: ImChat,
    messages: Schema.Array(ImMessage),
  }),
  Schema.TaggedStruct("Summarize", {}),
  Schema.TaggedStruct("Flush", { date: Schema.String }),
  Schema.TaggedStruct("Checkpoint", {
    date: Schema.String,
    generation: Schema.String,
    change: SummaryCheckpoint,
    replyTo: ReplyTo<ChatDay | undefined>(),
  }),
  Schema.TaggedStruct("Summarized", {
    generation: Schema.String,
    date: Schema.String,
    result: Schema.Union([
      Schema.TaggedStruct("Success", { value: Schema.optional(Commit) }),
      Schema.TaggedStruct("Failure", { error: Schema.instanceOf(ImSummaryError) }),
    ]),
  }),
]);
export type ChatCommand = typeof ChatCommand.Type;
export class LarkChatActor extends ContextActor.Service<
  LarkChatActor,
  ChatSummarizer | ImStorage | ImAgentQueue | ImSummaryGate
>()("lark/ChatActor", {
  command: ChatCommand,
  context: defineContext({
    identity: "Work Lark conversation",
    state: Schema.Struct({ chat: ImChat, summary: Schema.optional(ChatSummary) }),
    message: ImMessage,
    signalSource: true,
  }),
}) {
  static readonly layer = Layer.effect(
    LarkChatActor,
    Effect.gen(function* () {
      const registry = yield* ContextRegistry;
      const summarizer = yield* ChatSummarizer;
      const storage = yield* ImStorage;
      const admission = yield* ImAgentQueue;
      const gate = yield* ImSummaryGate;
      const generation = randomUUID();
      let path = "",
        id = "";
      let busy = false,
        scheduled = false;
      // A running actor may finish yesterday's work. A restarted actor resumes today only.
      const activeDates = new Set<string>();
      const publishPending = (chat: ImChat) =>
        Effect.gen(function* () {
          const previous = registry.get(path);
          const messages = new Map(
            ((previous?.messages ?? []) as readonly ImMessage[]).map((message) => [
              message.id,
              message,
            ]),
          );
          for (const date of activeDates) {
            const day = storage.get(date, id);
            if (!day) continue;
            for (const [key, message] of messages) {
              if (day.seen[key]?.fingerprint === messageFingerprint(message)) messages.delete(key);
            }
            for (const message of day.pending) messages.set(message.id, message);
          }
          yield* registry.set(
            {
              path,
              description:
                previous?.description || `Work Lark conversation: ${chat.name || chat.id}`,
              state: {
                chat,
                ...(object(previous?.state).summary
                  ? { summary: object(previous?.state).summary }
                  : {}),
              },
              messages: [...messages.values()].sort((a, b) => Date.parse(a.at) - Date.parse(b.at)),
            },
            { evaluate: false },
          );
        });
      const commitSummary = (date: string, commit: SummaryCommit) =>
        Effect.gen(function* () {
          const day = storage.get(date, id)!;
          // Journal both outputs before touching the archive or public state. Replay after a crash.
          day.commit = commit;
          storage.save(day);
          storage.archive(day, commit);
          const current = registry.get(path)!;
          const covered = new Map(
            commit.batch.map((message) => [message.id, messageFingerprint(message)]),
          );
          yield* registry.set(
            {
              ...current,
              state: { chat: day.chat, summary: commit.rolling },
              messages: (current.messages as readonly ImMessage[]).filter(
                (message) => covered.get(message.id) !== messageFingerprint(message),
              ),
            },
            { evaluate: commit.evaluate },
          );
          storage.finish(date, id, commit);
          yield* Effect.logInfo(
            JSON.stringify({
              event: "chat.summary.saved",
              path,
              date,
              covered: commit.batch.length,
            }),
          );
        });
      return LarkChatActor.of({
        started: (context) =>
          Effect.gen(function* () {
            path = contextPath(context);
            id = path.split("/").at(-1)!;
            // Pending and queued work must survive idle periods longer than the admission backlog.
            const previous = registry.get(path);
            const today = imDate(yield* Clock.currentTimeMillis);
            activeDates.add(today);
            if (previous) {
              const chat = Schema.decodeUnknownSync(ImChat)(object(previous.state).chat);
              const pending = (previous.messages as readonly ImMessage[]).filter(
                (message) => imDate(message.at) === today,
              );
              storage.ingest({ chat, messages: pending }, true);
              yield* publishPending(chat);
            } else {
              const day = storage.get(today, id);
              if (day) yield* publishPending(day.chat);
            }
            yield* context.self.tell({ _tag: "Summarize" });
          }),
        receive: (command, context) =>
          Effect.gen(function* () {
            if (command._tag === "Checkpoint") {
              if (command.generation !== generation) {
                yield* command.replyTo.tell(undefined);
                return;
              }
              const day = storage.get(command.date, id)!;
              Match.value(command.change).pipe(
                Match.tag("Assessed", ({ fingerprint, needed }) => {
                  day.assessment = { fingerprint, needed };
                }),
                Match.tag("Stage", () => {
                  day.stage ??= { batch: [...day.pending] };
                }),
                Match.tag("Daily", ({ summary }) => {
                  day.stage!.daily = summary;
                }),
                Match.exhaustive,
              );
              delete day.lastError;
              delete day.retryAt;
              storage.save(day);
              yield* command.replyTo.tell(day);
              return;
            }
            if (command._tag === "Flush") {
              const day = storage.get(command.date, id);
              if (day) {
                activeDates.add(command.date);
                day.flush = true;
                storage.save(day);
                yield* publishPending(day.chat);
                if (!busy) yield* context.self.tell({ _tag: "Summarize" });
              }
              return;
            }
            if (command._tag === "Update") {
              // The IM actor already journals ingress before advancing its watermark.
              // Keeping this idempotent also supports direct Chat updates.
              for (const date of storage.ingest(command)) activeDates.add(date);
              yield* publishPending(command.chat);
              yield* Effect.logInfo(
                JSON.stringify({ event: "chat.messages", path, received: command.messages.length }),
              );
              if (!busy && !scheduled) {
                scheduled = true;
                yield* context.pipeToSelf(Effect.sleep("1 second"), () => ({ _tag: "Summarize" }));
              }
              return;
            }
            if (command._tag === "Summarized") {
              if (command.generation !== generation) return;
              busy = false;
              if (command.result._tag === "Failure") {
                const day = storage.get(command.date, id)!;
                day.lastError = command.result.error.message;
                day.retryAt = new Date((yield* Clock.currentTimeMillis) + 30_000).toISOString();
                storage.save(day);
                yield* Effect.logError(
                  JSON.stringify({
                    event: "chat.summary.failed",
                    path,
                    date: command.date,
                    error: day.lastError,
                  }),
                );
                scheduled = true;
                yield* context.pipeToSelf(Effect.sleep("30 seconds"), () => ({
                  _tag: "Summarize",
                }));
                return;
              }
              if (command.result.value) yield* commitSummary(command.date, command.result.value);
              yield* context.self.tell({ _tag: "Summarize" });
              return;
            }
            scheduled = false;
            if (busy) return;
            const now = yield* Clock.currentTimeMillis;
            const candidates = [...activeDates]
              .sort()
              .map((date) => storage.get(date, id))
              .filter(
                (day) =>
                  day &&
                  (day.commit ||
                    day.stage ||
                    (day.pending.length &&
                      (day.flush ||
                        day.assessment?.needed ||
                        day.assessment?.fingerprint !== batchFingerprint(day.pending)))),
              );
            const day = candidates.find(
              (day) => day && (!day.retryAt || Date.parse(day.retryAt) <= now),
            );
            if (!day && candidates.length) {
              const next = Math.min(...candidates.map((day) => Date.parse(day!.retryAt!)));
              if (!scheduled) {
                scheduled = true;
                yield* context.pipeToSelf(Effect.sleep(Math.max(1, next - now)), () => ({
                  _tag: "Summarize",
                }));
              }
            }
            if (!day) return;
            if (day.commit) {
              yield* commitSummary(day.date, day.commit);
              yield* context.self.tell({ _tag: "Summarize" });
              return;
            }
            busy = true;
            const date = day.date;
            yield* Effect.logInfo(
              JSON.stringify({
                event: "chat.summary.requested",
                path,
                date,
                messages: day.pending.length,
              }),
            );
            yield* context.pipeToSelf(
              prepareChatSummary({
                path,
                date,
                id,
                storage,
                gate,
                admission,
                summarizer,
                previous: () =>
                  object(registry.get(path)?.state).summary as ChatSummary | undefined,
                checkpoint: (change) =>
                  context.self
                    .ask<ChatDay | undefined>((replyTo) => ({
                      _tag: "Checkpoint",
                      date,
                      generation,
                      change,
                      replyTo,
                    }))
                    .pipe(
                      Effect.mapError(
                        (cause) => new ImSummaryError({ cause, message: cause.message }),
                      ),
                      Effect.flatMap((day) => (day ? Effect.succeed(day) : Effect.interrupt)),
                    ),
              }),
              (result) => ({ _tag: "Summarized", date, generation, result }),
            );
          }),
      });
    }),
  );
}
