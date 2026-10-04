import { chatView } from "../public-views.js";
import { isLarkWritebackEcho } from "./writeback.js";
import { randomUUID } from "node:crypto";
import { ReplyTo, type ActorContext } from "@aster/actor";
import { ImSummaryError } from "../shared/errors.js";
import { Clock, Effect, HashSet, Layer, Match, Predicate, Ref, Schema, Struct } from "effect";
import { ContextActor, contextPath, ContextRegistry, defineContext } from "@aster/core";
import { ImAgentQueue } from "./agent-queue.js";
import { ImSummaryGate } from "./summary-gate.js";
import { batchFingerprint, prepareChatSummary, SummaryCheckpoint } from "./summary-work.js";
import { ChatSummary, ChatSummarizer } from "./summarizer.js";
import { ImChat, ImMessage } from "./model.js";
import { imDate } from "./dates.js";
import { ImStorage, messageFingerprint, type SummaryCommit, type ChatDay } from "./storage.js";

const ChatState = Schema.Struct({ chat: ImChat, summary: Schema.optional(ChatSummary) });
const ChatMessages = Schema.Array(ImMessage);
const chatId = (path: string) => path.slice(path.lastIndexOf("/") + 1);

const needsSummary = (day: ChatDay): boolean =>
  Match.value(day).pipe(
    Match.when(
      (day) => day.commit !== undefined || day.stage !== undefined,
      () => true,
    ),
    Match.when({ pending: (messages) => messages.length === 0 }, () => false),
    Match.when(
      (day) => day.flush === true || day.assessment?.needed === true,
      () => true,
    ),
    Match.orElse((day) => day.assessment?.fingerprint !== batchFingerprint(day.pending)),
  );

const applyCheckpoint = Effect.fnUntraced(function* (day: ChatDay, change: SummaryCheckpoint) {
  const updated = yield* Match.value(change).pipe(
    Match.tag("Assessed", ({ fingerprint, needed }) =>
      Effect.succeed({ ...day, assessment: { fingerprint, needed } }),
    ),
    Match.tag("Stage", () =>
      Effect.succeed({ ...day, stage: day.stage ?? { batch: [...day.pending] } }),
    ),
    Match.tag("Daily", ({ summary }) =>
      Effect.gen(function* () {
        if (!day.stage)
          return yield* Effect.die(new Error("Daily summary checkpoint requires a frozen batch"));
        return { ...day, stage: { ...day.stage, daily: summary } };
      }),
    ),
    Match.exhaustive,
  );
  return Struct.omit(updated, ["lastError", "retryAt"]);
});

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
type ChatContext = Pick<ActorContext<ChatCommand>, "path" | "metadata" | "self" | "pipeToSelf">;
export class LarkChatActor extends ContextActor.Service<
  LarkChatActor,
  ChatSummarizer | ImStorage | ImAgentQueue | ImSummaryGate
>()("lark/ChatActor", {
  command: ChatCommand,
  context: defineContext({
    view: chatView,
    identity: "Work Lark conversation",
    state: ChatState,
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
      // Mailbox handlers are the only writers; workers acknowledge changes through Checkpoint.
      const busy = yield* Ref.make(false);
      const scheduled = yield* Ref.make(false);
      // A running actor may finish yesterday's work. A restarted actor resumes today only.
      const activeDates = yield* Ref.make(HashSet.empty<string>());
      const loadDay = Effect.fnUntraced(function* (path: string, date: string) {
        const day = storage.get(date, chatId(path));
        if (!day)
          return yield* Effect.die(new Error(`Missing IM checkpoint for ${path} on ${date}`));
        return day;
      });
      const scheduleSummary = Effect.fnUntraced(function* (
        context: Pick<ActorContext<ChatCommand>, "pipeToSelf">,
        delayMs: number,
      ) {
        yield* Ref.set(scheduled, true);
        yield* context.pipeToSelf(Effect.sleep(delayMs), () => ({ _tag: "Summarize" }));
      });
      const publishPending = Effect.fn("LarkChatActor.publishPending")(function* (
        path: string,
        chat: ImChat,
      ) {
        const previous = registry.get(path);
        const restored = yield* Schema.decodeUnknownEffect(ChatMessages)(
          previous?.messages ?? [],
        ).pipe(Effect.orDie);
        const summary = previous
          ? Schema.decodeUnknownSync(ChatState)(previous.state).summary
          : undefined;
        const messages = new Map(restored.map((message) => [message.id, message]));
        const dates = yield* Ref.get(activeDates);
        for (const date of [...dates].sort()) {
          const day = storage.get(date, chatId(path));
          if (!day) continue;
          for (const [key, message] of messages) {
            if (day.seen[key]?.fingerprint === messageFingerprint(message)) messages.delete(key);
          }
          for (const message of day.pending) messages.set(message.id, message);
        }
        yield* registry
          .commit(
            {
              path,
              description:
                previous?.description || `Work Lark conversation: ${chat.name || chat.id}`,
              state: {
                chat,
                ...(summary ? { summary } : {}),
              },
              messages: [...messages.values()].sort((a, b) => Date.parse(a.at) - Date.parse(b.at)),
            },
            { evaluate: false, expectedRevision: previous?.revision ?? 0 },
          )
          .pipe(Effect.asVoid, Effect.orDie);
      });
      const commitSummary = Effect.fn("LarkChatActor.commitSummary")(function* (
        path: string,
        date: string,
        commit: SummaryCommit,
      ) {
        const day = { ...(yield* loadDay(path, date)), commit };
        // Journal both outputs before touching the archive or public state. Replay after a crash.
        storage.save(day);
        storage.archive(day, commit);
        const current = registry.get(path);
        if (!current) return yield* Effect.die(new Error(`Missing IM Context for ${path}`));
        const messages = yield* Schema.decodeUnknownEffect(ChatMessages)(current.messages).pipe(
          Effect.orDie,
        );
        const covered = new Map(
          commit.batch.map((message) => [message.id, messageFingerprint(message)]),
        );
        yield* registry
          .commit(
            {
              ...current,
              state: { chat: day.chat, summary: commit.rolling },
              messages: messages.filter(
                (message) => covered.get(message.id) !== messageFingerprint(message),
              ),
            },
            { evaluate: commit.evaluate, expectedRevision: current.revision ?? 0 },
          )
          .pipe(Effect.asVoid, Effect.orDie);
        storage.finish(date, chatId(path), commit);
        yield* Effect.logInfo(
          JSON.stringify({
            event: "chat.summary.saved",
            path,
            date,
            covered: commit.batch.length,
          }),
        );
      });
      const completeSummary = Effect.fn("LarkChatActor.completeSummary")(function* (
        command: Extract<ChatCommand, { readonly _tag: "Summarized" }>,
        context: ChatContext,
      ) {
        const path = contextPath(context);
        if (command.generation !== generation) return;
        yield* Ref.set(busy, false);
        yield* Match.value(command.result).pipe(
          Match.tag("Failure", ({ error }) =>
            Effect.gen(function* () {
              const day = yield* loadDay(path, command.date);
              const now = yield* Clock.currentTimeMillis;
              storage.save({
                ...day,
                lastError: error.message,
                retryAt: new Date(now + 30_000).toISOString(),
              });
              yield* Effect.logError(
                JSON.stringify({
                  event: "chat.summary.failed",
                  path,
                  date: command.date,
                  error: error.message,
                }),
              );
              yield* scheduleSummary(context, 30_000);
            }),
          ),
          Match.tag("Success", ({ value }) =>
            Effect.gen(function* () {
              if (value) yield* commitSummary(path, command.date, value);
              yield* context.self.tell({ _tag: "Summarize" });
            }),
          ),
          Match.exhaustive,
        );
      });
      const summarize = Effect.fn("LarkChatActor.summarize")(function* (context: ChatContext) {
        const path = contextPath(context);
        const id = chatId(path);
        yield* Ref.set(scheduled, false);
        if (yield* Ref.get(busy)) return;
        const now = yield* Clock.currentTimeMillis;
        const dates = yield* Ref.get(activeDates);
        const candidates = [...dates]
          .sort()
          .map((date) => storage.get(date, id))
          .filter(Predicate.isNotUndefined)
          .filter(needsSummary)
          .map((day) => ({ day, readyAt: day.retryAt ? Date.parse(day.retryAt) : now }));
        const candidate = candidates.find(({ readyAt }) => readyAt <= now);
        if (!candidate) {
          if (candidates.length) {
            const next = Math.min(...candidates.map(({ readyAt }) => readyAt));
            yield* scheduleSummary(context, Math.max(1, next - now));
          }
          return;
        }
        const { day } = candidate;
        if (day.commit) {
          yield* commitSummary(path, day.date, day.commit);
          yield* context.self.tell({ _tag: "Summarize" });
          return;
        }
        yield* Ref.set(busy, true);
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
            isExternalInput: (message) => !isLarkWritebackEcho(registry, path, message),
            path,
            date,
            id,
            storage,
            gate,
            admission,
            summarizer,
            previous: () => {
              const current = registry.get(path);
              return current
                ? Schema.decodeUnknownSync(ChatState)(current.state).summary
                : undefined;
            },
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
                  Effect.mapError((cause) => new ImSummaryError({ cause, message: cause.message })),
                  Effect.flatMap((day) => (day ? Effect.succeed(day) : Effect.interrupt)),
                ),
          }),
          (result) => ({ _tag: "Summarized", date, generation, result }),
        );
      });
      return LarkChatActor.of({
        started: (context) =>
          Effect.gen(function* () {
            const path = contextPath(context);
            // Pending and queued work must survive idle periods longer than the admission backlog.
            const previous = registry.get(path);
            const today = imDate(yield* Clock.currentTimeMillis);
            yield* Ref.update(activeDates, HashSet.add(today));
            if (previous) {
              const { chat } = yield* Schema.decodeUnknownEffect(ChatState)(previous.state).pipe(
                Effect.orDie,
              );
              const messages = yield* Schema.decodeUnknownEffect(ChatMessages)(
                previous.messages,
              ).pipe(Effect.orDie);
              const pending = messages.filter((message) => imDate(message.at) === today);
              storage.ingest({ chat, messages: pending }, true);
              yield* publishPending(path, chat);
            } else {
              const day = storage.get(today, chatId(path));
              if (day) yield* publishPending(path, day.chat);
            }
            yield* context.self.tell({ _tag: "Summarize" });
          }),
        receive: (command, context) => {
          const path = contextPath(context);
          const id = chatId(path);
          return Match.value(command).pipe(
            Match.tag("Checkpoint", (command) =>
              Effect.gen(function* () {
                if (command.generation !== generation) {
                  yield* command.replyTo.tell(undefined);
                  return;
                }
                const day = yield* loadDay(path, command.date);
                const updated = yield* applyCheckpoint(day, command.change);
                storage.save(updated);
                yield* command.replyTo.tell(updated);
              }),
            ),
            Match.tag("Flush", (command) =>
              Effect.gen(function* () {
                const day = storage.get(command.date, id);
                if (!day) return;
                yield* Ref.update(activeDates, HashSet.add(command.date));
                storage.save({ ...day, flush: true });
                yield* publishPending(path, day.chat);
                if (!(yield* Ref.get(busy))) yield* context.self.tell({ _tag: "Summarize" });
              }),
            ),
            Match.tag("Update", (command) =>
              Effect.gen(function* () {
                // The IM actor already journals ingress before advancing its watermark.
                // Keeping this idempotent also supports direct Chat updates.
                const dates = storage.ingest(command);
                yield* Ref.update(activeDates, HashSet.union(HashSet.fromIterable(dates)));
                yield* publishPending(path, command.chat);
                yield* Effect.logInfo(
                  JSON.stringify({
                    event: "chat.messages",
                    path,
                    received: command.messages.length,
                  }),
                );
                if (!(yield* Ref.get(busy)) && !(yield* Ref.get(scheduled)))
                  yield* scheduleSummary(context, 1000);
              }),
            ),
            Match.tag("Summarized", (command) => completeSummary(command, context)),
            Match.tag("Summarize", () => summarize(context)),
            Match.exhaustive,
          );
        },
      });
    }),
  );
}
