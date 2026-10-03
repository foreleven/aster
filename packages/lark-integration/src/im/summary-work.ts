import { createHash } from "node:crypto";
import { Clock, Effect, Schema } from "effect";
import type { ImStorage, ChatDay } from "./storage.js";
import type { ImSummaryGate } from "./summary-gate.js";
import type { AgentAdmission } from "./agent-queue.js";
import { ChatSummary, type ChatSummarizer } from "./summarizer.js";
import { ImMessage } from "./model.js";
import type { ImSummaryError } from "../shared/errors.js";

export const batchFingerprint = (messages: readonly ImMessage[]) =>
  createHash("sha256").update(JSON.stringify(messages)).digest("hex");

export const SummaryCheckpoint = Schema.Union([
  Schema.TaggedStruct("Assessed", { fingerprint: Schema.String, needed: Schema.Boolean }),
  Schema.TaggedStruct("Stage", {}),
  Schema.TaggedStruct("Daily", { summary: ChatSummary }),
]);
export type SummaryCheckpoint = typeof SummaryCheckpoint.Type;

const FrozenStage = Schema.Struct({
  batch: Schema.Array(ImMessage),
  daily: Schema.optional(ChatSummary),
});
const PreparedStage = Schema.Struct({ batch: Schema.Array(ImMessage), daily: ChatSummary });

/** Model work stays in the caller Fiber; only the Chat mailbox commits checkpoints. */
export const prepareChatSummary = Effect.fn("prepareChatSummary")(function* (input: {
  path: string;
  date: string;
  id: string;
  storage: ImStorage["Service"];
  gate: ImSummaryGate["Service"];
  admission: AgentAdmission;
  summarizer: ChatSummarizer["Service"];
  previous: () => ChatSummary | undefined;
  isExternalInput?: (message: ImMessage) => boolean;
  checkpoint: (change: SummaryCheckpoint) => Effect.Effect<ChatDay, ImSummaryError>;
}) {
  const { storage, date, id, path } = input;
  let day = storage.get(date, id)!;
  if (day.commit) return day.commit;
  if (!day.stage && !day.flush && !day.assessment?.needed) {
    const fingerprint = batchFingerprint(day.pending);
    if (day.assessment?.fingerprint === fingerprint) return undefined;
    const messages = day.pending.filter(input.isExternalInput ?? (() => true));
    const needed =
      messages.length > 0 &&
      (yield* input.gate.needed({
        path,
        chat: day.chat,
        date,
        messages,
        previous: input.previous(),
      }));
    day = yield* input.checkpoint({ _tag: "Assessed", fingerprint, needed });
    if (!needed && !day.flush) return undefined;
  }
  if (!day.stage?.daily) {
    yield* input.admission.run(
      id,
      Effect.gen(function* () {
        // Freeze after admission, including arrivals received while this chat was queued.
        const current = yield* input.checkpoint({ _tag: "Stage" });
        // A successful mailbox acknowledgement must include the frozen batch.
        const stage = yield* Schema.decodeUnknownEffect(FrozenStage)(current.stage).pipe(
          Effect.orDie,
        );
        yield* Effect.logInfo(
          JSON.stringify({
            event: "chat.summary.started",
            path,
            date,
            stage: "daily",
            messages: stage.batch.length,
          }),
        );
        const daily = yield* input.summarizer.summarize({
          path,
          chat: current.chat,
          date,
          previous: current.summary,
          messages: stage.batch,
        });
        yield* input.checkpoint({ _tag: "Daily", summary: daily });
      }),
    );
  }
  return yield* input.admission.run(
    id,
    Effect.gen(function* () {
      const current = storage.get(date, id)!;
      const stage = yield* Schema.decodeUnknownEffect(PreparedStage)(current.stage).pipe(
        Effect.orDie,
      );
      yield* Effect.logInfo(
        JSON.stringify({
          event: "chat.summary.started",
          path,
          date,
          stage: "rolling",
          messages: stage.batch.length,
        }),
      );
      const rolling = yield* input.summarizer.summarize({
        path,
        chat: current.chat,
        previous: input.previous(),
        messages: stage.batch,
      });
      return {
        batch: stage.batch,
        daily: stage.daily,
        rolling,
        evaluate: stage.batch.some(input.isExternalInput ?? (() => true)),
        updatedAt: new Date(yield* Clock.currentTimeMillis).toISOString(),
      };
    }),
  );
});
