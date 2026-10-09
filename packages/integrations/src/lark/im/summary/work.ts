import { validateConfig } from "@aster/core";
import { Context, Effect, Layer, Ref, Schedule, Schema } from "effect";
import { LarkConfig } from "../../config.js";
import { ChatSummaryBatchConfig } from "../config.js";
import { formatDate } from "../service/dates.js";
import { batchFingerprint, type ChatWork, type SummaryCommit } from "../chat/snapshot.js";
import { ChatSummaryGate } from "./gate.js";
import { ImAgentQueue } from "./agent-queue.js";
import { ChatSummarizer, type ChatSummaryInput } from "./summarizer.js";
import type { ChatSummaryError } from "../../shared/errors.js";

const transientRetry = {
  schedule: Schedule.max([Schedule.spaced("30 seconds"), Schedule.recurs(3)]),
  while: (error: ChatSummaryError) => error.kind === "transient",
};

/** One Behavior-scoped workflow owns assessment and model retries; all writes use the mailbox. */
const makeChatSummaryWork = Effect.gen(function* () {
  const gate = yield* ChatSummaryGate;
  const admission = yield* ImAgentQueue;
  const summarizer = yield* ChatSummarizer;
  const config = yield* LarkConfig;
  const settings = yield* validateConfig("Lark IM summary batches", () =>
    Schema.decodeUnknownSync(ChatSummaryBatchConfig)(config.im?.config?.summary ?? {}),
  );
  const deferred = yield* Ref.make<string | undefined>(undefined);
  const run = Effect.fn("ChatSummaryWork.run")(function* (input: {
    path: string;
    read: Effect.Effect<ChatWork, ChatSummaryError>;
    commit: (result: SummaryCommit) => Effect.Effect<ChatWork, ChatSummaryError>;
  }) {
    let current = yield* input.read;
    if (!current.pending.length) return false;
    const fingerprint = batchFingerprint(current.pending);
    if (!current.flushThrough && current.pending.length <= settings.maxMessages) {
      if ((yield* Ref.get(deferred)) === fingerprint) return false;
      const needed = yield* gate
        .needed({
          path: input.path,
          chat: current.chat,
          messages: current.pending,
          previous: current.summary,
        })
        .pipe(
          Effect.retry(transientRetry),
          // A capacity failure cannot safely be assessed using only a subset of the evidence.
          Effect.catchTag("ChatSummaryError", (error) =>
            error.kind === "capacity" ? Effect.succeed(true) : Effect.fail(error),
          ),
        );
      if (!needed) {
        yield* Ref.set(deferred, fingerprint);
        return false;
      }
    }
    yield* Ref.set(deferred, undefined);
    let remaining: Set<string> | undefined;
    let batchLimit = settings.maxMessages;
    do {
      let selected: ChatSummaryInput | undefined;
      const attempt = admission.run(
        current.chat.id,
        Effect.gen(function* () {
          if (!selected) {
            // Arrivals queued before admission join this input. Retries keep the selected versions.
            const latest = yield* input.read;
            remaining ??= new Set(
              latest.pending
                .filter(
                  (message) =>
                    !latest.flushThrough || formatDate(message.at) <= latest.flushThrough,
                )
                .map((message) => message.id),
            );
            selected = {
              path: input.path,
              chat: latest.chat,
              previous: latest.summary,
              messages: latest.pending.slice(0, batchLimit),
            };
          }
          if (!selected.messages.length) return undefined;
          yield* Effect.logInfo({
            event: "chat.summary.started",
            path: input.path,
            messages: selected.messages.length,
          });
          const rolling = yield* summarizer.summarize(selected);
          return { batch: [...selected.messages], rolling };
        }),
      );
      const result = yield* attempt.pipe(
        // Admission releases its permit before retry waits or smaller-input attempts.
        Effect.retry(transientRetry),
        Effect.retry({
          times: Math.ceil(Math.log2(settings.maxMessages)),
          while: (error) =>
            Effect.gen(function* () {
              if (error.kind !== "capacity" || !selected || selected.messages.length <= 1)
                return false;
              batchLimit = Math.max(1, Math.floor(selected.messages.length / 2));
              selected = { ...selected, messages: selected.messages.slice(0, batchLimit) };
              yield* Effect.logWarning({
                event: "chat.summary.reduced",
                path: input.path,
                messages: selected.messages.length,
              });
              return true;
            }),
        }),
      );
      if (!result) return false;
      current = yield* input.commit(result);
      for (const message of result.batch) remaining?.delete(message.id);
      // Finish the admitted backlog. Edits of covered messages and later arrivals get a fresh assessment.
    } while (current.pending.some((message) => remaining?.has(message.id)));
    return current.pending.length > 0;
  });
  return { run };
});
export class ChatSummaryWork extends Context.Service<
  ChatSummaryWork,
  Effect.Success<typeof makeChatSummaryWork>
>()("lark/im/ChatSummaryWork") {
  static readonly layer = Layer.effect(ChatSummaryWork, makeChatSummaryWork);
}
