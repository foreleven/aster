import { Schema, Struct } from "effect";
import type { ContextMutation, ContextOwnerSnapshot } from "@aster/core";
import { ChatInfo, ChatMessage, ChatSummary, publicChatMessage, type ChatBatch } from "../model.js";
import { CalendarDate, formatDate } from "../../dates.js";

export const ChatState = Schema.Struct({
  chat: ChatInfo,
  summary: Schema.optional(ChatSummary),
  flushThrough: Schema.optional(CalendarDate),
});
export type ChatState = typeof ChatState.Type;
export const SummaryCommit = Schema.Struct({
  batch: Schema.Array(ChatMessage),
  rolling: ChatSummary,
});
export type SummaryCommit = typeof SummaryCommit.Type;
export interface ChatWork extends ChatState {
  readonly pending: readonly ChatMessage[];
}
type Snapshot = ContextOwnerSnapshot<ChatState, ChatMessage>;
export const serializeChat = ({ chat, summary }: Readonly<ChatState>) =>
  `## Chat: ${chat.name}\n\n\n${summary?.text ?? "No content"}`;

/** Storage retains append order; model batches use event time without modifying storage. */
export const chatWork = ({ state, messages }: Snapshot): ChatWork => ({
  ...state,
  pending: [...messages].sort((a, b) => a.at.localeCompare(b.at) || a.id.localeCompare(b.id)),
});
export const acceptChat = (
  current: Snapshot,
  batch: ChatBatch,
): ContextMutation<ChatState, ChatMessage> => {
  const retained = new Set(current.messages.map((message) => message.id));
  const incoming = new Map(
    batch.messages.map((message) => [message.id, publicChatMessage(message)]),
  );
  return {
    state: { ...current.state, chat: batch.chat },
    messages: { append: [...incoming.values()].filter((message) => !retained.has(message.id)) },
  };
};
export const flushChat = (
  current: Snapshot,
  date: string,
): ContextMutation<ChatState, ChatMessage> => {
  if (!current.messages.some((message) => formatDate(message.at) <= date)) return {};
  return {
    state: {
      ...current.state,
      flushThrough:
        current.state.flushThrough && current.state.flushThrough > date
          ? current.state.flushThrough
          : date,
    },
  };
};
export const applySummary = (
  current: Snapshot,
  result: SummaryCommit,
): ContextMutation<ChatState, ChatMessage> => {
  const covered = new Set(result.batch.map((message) => message.id));
  const pending = current.messages.filter((message) => !covered.has(message.id));
  const through = current.state.flushThrough;
  return {
    state: {
      ...Struct.omit(current.state, ["flushThrough"]),
      summary: result.rolling,
      ...(through && pending.some((message) => formatDate(message.at) <= through)
        ? { flushThrough: through }
        : {}),
    },
    messages: { remove: [...covered] },
  };
};
