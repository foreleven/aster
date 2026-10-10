import { Schema } from "effect";
import { ChatInfo, ChatMessage, ChatSummary } from "../service/model.js";

export const ChatSnapshot = Schema.Struct({
  chat: ChatInfo,
  summary: Schema.optional(ChatSummary),
  flushThrough: Schema.optional(Schema.String),
});
export type ChatSnapshot = typeof ChatSnapshot.Type;
export const SummaryCommit = Schema.Struct({
  batch: Schema.Array(ChatMessage),
  rolling: ChatSummary,
});
export type SummaryCommit = typeof SummaryCommit.Type;
export interface ChatWork extends ChatSnapshot {
  readonly pending: readonly ChatMessage[];
}
