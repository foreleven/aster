import { createHash } from "node:crypto";
import { Schema } from "effect";
import { ChatInfo, ChatMessage, ChatSummary, publicChatMessage } from "../service/model.js";

export const ChatSnapshot = Schema.Struct({
  chat: ChatInfo,
  summary: Schema.optional(ChatSummary),
  seen: Schema.Record(
    Schema.String,
    Schema.Struct({ fingerprint: Schema.String, at: Schema.String }),
  ),
  flushThrough: Schema.optional(Schema.String),
  replayFrom: Schema.optional(Schema.String),
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
export const batchFingerprint = (messages: readonly ChatMessage[]) =>
  createHash("sha256")
    .update(JSON.stringify(messages.map(publicChatMessage)))
    .digest("hex");
export const messageFingerprint = (message: ChatMessage) =>
  createHash("sha256")
    .update(JSON.stringify(publicChatMessage(message)))
    .digest("hex");
