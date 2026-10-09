import { Schema } from "effect";
import { ChatInfo, ChatMessage } from "../service/model.js";
export const ImSnapshot = Schema.Struct({
  ready: Schema.Boolean,
  through: Schema.optional(Schema.String),
  chats: Schema.Number,
  lastError: Schema.optional(Schema.String),
});
export type ImSnapshot = typeof ImSnapshot.Type;
export const ImPollResult = Schema.Struct({
  start: Schema.String,
  through: Schema.String,
  caughtUp: Schema.Boolean,
  batches: Schema.Array(Schema.Struct({ chat: ChatInfo, messages: Schema.Array(ChatMessage) })),
});
export type ImPollResult = typeof ImPollResult.Type;
