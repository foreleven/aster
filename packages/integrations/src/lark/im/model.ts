import { Schema, Struct } from "effect";
import { LarkId } from "../paths.js";
import { Timestamp } from "../dates.js";
export const ChatInfo = Schema.Struct({
  id: LarkId,
  name: Schema.String,
  mode: Schema.String,
  description: Schema.String,
});
export type ChatInfo = typeof ChatInfo.Type;
/** Only canonical sender identity is accepted; transport fields are not retained. */
export const ChatSender = Schema.Struct({
  id: Schema.optional(Schema.String),
  id_type: Schema.optional(Schema.String),
  sender_type: Schema.optional(Schema.String),
  name: Schema.optional(Schema.String),
  open_id: Schema.optional(Schema.String),
});
export const ChatMessage = Schema.Struct({
  id: LarkId,
  at: Timestamp,
  content: Schema.String,
  sender: ChatSender,
  url: Schema.String,
  deleted: Schema.Boolean,
});
export type ChatMessage = typeof ChatMessage.Type;
/** Also sanitize trusted fixture/client values before encoding prompts or acceptance. */
export const publicChatMessage = (message: ChatMessage): ChatMessage => ({
  ...Struct.pick(message, ["id", "at", "content", "url", "deleted"]),
  sender: Struct.pick(message.sender, ["id", "id_type", "sender_type", "name", "open_id"]),
});

export const ChatSummary = Schema.Struct({
  text: Schema.String,
  references: Schema.Array(Schema.Struct({ id: LarkId, url: Schema.String })),
});
export type ChatSummary = typeof ChatSummary.Type;

export interface ChatBatch {
  readonly chat: ChatInfo;
  readonly messages: readonly ChatMessage[];
}

/** Query pagination follows the CLI cursor rather than a local retained-message offset. */
export const ChatHistoryArgs = Schema.Struct({
  chatId: Schema.optional(Schema.NonEmptyString),
  userId: Schema.optional(Schema.NonEmptyString),
  start: Schema.optional(Schema.NonEmptyString),
  end: Schema.optional(Schema.NonEmptyString),
  order: Schema.optional(Schema.Literals(["asc", "desc"])),
  pageSize: Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 50 }))),
  pageToken: Schema.optional(Schema.NonEmptyString),
});
export type ChatHistoryArgs = typeof ChatHistoryArgs.Type;

export const ImState = Schema.Struct({ through: Schema.optional(Timestamp) });
export type ImState = typeof ImState.Type;
export const ImPollResult = Schema.Struct({
  start: Schema.String,
  through: Schema.String,
  caughtUp: Schema.Boolean,
  batches: Schema.Array(Schema.Struct({ chat: ChatInfo, messages: Schema.Array(ChatMessage) })),
});
export type ImPollResult = typeof ImPollResult.Type;
