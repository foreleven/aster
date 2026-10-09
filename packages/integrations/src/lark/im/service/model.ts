import { Schema } from "effect";
export const ChatInfo = Schema.Struct({
  id: Schema.String,
  name: Schema.String,
  mode: Schema.String,
  description: Schema.String,
});
export type ChatInfo = typeof ChatInfo.Type;
export const ChatMessage = Schema.Struct({
  id: Schema.String,
  at: Schema.String,
  content: Schema.String,
  sender: Schema.ObjectKeyword,
  url: Schema.String,
  deleted: Schema.Boolean,
});
export type ChatMessage = typeof ChatMessage.Type;

/** Sender identity is readable; opaque transport/provider fields are not. */
export const ChatPublicMessage = Schema.Struct({
  ...ChatMessage.fields,
  sender: Schema.Struct({
    id: Schema.optional(Schema.String),
    id_type: Schema.optional(Schema.String),
    sender_type: Schema.optional(Schema.String),
    name: Schema.optional(Schema.String),
    open_id: Schema.optional(Schema.String),
  }),
});
export const publicChatMessage = (message: ChatMessage) =>
  Schema.decodeUnknownSync(ChatPublicMessage)(message);

export const ChatSummary = Schema.Struct({
  text: Schema.String,
  references: Schema.Array(Schema.Struct({ id: Schema.String, url: Schema.String })),
});
export type ChatSummary = typeof ChatSummary.Type;

export interface ChatBatch {
  readonly chat: ChatInfo;
  readonly messages: readonly ChatMessage[];
}
