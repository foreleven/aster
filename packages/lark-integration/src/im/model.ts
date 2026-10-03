import { Schema } from "effect";
export const ImChat = Schema.Struct({
  id: Schema.String,
  name: Schema.String,
  mode: Schema.String,
  description: Schema.String,
});
export type ImChat = typeof ImChat.Type;
export const ImMessage = Schema.Struct({
  id: Schema.String,
  at: Schema.String,
  content: Schema.String,
  sender: Schema.ObjectKeyword,
  url: Schema.String,
  deleted: Schema.Boolean,
});
export type ImMessage = typeof ImMessage.Type;

/** Sender identity is readable; opaque transport/provider fields are not. */
export const PublicImMessage = Schema.Struct({
  ...ImMessage.fields,
  sender: Schema.Struct({
    id: Schema.optional(Schema.String),
    id_type: Schema.optional(Schema.String),
    sender_type: Schema.optional(Schema.String),
    name: Schema.optional(Schema.String),
    open_id: Schema.optional(Schema.String),
  }),
});
export const publicImMessage = (message: ImMessage) =>
  Schema.decodeUnknownSync(PublicImMessage)(message);
