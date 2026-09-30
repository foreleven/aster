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
