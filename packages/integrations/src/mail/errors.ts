import { Schema } from "effect";

export class MailFetchError extends Schema.TaggedError<MailFetchError>()("MailFetchError", {
  mailbox: Schema.String,
  message: Schema.String,
  cause: Schema.Unknown,
}) {}
