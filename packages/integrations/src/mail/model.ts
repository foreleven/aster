import { Schema } from "effect";

/** A normalized message returned by the mailbox transport. */
export const MailMessage = Schema.Struct({
  id: Schema.String,
  mailbox: Schema.String,
  from: Schema.String,
  to: Schema.Array(Schema.String),
  subject: Schema.String,
  text: Schema.String,
  date: Schema.optional(Schema.String),
});
export type MailMessage = typeof MailMessage.Type;

export const Mailbox = Schema.Struct({
  id: Schema.NonEmptyString,
  protocol: Schema.optional(Schema.Literals(["imap", "pop3"])),
  host: Schema.NonEmptyString,
  port: Schema.optional(Schema.Int),
  secure: Schema.optional(Schema.Boolean),
  username: Schema.NonEmptyString,
  password: Schema.Redacted(Schema.String),
  folder: Schema.optional(Schema.NonEmptyString),
  maxMessages: Schema.optional(Schema.Int),
});
export type Mailbox = typeof Mailbox.Type;

export const MailConfig = Schema.Struct({
  mailboxes: Schema.Array(Mailbox),
});
export type MailConfig = typeof MailConfig.Type;
