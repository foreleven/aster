import { DateTime, Option, Schema } from "effect";

export const MailTimestamp = Schema.String.check(
  Schema.isPattern(/T.*(?:Z|[+-]\d{2}:\d{2})$/),
  Schema.makeFilter((value) => Option.isSome(DateTime.make(value)), {
    expected: "An ISO timestamp with timezone",
  }),
);
export const MailTimeZone = Schema.String.check(
  Schema.makeFilter((value) => Option.isSome(DateTime.zoneMakeNamed(value)), {
    expected: "An IANA time zone",
  }),
);

/** A normalized message returned by the mailbox transport. */
export const MailMessage = Schema.Struct({
  id: Schema.NonEmptyString,
  mailbox: Schema.NonEmptyString,
  from: Schema.String,
  to: Schema.Array(Schema.String),
  subject: Schema.String,
  text: Schema.String,
  date: Schema.optional(MailTimestamp),
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
  timeZone: Schema.optional(MailTimeZone),
});
export type Mailbox = typeof Mailbox.Type;

export const MailConfig = Schema.Struct({
  pollIntervalMs: Schema.optional(Schema.Int.check(Schema.isGreaterThan(0))),
  mailboxes: Schema.Array(Mailbox),
});
export type MailConfig = typeof MailConfig.Type;

export const MailboxWindow = Schema.Struct({ from: MailTimestamp, through: MailTimestamp });
export type MailboxWindow = typeof MailboxWindow.Type;
export const MailBatch = Schema.Struct({
  ids: Schema.Array(Schema.NonEmptyString),
  messages: Schema.Array(MailMessage),
  undated: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
});
export type MailBatch = typeof MailBatch.Type;
