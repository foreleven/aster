import { Schema } from "effect";
export const MailboxProfile = Schema.Struct({
  address: Schema.String,
  name: Schema.String,
});
export type MailboxProfile = typeof MailboxProfile.Type;
export const EmailData = Schema.Struct({
  messageId: Schema.String,
  mailbox: Schema.String,
  from: Schema.String,
  subject: Schema.String,
  bodyPlainText: Schema.String,
  attachments: Schema.Array(
    Schema.Struct({
      id: Schema.String,
      filename: Schema.String,
      contentType: Schema.String,
      isInline: Schema.Boolean,
    }),
  ),
});
export type EmailData = typeof EmailData.Type;

export const MailListArgs = Schema.Struct({
  query: Schema.optional(Schema.NonEmptyString.check(Schema.isMaxLength(50))),
  folder: Schema.optional(Schema.NonEmptyString),
  folderId: Schema.optional(Schema.NonEmptyString),
  label: Schema.optional(Schema.NonEmptyString),
  labelId: Schema.optional(Schema.NonEmptyString),
  from: Schema.optional(Schema.NonEmptyString),
  to: Schema.optional(Schema.NonEmptyString),
  cc: Schema.optional(Schema.NonEmptyString),
  bcc: Schema.optional(Schema.NonEmptyString),
  subject: Schema.optional(Schema.NonEmptyString),
  isUnread: Schema.optional(Schema.Boolean),
  hasAttachment: Schema.optional(Schema.Boolean),
  start: Schema.optional(Schema.NonEmptyString),
  end: Schema.optional(Schema.NonEmptyString),
  limit: Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 400 }))),
  pageToken: Schema.optional(Schema.NonEmptyString),
});
export type MailListArgs = typeof MailListArgs.Type;
export const MailListPage = Schema.Struct({
  items: Schema.Array(
    Schema.Struct({
      messageId: Schema.NonEmptyString,
      date: Schema.optional(Schema.String),
      from: Schema.String,
      subject: Schema.String,
      labels: Schema.optional(Schema.String),
    }),
  ),
  hasMore: Schema.Boolean,
  nextPageToken: Schema.NullOr(Schema.String),
});
export type MailListPage = typeof MailListPage.Type;
