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
