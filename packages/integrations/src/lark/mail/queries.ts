import { ContextCommand, ContextQueryError } from "@aster/core";
import { Schema } from "effect";
import { MailListArgs, MailListPage, MailboxProfile, EmailData } from "./model.js";

const ProviderError = Schema.TaggedUnion({
  LarkCliError: { message: Schema.String },
  LarkResponseError: { message: Schema.String, kind: Schema.optional(Schema.String) },
});
const MailQueryError = Schema.Union([ContextQueryError, ProviderError]);

export class MailProfile extends ContextCommand.Class<MailProfile>()("profile", {
  description: "Read the Mailbox Actor's retained public identity.",
  payload: {},
  success: MailboxProfile,
  error: ContextQueryError,
}) {
  static override text = (profile: typeof MailboxProfile.Type): string =>
    `${profile.name} <${profile.address}>`;
}
export class ListMail extends ContextCommand.Class<ListMail>()("list", {
  description:
    "List provider mail metadata. query searches from/to/subject/body (max 50 characters); from/to/cc/bcc accept comma-separated addresses. Supports folder/folderId, label/labelId, subject, isUnread, hasAttachment, start/end ISO timestamps with timezone, limit (1–400), and pageToken. Defaults to inbox; returns no bodies. Continue with nextPageToken using the same filters.",
  payload: MailListArgs.fields,
  success: Schema.Struct({
    ...MailListPage.fields,
    coverage: Schema.Struct({ source: Schema.Literal("provider"), complete: Schema.Boolean }),
  }),
  error: ProviderError,
}) {}
export class ReadMail extends ContextCommand.Class<ReadMail>()("read", {
  description:
    "Read one email by messageId, using its Message Actor when retained and the provider otherwise.",
  payload: { messageId: Schema.NonEmptyString.check(Schema.isPattern(/^[^/]+$/)) },
  success: EmailData,
  error: MailQueryError,
}) {}
export const LarkMailCommands = [MailProfile, ListMail, ReadMail] as const;
