import { ContextCommand, ContextQueryError, DurableContext } from "@aster/core";
import { Effect, Schema } from "effect";
import { MailListArgs, MailListPage, MailboxProfile, EmailData, MailboxState } from "./model.js";
import { LarkMailClient } from "./client.js";
import { queryError } from "../errors.js";
import { emailPath, LarkId } from "../paths.js";

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
  error: ContextQueryError,
}) {}
export class ReadMail extends ContextCommand.Class<ReadMail>()("read", {
  description:
    "Read one email by messageId, from committed local state, falling back to the provider when absent.",
  payload: { messageId: LarkId },
  success: EmailData,
  error: ContextQueryError,
}) {}
export const LarkMailCommands = [MailProfile, ListMail, ReadMail] as const;

export const readMailboxProfile = Effect.fn("LarkMail.readProfile")(function* () {
  const state = yield* (yield* DurableContext)
    .readState("/lark/mail", MailboxState)
    .pipe(Effect.catchTag("ContextValidationError", (error) => Effect.fail(queryError(error))));
  if (!state.profile)
    return yield* new ContextQueryError({
      kind: "unavailable",
      message: "Mailbox profile is not available",
    });
  return state.profile;
});
export const readEmail = Effect.fn("LarkMail.readEmail")(function* (
  mailbox: string,
  messageId: string,
) {
  const registry = yield* DurableContext;
  const path = emailPath(mailbox, messageId);
  if (registry.get(path)) {
    const email = yield* registry
      .readState(path, EmailData)
      .pipe(Effect.catchTag("ContextValidationError", (error) => Effect.fail(queryError(error))));
    if (email.mailbox !== mailbox || email.messageId !== messageId)
      return yield* new ContextQueryError({
        kind: "failed",
        message: "Retained email identity mismatch",
      });
    return email;
  }
  const emails = yield* (yield* LarkMailClient)
    .getMessages(mailbox, [messageId])
    .pipe(Effect.mapError(queryError));
  const email = emails.find((email) => email.mailbox === mailbox && email.messageId === messageId);
  if (!email)
    return yield* new ContextQueryError({
      kind: "unavailable",
      message: "Email evidence unavailable",
    });
  return email;
});
