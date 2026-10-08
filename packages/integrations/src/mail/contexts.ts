import { contextView } from "@aster/core";
import { Schema } from "effect";
import { MailboxSnapshot } from "./state/snapshot.js";
import { MailMessage } from "./model.js";

/** Encode opaque provider identities as a single public Context path segment. */
export const mailSegment = (value: string) => encodeURIComponent(value).replaceAll(".", "%2E");
export const mailboxPath = (id: string) => `/mail/${mailSegment(id)}`;
export const mailMessagePath = (email: Pick<MailMessage, "mailbox" | "id">) =>
  `${mailboxPath(email.mailbox)}/${mailSegment(email.id)}`;

export const MailRootState = Schema.Struct({ mailboxes: Schema.Array(Schema.String) });
export const mailRootView = contextView({
  matches: (path) => path === "/mail",
  state: MailRootState,
});
export const mailboxView = contextView({
  matches: (path) => /^\/mail\/[^/]+$/.test(path),
  state: Schema.Struct({
    timeZone: MailboxSnapshot.fields.timeZone,
    dateBasis: MailboxSnapshot.fields.dateBasis,
    through: MailboxSnapshot.fields.through,
    today: MailboxSnapshot.fields.today,
    status: MailboxSnapshot.fields.status,
    undatedObserved: MailboxSnapshot.fields.undatedObserved,
    lastFailure: MailboxSnapshot.fields.lastFailure,
  }),
});
export const mailMessageView = contextView({
  matches: (path) => /^\/mail\/[^/]+\/[^/]+$/.test(path),
  state: MailMessage,
});
export const mailContextViews = [mailRootView, mailboxView, mailMessageView];
