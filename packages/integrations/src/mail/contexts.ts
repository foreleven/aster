import { contextView } from "@aster/core";
import { Schema } from "effect";
import { MailFailureDetails } from "./errors.js";
import { MailMessage } from "./model.js";

/** Encode opaque provider identities as a single public Context path segment. */
export const mailSegment = (value: string) => encodeURIComponent(value).replaceAll(".", "%2E");
export const mailboxPath = (id: string) => `/mail/${mailSegment(id)}`;
export const mailMessagePath = (email: MailMessage) =>
  `${mailboxPath(email.mailbox)}/${mailSegment(email.id)}`;

export const MailRootState = Schema.Struct({ mailboxes: Schema.Array(Schema.String) });
export const MailboxState = Schema.Struct({
  mailbox: Schema.String,
  protocol: Schema.Literals(["imap", "pop3"]),
  folder: Schema.String,
  status: Schema.Literals(["starting", "ready", "error"]),
  lastSyncedAt: Schema.optional(Schema.String),
  lastError: Schema.optional(Schema.String),
  lastFailure: Schema.optional(MailFailureDetails),
});
export type MailboxState = typeof MailboxState.Type;

export const mailRootView = contextView({
  matches: (path) => path === "/mail",
  state: MailRootState,
});
export const mailboxView = contextView({
  matches: (path) => /^\/mail\/[^/]+$/.test(path),
  state: MailboxState,
});
export const mailMessageView = contextView({
  matches: (path) => /^\/mail\/[^/]+\/[^/]+$/.test(path),
  state: MailMessage,
});
export const mailContextViews = [mailRootView, mailboxView, mailMessageView];
