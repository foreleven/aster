import { Schema } from "effect";

import { MailMessage } from "./model.js";

/** Encode opaque provider identities as a single public Context path segment. */
export const mailSegment = (value: string) => encodeURIComponent(value).replaceAll(".", "%2E");
export const mailboxPath = (id: string) => `/mail/${mailSegment(id)}`;
export const mailMessagePath = (email: Pick<MailMessage, "mailbox" | "id">) =>
  `${mailboxPath(email.mailbox)}/${mailSegment(email.id)}`;

export const MailRootState = Schema.Struct({ mailboxes: Schema.Array(Schema.String) });
