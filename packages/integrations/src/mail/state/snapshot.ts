import { Schema } from "effect";
import { MailTimestamp, MailTimeZone } from "../model.js";
import { MailDate } from "../dates.js";
import { MailFailureDetails } from "../errors.js";
export const MailSummary = Schema.Struct({
  id: Schema.String,
  from: Schema.String,
  subject: Schema.String,
  date: MailTimestamp,
});
export const MailboxSnapshot = Schema.Struct({
  timeZone: MailTimeZone,
  dateBasis: Schema.Literals(["received", "sent"]),
  startedAt: MailTimestamp,
  through: Schema.optional(MailTimestamp),
  known: Schema.optional(Schema.Array(Schema.NonEmptyString)),
  today: Schema.Struct({ date: MailDate, emails: Schema.Array(MailSummary) }),
  status: Schema.Literals(["syncing", "ready", "error"]),
  undatedObserved: Schema.Int,
  lastFailure: Schema.optional(MailFailureDetails),
});
export type MailboxSnapshot = typeof MailboxSnapshot.Type;
