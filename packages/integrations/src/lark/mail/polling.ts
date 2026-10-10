import { DateTime, Schema } from "effect";
import { Effect } from "effect";
import { LarkMailClient, LarkResponseError } from "./client.js";

export const MailWindow = Schema.Struct({
  start: Schema.Number,
  through: Schema.Number,
  caughtUp: Schema.Boolean,
});
export type MailWindow = typeof MailWindow.Type;

export const mailDayStart = (now: number): number =>
  DateTime.makeUnsafe(now).pipe(
    DateTime.setZoneNamedUnsafe("Asia/Shanghai"),
    DateTime.startOf("day"),
    DateTime.toEpochMillis,
  );

/** Replay today on restart; an uninterrupted session can finish yesterday's tail. */
export const mailWindow = (
  cursor: number | undefined,
  now: number,
  sessionStart: number,
): MailWindow => {
  const previous =
    cursor !== undefined && Number.isFinite(cursor) && cursor <= now
      ? cursor - 60_000
      : sessionStart;
  const start = Math.max(sessionStart, previous);
  const through = Math.min(now, start + 60 * 60_000);
  return { start, through, caughtUp: through >= now };
};

/** Retrieval is complete only when every requested, absent local ID has a provider record. */
export const pollMail = Effect.fn("LarkMail.poll")(function* (
  cli: LarkMailClient["Service"],
  mailbox: string,
  window: MailWindow,
  retained: (id: string) => boolean,
) {
  if (window.start >= window.through) return [];
  const ids = yield* cli.listIds(mailbox, window.start, window.through);
  const unseen = ids.filter((id) => !retained(id));
  if (!unseen.length) return [];
  const emails = yield* cli.getMessages(mailbox, unseen);
  const received = new Set(emails.map((email) => email.messageId));
  if (
    received.size !== emails.length ||
    unseen.some((id) => !received.has(id)) ||
    emails.some((email) => email.mailbox !== mailbox || !unseen.includes(email.messageId))
  )
    return yield* new LarkResponseError({ cause: "Incomplete or unexpected mail messages" });
  return emails;
});
