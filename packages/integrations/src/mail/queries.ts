import { ContextCommand, ContextQueryError, ContextRegistry, publicJson } from "@aster/core";
import { DateTime, Effect, Match, Schema } from "effect";
import { MailFetcher } from "./client.js";
import { mailMessagePath } from "./contexts.js";
import { MailDate, dayWindow } from "./dates.js";
import { MailMessage, type Mailbox } from "./model.js";
const listArgs = Schema.Struct({
  query: Schema.optional(Schema.String),
  date: Schema.optional(MailDate),
});
const readArgs = Schema.Struct({ id: Schema.NonEmptyString });
export class ListMail extends ContextCommand.Class<ListMail>()("list", {
  success: Schema.Json,
  error: ContextQueryError,
  description:
    "List one day (default today), optionally matching sender or subject. Newest first. Returns metadata without bodies and reports date coverage.",
  payload: listArgs.fields,
}) {}
export class ReadMail extends ContextCommand.Class<ReadMail>()("read", {
  success: Schema.Json,
  error: ContextQueryError,
  description: "Read one email by its mailbox-scoped id. Reuses durable evidence when present.",
  payload: readArgs.fields,
}) {}
export const MailboxQueries = [ListMail, ReadMail] as const;
export const queryMailbox = Effect.fn("Mail.query")(
  function* (mailbox: Mailbox, command: ListMail | ReadMail) {
    const fetcher = yield* MailFetcher;
    const registry = yield* ContextRegistry;

    const timeZone = mailbox.timeZone ?? "Asia/Shanghai";
    const now = yield* DateTime.now;
    const dateBasis = mailbox.protocol === "pop3" ? "sent" : "received";
    const data = yield* Match.value(command).pipe(
      Match.tag("read", (command) =>
        Effect.gen(function* () {
          const { id } = yield* Schema.decodeUnknownEffect(readArgs)(command).pipe(Effect.orDie);
          const record = registry.get(mailMessagePath({ id, mailbox: mailbox.id }));
          const email = record
            ? yield* Schema.decodeUnknownEffect(MailMessage)(record.state).pipe(Effect.orDie)
            : yield* fetcher.read(mailbox, id);
          return publicJson({ ...email, dateBasis });
        }),
      ),
      Match.tag("list", (command) =>
        Effect.gen(function* () {
          const args = yield* Schema.decodeUnknownEffect(listArgs)(command).pipe(Effect.orDie);
          const date =
            args.date ?? DateTime.formatIsoDate(DateTime.setZoneNamedUnsafe(now, timeZone));
          const window = dayWindow(date, timeZone);
          const batch = yield* fetcher.list(mailbox, window);
          const query = args.query?.toLowerCase();
          const messages = batch.messages
            .filter((m) => !query || `${m.from} ${m.subject}`.toLowerCase().includes(query))
            .sort((a, b) => Date.parse(b.date!) - Date.parse(a.date!) || a.id.localeCompare(b.id))
            .map(({ id, from, subject, date }) => ({ id, from, subject, date }));
          return publicJson({
            date,
            timeZone,
            dateBasis,
            previousDate: DateTime.formatIsoDate(
              DateTime.subtract(DateTime.makeZonedUnsafe(window.from, { timeZone }), {
                days: 1,
              }),
            ),
            coverage: { source: "provider", complete: true, undatedObserved: batch.undated },
            items: messages,
            total: messages.length,
          });
        }),
      ),
      Match.exhaustive,
    );
    return data;
  },
  Effect.catchTag("MailFetchError", () =>
    Effect.fail(
      new ContextQueryError({
        kind: "failed",
        message: "Mailbox query failed; no complete result is available",
      }),
    ),
  ),
);
