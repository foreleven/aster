import {
  ContextQueries,
  ContextQueryError,
  ContextRegistry,
  ContextListArgs,
  contextPage,
  publicJson,
} from "@aster/core";
import { DateTime, Effect, Schema } from "effect";
import { MailFetcher } from "./client.js";
import { MailDate, dayWindow } from "./dates.js";
import { mailMessagePath } from "./contexts.js";
import { MailMessage, type Mailbox } from "./model.js";
const listArgs = Schema.Struct({ ...ContextListArgs.fields, date: Schema.optional(MailDate) });
const readArgs = Schema.Struct({ id: Schema.NonEmptyString });
export const mailboxQueryDefinition = {
  description: "Read mailbox messages by calendar day. Historical reads do not trigger work.",
  commands: {
    list: {
      description:
        "List one day (default today), optionally matching sender or subject. Newest first; pagination is a live provider view. Returns metadata without bodies and reports date coverage.",
      schema: listArgs,
    },
    read: {
      description: "Read one email by its mailbox-scoped id. Reuses durable evidence when present.",
      schema: readArgs,
    },
  },
};
export const makeMailboxQuery = Effect.fnUntraced(function* (mailbox: Mailbox) {
  const fetcher = yield* MailFetcher;
  const registry = yield* ContextRegistry;
  return Effect.fn("Mail.query")(
    function* (input: Parameters<ContextQueries["Service"]["query"]>[0]) {
      const timeZone = mailbox.timeZone ?? "Asia/Shanghai";
      const now = yield* DateTime.now;
      const dateBasis = mailbox.protocol === "pop3" ? "sent" : "received";
      const data =
        input.command === "read"
          ? yield* Effect.gen(function* () {
              const { id } = yield* Schema.decodeUnknownEffect(readArgs)(input.args).pipe(
                Effect.orDie,
              );
              const record = registry.get(mailMessagePath({ id, mailbox: mailbox.id }));
              const email = record
                ? yield* Schema.decodeUnknownEffect(MailMessage)(record.state).pipe(Effect.orDie)
                : yield* fetcher.read(mailbox, id);
              return publicJson({ ...email, dateBasis });
            })
          : yield* Effect.gen(function* () {
              const args = yield* Schema.decodeUnknownEffect(listArgs)(input.args).pipe(
                Effect.orDie,
              );
              const date =
                args.date ?? DateTime.formatIsoDate(DateTime.setZoneNamedUnsafe(now, timeZone));
              const window = dayWindow(date, timeZone);
              const batch = yield* fetcher.list(mailbox, window);
              const query = args.query?.toLowerCase();
              const messages = batch.messages
                .filter((m) => !query || `${m.from} ${m.subject}`.toLowerCase().includes(query))
                .sort(
                  (a, b) => Date.parse(b.date!) - Date.parse(a.date!) || a.id.localeCompare(b.id),
                )
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
                ...contextPage(messages, args),
              });
            });
      return { path: input.path, command: input.command, queriedAt: DateTime.formatIso(now), data };
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
});
