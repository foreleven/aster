import { ContextRegistry } from "@aster/core";
import { Context, DateTime, Effect, Layer, Schema } from "effect";
import { MailboxSnapshot } from "./snapshot.js";
import { mailDay } from "../dates.js";
import type { Mailbox, MailBatch, MailboxWindow } from "../model.js";
import type { MailFailureDetails } from "../errors.js";

const makeState = Effect.fn("MailboxState.make")(function* (path: string, mailbox: Mailbox) {
  const registry = yield* ContextRegistry;
  const timeZone = mailbox.timeZone ?? "Asia/Shanghai";
  const snapshot = Effect.suspend(() =>
    Schema.decodeUnknownEffect(MailboxSnapshot)(registry.get(path)!.state).pipe(Effect.orDie),
  );
  // All callers are mailbox handlers. Preserve the revision used to calculate each transition.
  const update = Effect.fnUntraced(function* (change: (state: MailboxSnapshot) => MailboxSnapshot) {
    const record = registry.get(path)!;
    const state = yield* Schema.decodeUnknownEffect(MailboxSnapshot)(record.state).pipe(
      Effect.orDie,
    );
    yield* registry
      .commit(
        { ...record, state: change(state) },
        { expectedRevision: record.revision, mode: "bootstrap" },
      )
      .pipe(Effect.orDie);
  });
  const now = yield* DateTime.now;
  const record = registry.get(path);
  if (!record)
    yield* registry
      .commit(
        {
          path,
          description: `Mailbox ${mailbox.id}. List messages by day or read a selected email.`,
          messages: [],
          state: {
            timeZone,
            dateBasis: mailbox.protocol === "pop3" ? "sent" : "received",
            startedAt: DateTime.formatIsoOffset(DateTime.setZoneNamedUnsafe(now, timeZone)),
            today: { date: DateTime.formatIsoDate(mailDay(now, timeZone)), emails: [] },
            status: "syncing",
            undatedObserved: 0,
          },
        },
        { expectedRevision: 0, mode: "bootstrap" },
      )
      .pipe(Effect.orDie);
  else yield* update((state) => ({ ...state, status: "syncing" }));
  return {
    snapshot,
    baseline: (known: readonly string[]) =>
      update((state) => (state.known === undefined ? { ...state, known } : state)),
    nextWindow: Effect.fnUntraced(function* (
      now: DateTime.DateTime,
    ): Effect.fn.Return<MailboxWindow> {
      const state = yield* snapshot;
      const from =
        state.through ??
        DateTime.formatIsoOffset(mailDay(DateTime.makeUnsafe(state.startedAt), state.timeZone));
      const end = DateTime.add(mailDay(DateTime.makeUnsafe(from), state.timeZone), { days: 1 });
      const through = Math.max(
        Date.parse(from),
        Math.min(DateTime.toEpochMillis(end), DateTime.toEpochMillis(now)),
      );
      return {
        from,
        through: DateTime.formatIsoOffset(
          DateTime.makeZonedUnsafe(through, { timeZone: state.timeZone }),
        ),
      };
    }),
    rollDay: (now: DateTime.DateTime) =>
      update((state) => {
        const date = DateTime.formatIsoDate(mailDay(now, state.timeZone));
        return state.today.date === date ? state : { ...state, today: { date, emails: [] } };
      }),
    completeSync: Effect.fn("MailboxState.completeSync")(function* (
      window: MailboxWindow,
      batch: MailBatch,
      caughtUp: boolean,
    ) {
      yield* update((state) => {
        if (state.through && Date.parse(window.from) !== Date.parse(state.through))
          throw new Error("Stale mail retrieval interval");
        if (Date.parse(window.through) < Date.parse(window.from))
          throw new Error("Mail cursor cannot regress");
        const entries = new Map(state.today.emails.map((email) => [email.id, email]));
        for (const email of batch.messages) {
          if (
            !email.date ||
            DateTime.formatIsoDate(
              DateTime.setZoneNamedUnsafe(DateTime.makeUnsafe(email.date), state.timeZone),
            ) !== state.today.date
          )
            continue;
          entries.set(email.id, {
            id: email.id,
            from: email.from,
            subject: email.subject,
            date: email.date,
          });
        }
        const { lastFailure: _failure, ...previous } = state;
        return {
          ...previous,
          known: batch.ids,
          through: window.through,
          undatedObserved: batch.undated,
          today: {
            date: state.today.date,
            emails: [...entries.values()].sort(
              (a, b) => Date.parse(b.date) - Date.parse(a.date) || a.id.localeCompare(b.id),
            ),
          },
          status: caughtUp ? "ready" : "syncing",
        };
      });
    }),
    failSync: (lastFailure: MailFailureDetails) =>
      update((state) => ({ ...state, status: "error", lastFailure })),
  };
});

export class MailboxState extends Context.Service<
  MailboxState,
  Effect.Success<ReturnType<typeof makeState>>
>()("mail/State") {
  static readonly layer = (path: string, mailbox: Mailbox) =>
    Layer.effect(MailboxState, makeState(path, mailbox));
}
