import { Context, Effect, Layer, Redacted, Schema } from "effect";
import { ImapFlow } from "imapflow";
import { simpleParser } from "mailparser";
import Pop3Command from "node-pop3";
import type { Mailbox, MailMessage, MailboxWindow, MailBatch } from "./model.js";
import { MailFetchError, mailFailureDetails, type MailFailureStage } from "./errors.js";

import { inWindow } from "./dates.js";

export class MailFetcher extends Context.Service<
  MailFetcher,
  {
    readonly inventory: (mailbox: Mailbox) => Effect.Effect<readonly string[], MailFetchError>;
    readonly pull: (
      mailbox: Mailbox,
      window: MailboxWindow,
      known: readonly string[],
    ) => Effect.Effect<MailBatch, MailFetchError>;
    readonly list: (
      mailbox: Mailbox,
      window: MailboxWindow,
    ) => Effect.Effect<MailBatch, MailFetchError>;
    readonly read: (mailbox: Mailbox, id: string) => Effect.Effect<MailMessage, MailFetchError>;
  }
>()("mail/Fetcher") {}

type Request =
  | { kind: "inventory" }
  | { kind: "read"; id: string }
  | { kind: "list"; window: MailboxWindow }
  | { kind: "pull"; window: MailboxWindow; known: ReadonlySet<string> };
const selected = (request: Extract<Request, { kind: "list" | "pull" }>, message: MailMessage) =>
  inWindow(message.date, request.window) ||
  (request.kind === "pull" && !request.known.has(message.id));
const batch = (): { ids: string[]; messages: MailMessage[]; undated: number } => ({
  ids: [],
  messages: [],
  undated: 0,
});
const validDate = (value: Date | string | undefined) => {
  const time = value === undefined ? NaN : new Date(value).getTime();
  return Number.isFinite(time) ? new Date(time).toISOString() : undefined;
};

const address = (value: { name?: string; address?: string } | undefined): string =>
  value === undefined ? "" : [value.name, value.address].filter(Boolean).join(" ");

const addresses = (value: unknown): ReadonlyArray<string> => {
  if (value === undefined || typeof value !== "object" || value === null) return [];
  if ("value" in value && Array.isArray(value.value)) return value.value.map(address);
  return [address(value as { name?: string; address?: string })];
};

const normalize = async (
  mailbox: Mailbox,
  id: string,
  source: Buffer | string,
): Promise<MailMessage> => {
  const parsed = await simpleParser(source);
  return {
    id,
    mailbox: mailbox.id,
    from: address(parsed.from?.value[0]),
    to: addresses(parsed.to),
    subject: parsed.subject ?? "",
    text: parsed.text ?? "",
    ...(validDate(parsed.date) === undefined ? {} : { date: validDate(parsed.date) }),
  };
};

const pullMailbox = (mailbox: Mailbox, request: Request) =>
  Effect.suspend(() => {
    let stage: MailFailureStage = "connect";
    return Effect.tryPromise({
      try: async (signal) => {
        const client = new ImapFlow({
          host: mailbox.host,
          port: mailbox.port ?? (mailbox.secure === false ? 143 : 993),
          secure: mailbox.secure ?? true,
          auth: { user: mailbox.username, pass: Redacted.value(mailbox.password) },
          logger: false,
        });
        const onAbort = () => client.close();
        signal.addEventListener("abort", onAbort, { once: true });
        try {
          if (signal.aborted) {
            onAbort();
            throw signal.reason;
          }
          await client.connect();
          stage = "open-mailbox";
          const lock = await client.getMailboxLock(mailbox.folder ?? "INBOX");
          try {
            stage = "fetch";
            const result = batch();
            const box = client.mailbox;
            if (!box || box.exists === 0) return result;
            const identity = (uid: number) =>
              `imap:${mailbox.folder ?? "INBOX"}:${box.uidValidity}:${uid}`;
            const ids = await client.search({ all: true }, { uid: true });
            if (!Array.isArray(ids)) throw new Error("Incomplete IMAP inventory");
            result.ids = ids.map(identity);
            if (request.kind === "inventory") return result;
            if (request.kind === "read") {
              const uid = ids.find((id) => identity(id) === request.id);
              if (uid === undefined) throw new Error("Email is no longer available");
              const item = await client.fetchOne(
                String(uid),
                { source: true, internalDate: true },
                { uid: true },
              );
              if (!item || !item.source) throw new Error("Incomplete IMAP message");
              result.messages.push({
                ...(await normalize(mailbox, request.id, item.source)),
                date: validDate(item.internalDate),
              });
              return result;
            }
            // SEARCH dates ignore timezone/time-of-day. Widen the candidate window,
            // then apply precise INTERNALDATE bounds before downloading bodies.
            const since = new Date(Date.parse(request.window.from) - 86_400_000);
            const before = new Date(Date.parse(request.window.through) + 86_400_000);
            const dated = await client.search({ since, before }, { uid: true });
            if (!Array.isArray(dated)) throw new Error("Incomplete IMAP date search");
            const candidates = new Set(dated);
            if (request.kind === "pull") {
              const prefix = `imap:${mailbox.folder ?? "INBOX"}:${box.uidValidity}:`;
              if ([...request.known].some((id) => !id.startsWith(prefix)))
                throw Object.assign(
                  new Error("IMAP UIDVALIDITY changed; discovery baseline requires reconciliation"),
                  { code: "UIDVALIDITY_CHANGED" },
                );
              for (const uid of ids) if (!request.known.has(identity(uid))) candidates.add(uid);
            }
            const uids = [...candidates];
            for (let offset = 0; offset < uids.length; offset += 200) {
              const headers: MailMessage[] = [];
              const pending = new Set(uids.slice(offset, offset + 200));
              for await (const item of client.fetch(
                uids.slice(offset, offset + 200),
                { uid: true, headers: true, internalDate: true },
                { uid: true },
              )) {
                if (!item.headers || !pending.delete(item.uid))
                  throw new Error("Incomplete IMAP headers");
                stage = "parse";
                const message = {
                  ...(await normalize(mailbox, identity(item.uid), item.headers)),
                  date: validDate(item.internalDate),
                };
                if (!message.date) result.undated++;
                if (selected(request, message)) headers.push(message);
                stage = "fetch";
              }
              if (pending.size > 0) throw new Error("Incomplete IMAP header scan");
              // Never issue another IMAP command inside its fetch iterator.
              for (const header of headers) {
                if (request.kind === "list") {
                  result.messages.push(header);
                  continue;
                }
                const item = await client.fetchOne(
                  header.id.slice(header.id.lastIndexOf(":") + 1),
                  { source: true },
                  { uid: true },
                );
                if (!item || !item.source) throw new Error("Email disappeared during retrieval");
                result.messages.push({
                  ...(await normalize(mailbox, header.id, item.source)),
                  date: header.date,
                });
              }
            }
            return result;
          } finally {
            lock.release();
          }
        } finally {
          signal.removeEventListener("abort", onAbort);
          client.close();
        }
      },
      catch: (cause) =>
        new MailFetchError({
          mailbox: mailbox.id,
          message: `Unable to pull mailbox ${mailbox.id}`,
          cause,
          details: mailFailureDetails(cause, stage),
        }),
    });
  });

const pullPop3Mailbox = (mailbox: Mailbox, request: Request) =>
  Effect.suspend(() => {
    let stage: MailFailureStage = "connect";
    return Effect.tryPromise({
      try: async (signal) => {
        const client = new Pop3Command({
          user: mailbox.username,
          password: Redacted.value(mailbox.password),
          host: mailbox.host,
          port: mailbox.port ?? (mailbox.secure === false ? 110 : 995),
          tls: mailbox.secure ?? true,
        });
        let rejectAborted: (reason?: unknown) => void = () => undefined;
        const onAbort = () => {
          // node-pop3 has no public abort API; close its socket at this SDK boundary.
          client._socket?.destroy();
          rejectAborted(new Error("POP3 pull interrupted"));
        };
        const aborted = new Promise<never>((_, reject) => {
          rejectAborted = reject;
          signal.addEventListener("abort", onAbort, { once: true });
        });
        try {
          const pull = async () => {
            await client.connect();
            if (signal.aborted) {
              client._socket?.destroy();
              throw signal.reason;
            }
            stage = "authenticate";
            const ids = Schema.decodeUnknownSync(
              Schema.Array(Schema.Tuple([Schema.String, Schema.String])),
            )(await client.UIDL());
            const result = batch();
            result.ids = ids.map(([, id]) => `pop3:${id}`);
            if (new Set(result.ids).size !== ids.length)
              throw new Error("Duplicate POP3 UIDL identity");
            if (request.kind === "inventory") return result;
            const textOf = async (source: Awaited<ReturnType<typeof client.RETR>>) =>
              typeof source === "string" ? source : await Pop3Command.stream2String(source);
            for (const [number, uid] of ids) {
              if (signal.aborted) throw signal.reason;
              const id = `pop3:${uid}`;
              stage = "fetch";
              if (request.kind === "read") {
                if (request.id === id)
                  result.messages.push(
                    await normalize(mailbox, id, await textOf(await client.RETR(Number(number)))),
                  );
                continue;
              }
              // POP3 cannot search by receipt date. TOP avoids downloading historical bodies.
              const headers = await textOf(await client.TOP(Number(number), 0));
              stage = "parse";
              const message = await normalize(mailbox, id, headers);
              if (!message.date) result.undated++;
              if (!selected(request, message)) continue;
              stage = "fetch";
              result.messages.push(
                request.kind === "list"
                  ? message
                  : await normalize(mailbox, id, await textOf(await client.RETR(Number(number)))),
              );
            }
            return result;
          };
          if (signal.aborted) {
            onAbort();
            return await aborted;
          }
          return await Promise.race([pull(), aborted]);
        } finally {
          signal.removeEventListener("abort", onAbort);
          if (signal.aborted) client._socket?.destroy();
          else await client.QUIT().catch(() => undefined);
        }
      },
      catch: (cause) =>
        new MailFetchError({
          mailbox: mailbox.id,
          message: `Unable to pull mailbox ${mailbox.id}`,
          cause,
          details: mailFailureDetails(cause, stage),
        }),
    });
  });

const fetch = (mailbox: Mailbox, request: Request) =>
  (mailbox.protocol ?? "imap") === "pop3"
    ? pullPop3Mailbox(mailbox, request)
    : pullMailbox(mailbox, request);

export const mailFetcherLayer = () =>
  Layer.succeed(MailFetcher, {
    inventory: (mailbox) =>
      fetch(mailbox, { kind: "inventory" }).pipe(Effect.map((result) => result.ids)),
    pull: (mailbox, window, known) =>
      fetch(mailbox, { kind: "pull", window, known: new Set(known) }),
    list: (mailbox, window) => fetch(mailbox, { kind: "list", window }),
    read: (mailbox, id) =>
      fetch(mailbox, { kind: "read", id }).pipe(
        Effect.flatMap((result) =>
          result.messages[0]
            ? Effect.succeed(result.messages[0])
            : Effect.fail(
                new MailFetchError({
                  mailbox: mailbox.id,
                  message: "Email is no longer available",
                  cause: undefined,
                }),
              ),
        ),
      ),
  });
