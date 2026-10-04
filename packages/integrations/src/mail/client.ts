import { Context, Effect, Layer, Redacted, Schema } from "effect";
import { ImapFlow } from "imapflow";
import { simpleParser } from "mailparser";
import Pop3Command from "node-pop3";
import type { Mailbox, MailMessage } from "./model.js";
import { MailFetchError, mailFailureDetails, type MailFailureStage } from "./errors.js";

export class MailFetcher extends Context.Service<
  MailFetcher,
  {
    readonly pull: (mailbox: Mailbox) => Effect.Effect<ReadonlyArray<MailMessage>, MailFetchError>;
    readonly pullAll: () => Effect.Effect<ReadonlyArray<MailMessage>, MailFetchError>;
  }
>()("mail/Fetcher") {}

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
    ...(parsed.date === undefined ? {} : { date: parsed.date.toISOString() }),
  };
};

const pullMailbox = (mailbox: Mailbox) =>
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
            const messages: MailMessage[] = [];
            const limit = mailbox.maxMessages ?? 50;
            const selected = client.mailbox;
            if (!selected || selected.exists === 0) return [];
            const first = Math.max(1, selected.exists - limit + 1);
            for await (const item of client.fetch(`${first}:${selected.exists}`, {
              uid: true,
              source: true,
            })) {
              if (item.source !== undefined) {
                stage = "parse";
                messages.push(
                  await normalize(
                    mailbox,
                    `imap:${mailbox.folder ?? "INBOX"}:${selected.uidValidity}:${item.uid}`,
                    item.source,
                  ),
                );
                stage = "fetch";
              }
              if (messages.length >= limit) break;
            }
            return messages;
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

const pullPop3Mailbox = (mailbox: Mailbox) =>
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
            const [countText] = (await client.STAT()).trim().split(/\s+/);
            stage = "fetch";
            const total = Number.parseInt(countText ?? "0", 10);
            const first = Math.max(1, total - (mailbox.maxMessages ?? 50) + 1);
            const ids = Schema.decodeUnknownSync(
              Schema.Array(Schema.Tuple([Schema.String, Schema.String])),
            )(await client.UIDL());
            const byNumber = new Map(ids);
            const messages: MailMessage[] = [];
            for (let number = first; number <= total; number++) {
              const source = await client.RETR(number);
              const text =
                typeof source === "string" ? source : await Pop3Command.stream2String(source);
              const uid = byNumber.get(String(number));
              if (!uid) throw new Error("Missing POP3 UIDL identity");
              stage = "parse";
              messages.push(await normalize(mailbox, `pop3:${uid}`, text));
              stage = "fetch";
            }
            return messages;
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

export const mailFetcherLayer = (mailboxes: ReadonlyArray<Mailbox>) =>
  Layer.effect(
    MailFetcher,
    Effect.succeed(
      MailFetcher.of({
        pull: (mailbox) =>
          (mailbox.protocol ?? "imap") === "pop3" ? pullPop3Mailbox(mailbox) : pullMailbox(mailbox),
        pullAll: () =>
          Effect.forEach(
            mailboxes,
            (mailbox) =>
              (mailbox.protocol ?? "imap") === "pop3"
                ? pullPop3Mailbox(mailbox)
                : pullMailbox(mailbox),
            { concurrency: 1 },
          ).pipe(Effect.map((groups) => groups.flat())),
      }),
    ),
  );
