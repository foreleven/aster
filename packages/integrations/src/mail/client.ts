import { Context, Effect, Layer, Redacted } from "effect";
import { ImapFlow } from "imapflow";
import { simpleParser } from "mailparser";
import Pop3Command from "node-pop3";
import type { Mailbox, MailMessage } from "./model.js";
import { MailFetchError } from "./errors.js";

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
  uid: number,
  source: Buffer | string,
): Promise<MailMessage> => {
  const parsed = await simpleParser(source);
  return {
    id: String(parsed.messageId ?? uid),
    mailbox: mailbox.id,
    from: address(parsed.from?.value[0]),
    to: addresses(parsed.to),
    subject: parsed.subject ?? "",
    text: parsed.text ?? "",
    ...(parsed.date === undefined ? {} : { date: parsed.date.toISOString() }),
  };
};

const pullMailbox = (mailbox: Mailbox) =>
  Effect.tryPromise({
    try: async (signal) => {
      const client = new ImapFlow({
        host: mailbox.host,
        port: mailbox.port ?? (mailbox.secure === false ? 143 : 993),
        secure: mailbox.secure ?? true,
        auth: { user: mailbox.username, pass: Redacted.value(mailbox.password) },
        logger: false,
      });
      signal.addEventListener("abort", () => void client.logout().catch(() => undefined), {
        once: true,
      });
      await client.connect();
      try {
        const lock = await client.getMailboxLock(mailbox.folder ?? "INBOX");
        try {
          const messages: MailMessage[] = [];
          const limit = mailbox.maxMessages ?? 50;
          for await (const item of client.fetch(
            "1:*",
            { uid: true, source: true },
            { uid: true },
          )) {
            if (item.source !== undefined)
              messages.push(await normalize(mailbox, item.uid, item.source));
            if (messages.length >= limit) break;
          }
          return messages;
        } finally {
          lock.release();
        }
      } finally {
        await client.logout();
      }
    },
    catch: (cause) =>
      new MailFetchError({
        mailbox: mailbox.id,
        message: `Unable to pull mailbox ${mailbox.id}`,
        cause,
      }),
  });

const pullPop3Mailbox = (mailbox: Mailbox) =>
  Effect.tryPromise({
    try: async (signal) => {
      const client = new Pop3Command({
        user: mailbox.username,
        password: Redacted.value(mailbox.password),
        host: mailbox.host,
        port: mailbox.port ?? (mailbox.secure === false ? 110 : 995),
        tls: mailbox.secure ?? true,
      });
      let rejectAborted: (reason?: unknown) => void = () => undefined;
      const onAbort = () => rejectAborted(new Error("POP3 pull interrupted"));
      const aborted = new Promise<never>((_, reject) => {
        rejectAborted = reject;
        signal.addEventListener("abort", onAbort, { once: true });
      });
      try {
        const pull = async () => {
          await client.connect();
          const [countText] = (await client.STAT()).trim().split(/\s+/);
          const total = Number.parseInt(countText ?? "0", 10);
          const first = Math.max(1, total - (mailbox.maxMessages ?? 50) + 1);
          const messages: MailMessage[] = [];
          for (let number = first; number <= total; number++) {
            const source = await client.RETR(number);
            const text =
              typeof source === "string" ? source : await Pop3Command.stream2String(source);
            messages.push(await normalize(mailbox, number, text));
          }
          return messages;
        };
        return await Promise.race([pull(), aborted]);
      } finally {
        signal.removeEventListener("abort", onAbort);
        await client.QUIT().catch(() => undefined);
      }
    },
    catch: (cause) =>
      new MailFetchError({
        mailbox: mailbox.id,
        message: `Unable to pull mailbox ${mailbox.id}`,
        cause,
      }),
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
