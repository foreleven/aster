import { LarkCliError } from "../shared/errors.js";
import { Context, Data, DateTime, Effect, Option, Schema, Stream } from "effect";
import { runLarkCli } from "../shared/cli.js";
import { parseCliOutput } from "../shared/response.js";
import { MailListArgs, type MailListPage, type EmailData, type MailboxProfile } from "./model.js";
import { parseMessages, parseMailboxProfile } from "./parser.js";
export class LarkMailCli extends Context.Service<
  LarkMailCli,
  {
    readonly getMailboxProfile: (
      mailbox: string,
    ) => Effect.Effect<MailboxProfile, LarkCliError | LarkResponseError>;
    readonly listMessages: (
      mailbox: string,
      args: MailListArgs,
    ) => Effect.Effect<MailListPage, LarkCliError | LarkResponseError>;
    readonly listIds: (
      mailbox: string,
      start: number,
      through: number,
    ) => Effect.Effect<ReadonlyArray<string>, LarkCliError | LarkResponseError>;
    readonly getMessages: (
      mailbox: string,
      ids: ReadonlyArray<string>,
    ) => Effect.Effect<ReadonlyArray<EmailData>, LarkCliError | LarkResponseError>;
  }
>()("lark/MailCli") {}

export class LarkResponseError extends Data.TaggedError("LarkResponseError")<{
  readonly cause: unknown;
  readonly kind?: "invalid-input" | "unavailable";
}> {
  override get message() {
    return `Invalid Lark mail response: ${String(this.cause)}`;
  }
}
const decode =
  <A>(parse: (stdout: string) => A) =>
  (stdout: string) =>
    Effect.try({ try: () => parse(stdout), catch: (cause) => new LarkResponseError({ cause }) });

const MailPage = Schema.Struct({
  messages: Schema.Array(Schema.Struct({ message_id: Schema.NonEmptyString })),
  has_more: Schema.Boolean,
  page_token: Schema.optional(Schema.String),
});

type MailRun = (args: ReadonlyArray<string>) => Effect.Effect<string, LarkCliError>;

// Mail search requires whole-second ISO timestamps. Effect's ISO formatter
// always includes milliseconds, so adapt its output at this transport boundary.
const formatMailTime = (seconds: number): string =>
  DateTime.formatIso(DateTime.makeUnsafe(seconds * 1000)).replace(".000Z", "+00:00");

export const makeMailClient = (run: MailRun): LarkMailCli["Service"] => {
  return {
    getMailboxProfile: (mailbox: string) =>
      run([
        "mail",
        "user_mailboxes",
        "profile",
        "--as",
        "user",
        "--params",
        JSON.stringify({ user_mailbox_id: mailbox }),
      ]).pipe(Effect.flatMap(decode(parseMailboxProfile))),
    listMessages: Effect.fn("LarkMail.listMessages")(function* (mailbox, input) {
      const args = yield* Schema.decodeUnknownEffect(MailListArgs)(input).pipe(
        Effect.mapError((cause) => new LarkResponseError({ kind: "invalid-input", cause })),
      );
      for (const time of [args.start, args.end]) {
        if (
          time !== undefined &&
          (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:Z|[+-]\d{2}:\d{2})$/.test(time) ||
            Option.isNone(DateTime.make(time)))
        )
          return yield* new LarkResponseError({
            kind: "invalid-input",
            cause: "Mail times must be whole-second ISO timestamps with timezone",
          });
      }
      if (args.start && args.end && Date.parse(args.start) >= Date.parse(args.end))
        return yield* new LarkResponseError({
          kind: "invalid-input",
          cause: "start must precede end",
        });
      const addresses = (value: string | undefined) =>
        value
          ?.split(",")
          .map((address) => address.trim())
          .filter(Boolean);
      const filter = {
        folder: args.folder ?? (args.folderId || args.label || args.labelId ? undefined : "inbox"),
        folder_id: args.folderId,
        label: args.label,
        label_id: args.labelId,
        from: addresses(args.from),
        to: addresses(args.to),
        cc: addresses(args.cc),
        bcc: addresses(args.bcc),
        subject: args.subject,
        is_unread: args.isUnread,
        has_attachment: args.hasAttachment,
        time_range:
          args.start || args.end ? { start_time: args.start, end_time: args.end } : undefined,
      };
      const stdout = yield* run([
        "mail",
        "+triage",
        "--as",
        "user",
        "--mailbox",
        mailbox,
        "--filter",
        JSON.stringify(filter),
        "--max",
        String(args.limit ?? 20),
        "--format",
        "json",
        ...(args.query ? ["--query", args.query] : []),
        ...(args.pageToken ? ["--page-token", args.pageToken] : []),
      ]);
      const page = yield* decode((stdout) =>
        Schema.decodeUnknownSync(
          Schema.Struct({
            messages: Schema.Array(
              Schema.Struct({
                message_id: Schema.NonEmptyString,
                date: Schema.optional(Schema.String),
                from: Schema.String,
                subject: Schema.String,
                labels: Schema.optional(Schema.String),
              }),
            ),
            has_more: Schema.Boolean,
            page_token: Schema.optional(Schema.String),
          }),
        )(parseCliOutput(stdout)),
      )(stdout);
      if (page.has_more && (!page.page_token || page.page_token === args.pageToken))
        return yield* new LarkResponseError({ cause: "Missing or repeated mail pagination token" });
      return {
        items: page.messages.map(({ message_id, ...message }) => ({
          messageId: message_id,
          ...message,
        })),
        hasMore: page.has_more,
        nextPageToken: page.has_more ? page.page_token! : null,
      };
    }),
    listIds: Effect.fn("LarkMail.listIds")(function* (mailbox, start, through) {
      if (!Number.isFinite(start) || !Number.isFinite(through) || start >= through)
        return yield* new LarkResponseError({ cause: "Invalid mail query window" });
      const filter = JSON.stringify({
        folder: "inbox",
        time_range: {
          start_time: formatMailTime(Math.floor(start / 1000)),
          end_time: formatMailTime(Math.ceil(through / 1000)),
        },
      });
      const tokens = new Set<string>();
      const ids = yield* Stream.paginate(undefined as string | undefined, (token) =>
        Effect.gen(function* () {
          const stdout = yield* run([
            "mail",
            "+triage",
            "--as",
            "user",
            "--mailbox",
            mailbox,
            "--filter",
            filter,
            "--max",
            "100",
            "--format",
            "json",
            ...(token === undefined ? [] : ["--page-token", token]),
          ]);
          const data = yield* decode(parseCliOutput)(stdout);
          const page = yield* Schema.decodeUnknownEffect(MailPage)(data).pipe(
            Effect.mapError((cause) => new LarkResponseError({ cause })),
          );
          if (!page.has_more)
            return [
              page.messages.map((message) => message.message_id),
              Option.none<string>(),
            ] as const;
          const next = page.page_token;
          if (!next || tokens.has(next) || tokens.size >= 999)
            return yield* new LarkResponseError({
              cause: "Invalid or repeated mail pagination token",
            });
          tokens.add(next);
          return [page.messages.map((message) => message.message_id), Option.some(next)] as const;
        }),
      ).pipe(Stream.runCollect);
      return [...new Set(ids)];
    }),
    getMessages: (mailbox: string, ids: ReadonlyArray<string>) =>
      ids.length === 0
        ? Effect.succeed([])
        : run([
            "mail",
            "+messages",
            "--as",
            "user",
            "--mailbox",
            mailbox,
            "--message-ids",
            ids.join(","),
            "--html=false",
            "--format",
            "json",
          ]).pipe(Effect.flatMap(decode((stdout) => parseMessages(stdout, mailbox)))),
  };
};

export const liveCli = (profile?: string) =>
  makeMailClient((args) =>
    Effect.tryPromise({
      try: (signal) => runLarkCli(args, profile, signal),
      catch: (cause) => new LarkCliError({ cause, message: `lark-cli failed: ${String(cause)}` }),
    }),
  );
