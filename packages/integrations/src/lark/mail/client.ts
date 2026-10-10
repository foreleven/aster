import { LarkId } from "../paths.js";
import { LarkCliError } from "../errors.js";
import { Context, Data, DateTime, Effect, Layer, Option, Schema, Stream } from "effect";
import { decodeCliOutput, LarkCli } from "../cli.js";
import { MailListArgs, type MailListPage, type EmailData, type MailboxProfile } from "./model.js";
export class LarkMailClient extends Context.Service<
  LarkMailClient,
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
>()("lark/MailClient") {
  static readonly layer = Layer.effect(
    LarkMailClient,
    Effect.map(LarkCli, (cli) => makeMailClient(cli.run)),
  );
}

export class LarkResponseError extends Data.TaggedError("LarkResponseError")<{
  readonly cause: unknown;
  readonly kind?: "invalid-input" | "unavailable";
}> {
  override get message() {
    return `Invalid Lark mail response: ${String(this.cause)}`;
  }
}
const decodeResponse = <A>(schema: Schema.ConstraintDecoder<A>, stdout: string) =>
  decodeCliOutput(stdout).pipe(
    Effect.mapError((cause) => new LarkResponseError({ cause })),
    Effect.flatMap(Schema.decodeUnknownEffect(schema)),
    Effect.catchTag("SchemaError", (cause) => Effect.fail(new LarkResponseError({ cause }))),
  );
const RawProfile = Schema.Struct({
  primary_email_address: Schema.optional(Schema.NonEmptyString),
  mail_address: Schema.optional(Schema.NonEmptyString),
  email: Schema.optional(Schema.NonEmptyString),
  name: Schema.optional(Schema.String),
  display_name: Schema.optional(Schema.String),
});
const ProfileResponse = Schema.Union([
  Schema.Struct({ profile: RawProfile }),
  Schema.Struct({ user_mailbox: RawProfile }),
  RawProfile,
]);
const MessagesResponse = Schema.Struct({
  messages: Schema.Array(
    Schema.Struct({
      message_id: LarkId,
      head_from: Schema.Struct({
        name: Schema.optional(Schema.String),
        mail_address: Schema.String,
      }),
      subject: Schema.String,
      body_plain_text: Schema.String,
      attachments: Schema.optional(
        Schema.Array(
          Schema.Struct({
            id: Schema.String,
            filename: Schema.String,
            content_type: Schema.String,
            is_inline: Schema.Boolean,
          }),
        ),
      ),
    }),
  ),
});

const MailPage = Schema.Struct({
  messages: Schema.Array(Schema.Struct({ message_id: LarkId })),
  has_more: Schema.Boolean,
  page_token: Schema.optional(Schema.String),
});

type MailRun = (args: ReadonlyArray<string>) => Effect.Effect<string, LarkCliError>;

// Mail search requires whole-second ISO timestamps. Effect's ISO formatter
// always includes milliseconds, so adapt its output at this transport boundary.
const formatMailTime = (seconds: number): string =>
  DateTime.formatIso(DateTime.makeUnsafe(seconds * 1000)).replace(".000Z", "+00:00");

export const makeMailClient = (run: MailRun): LarkMailClient["Service"] => {
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
      ]).pipe(
        Effect.flatMap((stdout) => decodeResponse(ProfileResponse, stdout)),
        Effect.flatMap((response) => {
          const profile =
            "profile" in response
              ? response.profile
              : "user_mailbox" in response
                ? response.user_mailbox
                : response;
          const address = profile.primary_email_address ?? profile.mail_address ?? profile.email;
          return address
            ? Effect.succeed({ address, name: profile.name ?? profile.display_name ?? "" })
            : Effect.fail(new LarkResponseError({ cause: "Missing mailbox address" }));
        }),
      ),
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
      const page = yield* decodeResponse(
        Schema.Struct({
          messages: Schema.Array(
            Schema.Struct({
              message_id: LarkId,
              date: Schema.optional(Schema.String),
              from: Schema.String,
              subject: Schema.String,
              labels: Schema.optional(Schema.String),
            }),
          ),
          has_more: Schema.Boolean,
          page_token: Schema.optional(Schema.String),
        }),
        stdout,
      );
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
          const page = yield* decodeResponse(MailPage, stdout);
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
          ]).pipe(
            Effect.flatMap((stdout) => decodeResponse(MessagesResponse, stdout)),
            Effect.map(({ messages }) =>
              messages.map((message) => ({
                messageId: message.message_id,
                mailbox,
                from: [message.head_from.name, message.head_from.mail_address]
                  .filter(Boolean)
                  .join(" "),
                subject: message.subject,
                bodyPlainText: message.body_plain_text,
                attachments: (message.attachments ?? []).map((a) => ({
                  id: a.id,
                  filename: a.filename,
                  contentType: a.content_type,
                  isInline: a.is_inline,
                })),
              })),
            ),
          ),
  };
};
