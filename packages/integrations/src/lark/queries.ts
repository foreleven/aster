import {
  ContextCommand,
  ContextListArgs,
  contextPage,
  ContextQueryError,
  ContextRegistry,
  publicJson,
} from "@aster/core";
import { DateTime, Effect, Schema } from "effect";
import { AccountProfile } from "./account/model.js";
import { LarkConfig } from "./config.js";
import { ChatInfo, ChatPublicMessage } from "./im/service/model.js";
import { ChatSummary } from "./im/service/model.js";
import { LarkMailCli } from "./mail/client.js";
import { EmailData, MailboxProfile } from "./mail/model.js";
const chatArgs = Schema.Struct({ ...ContextListArgs.fields, id: Schema.NonEmptyString });
const mailArgs = Schema.Struct({
  ...ContextListArgs.fields,
  date: Schema.optional(Schema.String.check(Schema.isPattern(/^\d{4}-\d{2}-\d{2}$/))),
});
const idArgs = Schema.Struct({ id: Schema.NonEmptyString });
const chatState = Schema.Struct({ chat: ChatInfo, summary: Schema.optional(ChatSummary) });
const unavailable = () =>
  new ContextQueryError({ kind: "unavailable", message: "Requested evidence is not available" });

export class AccountProfileQuery extends ContextCommand.Class<AccountProfileQuery>()("profile", {
  description: "Read the connected account's public identity.",
  payload: Schema.Struct({}).fields,
}) {}
export const LarkAccountCommands = [AccountProfileQuery] as const;
export const makeLarkAccountQuery = Effect.gen(function* () {
  const registry = yield* ContextRegistry;
  const reply = Effect.fnUntraced(function* (path: string, command: string, data: unknown) {
    return {
      path,
      command,
      queriedAt: DateTime.formatIso(yield* DateTime.now),
      data: publicJson(data),
    };
  });
  return Effect.fnUntraced(function* (command: AccountProfileQuery) {
    const record = registry.reader.get("/lark");
    const value =
      record &&
      Schema.decodeUnknownOption(Schema.Struct({ account: AccountProfile }))(record.state);
    if (!value || value._tag === "None") return yield* unavailable();
    return yield* reply("/lark", command._tag, value.value.account);
  });
});

export class ListChats extends ContextCommand.Class<ListChats>()("list_chats", {
  description: "List known chats, filtering by name or description.",
  payload: ContextListArgs.fields,
}) {}
export class ReadChatSummary extends ContextCommand.Class<ReadChatSummary>()("summary", {
  description: "Read the current rolling summary for a chat id.",
  payload: idArgs.fields,
}) {}
export class ReadChatMessages extends ContextCommand.Class<ReadChatMessages>()("messages", {
  description:
    "Read the retained message window for a chat id. Coverage is partial; this is not a complete historical archive.",
  payload: chatArgs.fields,
}) {}
export const LarkImCommands = [ListChats, ReadChatSummary, ReadChatMessages] as const;
export const makeLarkImQuery = Effect.gen(function* () {
  const registry = yield* ContextRegistry;
  const reply = Effect.fnUntraced(function* (path: string, command: string, data: unknown) {
    return {
      path,
      command,
      queriedAt: DateTime.formatIso(yield* DateTime.now),
      data: publicJson(data),
    };
  });
  return Effect.fnUntraced(function* (command: ListChats | ReadChatSummary | ReadChatMessages) {
    if (command._tag === "list_chats") {
      const args = yield* Schema.decodeUnknownEffect(ContextListArgs)(command).pipe(Effect.orDie);
      const items = registry.reader
        .directory()
        .filter((r) => /^\/lark\/im\/chats\/[^/]+$/.test(r.path))
        .flatMap(({ path }) => {
          const r = registry.reader.get(path)!;
          const value = Schema.decodeUnknownOption(chatState)(r.state);
          return value._tag === "Some" ? [{ path: r.path, ...value.value.chat }] : [];
        })
        .filter(
          (item) =>
            !args.query ||
            `${item.name} ${item.description}`.toLowerCase().includes(args.query.toLowerCase()),
        )
        .sort((a, b) => a.id.localeCompare(b.id));
      return yield* reply("/lark/im", command._tag, contextPage(items, args));
    }
    const args = yield* Schema.decodeUnknownEffect(chatArgs)(command).pipe(Effect.orDie);
    if (args.id.includes("/"))
      return yield* new ContextQueryError({
        kind: "invalid-input",
        message: "Invalid chat id",
      });
    const record = registry.reader.get(`/lark/im/chats/${args.id}`);
    if (!record) return yield* unavailable();
    const state = yield* Schema.decodeUnknownEffect(chatState)(record.state).pipe(Effect.orDie);
    if (command._tag === "summary")
      return yield* reply("/lark/im", command._tag, {
        chat: state.chat,
        summary: state.summary ?? null,
      });
    const messages = yield* Schema.decodeUnknownEffect(Schema.Array(ChatPublicMessage))(
      record.messages,
    ).pipe(Effect.orDie);
    const filtered = messages.filter(
      (m) => !args.query || m.content.toLowerCase().includes(args.query.toLowerCase()),
    );
    return yield* reply("/lark/im", command._tag, {
      chat: state.chat,
      coverage: { complete: false, source: "retained-window" },
      ...contextPage(filtered, args),
    });
  });
});

export class MailProfile extends ContextCommand.Class<MailProfile>()("profile", {
  description: "Read mailbox identity.",
  payload: Schema.Struct({}).fields,
}) {}
export class ListMail extends ContextCommand.Class<ListMail>()("list", {
  description:
    "List provider inbox mail for a calendar date in Asia/Shanghai (default today), filtering sender or subject. Returns metadata without bodies.",
  payload: mailArgs.fields,
}) {}
export class ReadMail extends ContextCommand.Class<ReadMail>()("read", {
  description: "Read one email by its provider message id, preferring retained evidence.",
  payload: idArgs.fields,
}) {}
export const LarkMailCommands = [MailProfile, ListMail, ReadMail] as const;
export const makeLarkMailQuery = Effect.gen(function* () {
  const registry = yield* ContextRegistry;
  const config = yield* LarkConfig;
  const mail = yield* LarkMailCli;
  const reply = Effect.fnUntraced(function* (path: string, command: string, data: unknown) {
    return {
      path,
      command,
      queriedAt: DateTime.formatIso(yield* DateTime.now),
      data: publicJson(data),
    };
  });
  return Effect.fnUntraced(
    function* (command: MailProfile | ListMail | ReadMail) {
      if (command._tag === "profile") {
        const profile = yield* mail.getMailboxProfile(config.mail.mailbox);
        return yield* reply(
          "/lark/mail",
          command._tag,
          Schema.decodeUnknownSync(MailboxProfile)(profile),
        );
      }
      if (command._tag === "read") {
        const { id } = yield* Schema.decodeUnknownEffect(idArgs)(command).pipe(Effect.orDie);
        if (id.includes("/"))
          return yield* new ContextQueryError({
            kind: "invalid-input",
            message: "Invalid email id",
          });
        const record = registry.get(`/lark/mail/${config.mail.mailbox}/${id}`);
        const email = record
          ? yield* Schema.decodeUnknownEffect(EmailData)(record.state).pipe(Effect.orDie)
          : (yield* mail.getMessages(config.mail.mailbox, [id])).find(
              (email) => email.messageId === id,
            );
        if (!email) return yield* unavailable();
        return yield* reply("/lark/mail", command._tag, email);
      }
      const args = yield* Schema.decodeUnknownEffect(mailArgs)(command).pipe(Effect.orDie);
      const date =
        args.date ??
        DateTime.formatIsoDate(DateTime.setZoneNamedUnsafe(yield* DateTime.now, "Asia/Shanghai"));
      const start = DateTime.makeZoned(`${date}T00:00:00Z`, {
        timeZone: "Asia/Shanghai",
        adjustForTimeZone: true,
      });
      if (start._tag === "None" || DateTime.formatIsoDate(start.value) !== date)
        return yield* new ContextQueryError({
          kind: "invalid-input",
          message: "Invalid calendar date",
        });
      const ids = yield* mail.listIds(
        config.mail.mailbox,
        DateTime.toEpochMillis(start.value),
        DateTime.toEpochMillis(DateTime.add(start.value, { days: 1 })),
      );
      const groups = [];
      for (let offset = 0; offset < ids.length; offset += 50)
        groups.push(ids.slice(offset, offset + 50));
      const emails = (yield* Effect.forEach(
        groups,
        (ids) => mail.getMessages(config.mail.mailbox, ids),
        { concurrency: 2 },
      )).flat();
      if (ids.some((id) => !emails.some((email) => email.messageId === id)))
        return yield* unavailable();
      const items = emails
        .filter(
          (email) =>
            !args.query ||
            `${email.from} ${email.subject}`.toLowerCase().includes(args.query.toLowerCase()),
        )
        .map(({ messageId, from, subject }) => ({ id: messageId, from, subject }));
      return yield* reply("/lark/mail", command._tag, {
        date,
        timeZone: "Asia/Shanghai",
        coverage: { complete: true, source: "provider" },
        ...contextPage(items, args),
      });
    },
    Effect.catchTags({
      LarkCliError: () => Effect.fail(unavailable()),
      LarkResponseError: () => Effect.fail(unavailable()),
    }),
  );
});
