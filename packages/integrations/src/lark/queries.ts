import {
  ContextQueries,
  ContextQueryError,
  ContextRegistry,
  ContextListArgs,
  contextPage,
  publicJson,
} from "@aster/core";
import { DateTime, Effect, Schema } from "effect";
import { LarkConfig } from "./config.js";
import { LarkMailCli } from "./mail/client.js";
import { EmailData, MailboxProfile } from "./mail/model.js";
import { AccountProfile } from "./account/model.js";
import { ImChat, PublicImMessage } from "./im/model.js";
import { ChatSummary } from "./im/summarizer.js";
const chatArgs = Schema.Struct({ ...ContextListArgs.fields, id: Schema.NonEmptyString });
const mailArgs = Schema.Struct({
  ...ContextListArgs.fields,
  date: Schema.optional(Schema.String.check(Schema.isPattern(/^\d{4}-\d{2}-\d{2}$/))),
});
const idArgs = Schema.Struct({ id: Schema.NonEmptyString });
const chatState = Schema.Struct({ chat: ImChat, summary: Schema.optional(ChatSummary) });
const unavailable = () =>
  new ContextQueryError({ kind: "unavailable", message: "Requested evidence is not available" });

export const registerLarkQueries = Effect.fn("Lark.registerQueries")(function* () {
  const registry = yield* ContextRegistry;
  const queries = yield* ContextQueries;
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
  yield* queries.register(
    "/lark",
    {
      description: config.description,
      commands: {
        profile: {
          description: "Read the connected account's public identity.",
          schema: Schema.Struct({}),
        },
      },
    },
    Effect.fnUntraced(function* (input) {
      const record = registry.reader.get("/lark");
      const value =
        record &&
        Schema.decodeUnknownOption(Schema.Struct({ account: AccountProfile }))(record.state);
      if (!value || value._tag === "None") return yield* unavailable();
      return yield* reply(input.path, input.command, value.value.account);
    }),
  );
  if (config.im)
    yield* queries.register(
      "/lark/im",
      {
        description: "Find connected chats and read retained summaries and message evidence.",
        commands: {
          list_chats: {
            description: "List known chats, filtering by name or description.",
            schema: ContextListArgs,
          },
          summary: {
            description: "Read the current rolling summary for a chat id.",
            schema: idArgs,
          },
          messages: {
            description:
              "Read the retained message window for a chat id. Coverage is partial; this is not a complete historical archive.",
            schema: chatArgs,
          },
        },
      },
      Effect.fnUntraced(function* (input) {
        if (input.command === "list_chats") {
          const args = yield* Schema.decodeUnknownEffect(ContextListArgs)(input.args).pipe(
            Effect.orDie,
          );
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
          return yield* reply(input.path, input.command, contextPage(items, args));
        }
        const args = yield* Schema.decodeUnknownEffect(chatArgs)(input.args).pipe(Effect.orDie);
        if (args.id.includes("/"))
          return yield* new ContextQueryError({
            kind: "invalid-input",
            message: "Invalid chat id",
          });
        const record = registry.reader.get(`/lark/im/chats/${args.id}`);
        if (!record) return yield* unavailable();
        const state = yield* Schema.decodeUnknownEffect(chatState)(record.state).pipe(Effect.orDie);
        if (input.command === "summary")
          return yield* reply(input.path, input.command, {
            chat: state.chat,
            summary: state.summary ?? null,
          });
        const messages = yield* Schema.decodeUnknownEffect(Schema.Array(PublicImMessage))(
          record.messages,
        ).pipe(Effect.orDie);
        const filtered = messages.filter(
          (m) => !args.query || m.content.toLowerCase().includes(args.query.toLowerCase()),
        );
        return yield* reply(input.path, input.command, {
          chat: state.chat,
          coverage: { complete: false, source: "retained-window" },
          ...contextPage(filtered, args),
        });
      }),
    );
  yield* queries.register(
    "/lark/mail",
    {
      description: config.mail.description,
      commands: {
        profile: { description: "Read mailbox identity.", schema: Schema.Struct({}) },
        list: {
          description:
            "List provider inbox mail for a calendar date in Asia/Shanghai (default today), filtering sender or subject. Returns metadata without bodies.",
          schema: mailArgs,
        },
        read: {
          description: "Read one email by its provider message id, preferring retained evidence.",
          schema: idArgs,
        },
      },
    },
    Effect.fnUntraced(
      function* (input) {
        if (input.command === "profile") {
          const profile = yield* mail.getMailboxProfile(config.mail.mailbox);
          return yield* reply(
            input.path,
            input.command,
            Schema.decodeUnknownSync(MailboxProfile)(profile),
          );
        }
        if (input.command === "read") {
          const { id } = yield* Schema.decodeUnknownEffect(idArgs)(input.args).pipe(Effect.orDie);
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
          return yield* reply(input.path, input.command, email);
        }
        const args = yield* Schema.decodeUnknownEffect(mailArgs)(input.args).pipe(Effect.orDie);
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
        return yield* reply(input.path, input.command, {
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
    ),
  );
});
