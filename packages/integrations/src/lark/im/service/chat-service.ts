import { Array, Context, Data, Effect, Layer, Option, Schema, Stream } from "effect";
import { LarkConfig } from "../../config.js";
import { runLarkCli } from "../../shared/cli.js";
import { LarkCliError } from "../../shared/errors.js";
import { object, parseCliOutput, string } from "../../shared/response.js";
import { ChatPublicMessage, type ChatBatch, type ChatMessage } from "./model.js";

export class LarkChatQueryError extends Data.TaggedError("LarkChatQueryError")<{
  readonly message: string;
  readonly cause?: unknown;
}> {}

export interface ChatMessageQuery {
  readonly start: string;
  readonly end: string;
  readonly query?: string;
  readonly excludeMuted?: boolean;
}
export interface ChatUserSetting {
  readonly chatId: string;
  readonly isMuted: boolean;
}

type CliRun = (args: readonly string[]) => Effect.Effect<string, LarkCliError>;
const MessagePage = Schema.Struct({
  messages: Schema.Array(
    Schema.Struct({
      message_id: Schema.NonEmptyString,
      chat_id: Schema.NonEmptyString,
      create_time: Schema.Union([Schema.String, Schema.Number]),
      chat_name: Schema.optional(Schema.String),
      chat_type: Schema.optional(Schema.String),
      chat_mode: Schema.optional(Schema.String),
      content: Schema.optional(Schema.Unknown),
      sender: Schema.optional(Schema.Unknown),
      message_app_link: Schema.optional(Schema.String),
      deleted: Schema.optional(Schema.Boolean),
    }),
  ),
  has_more: Schema.Boolean,
  page_token: Schema.optional(Schema.String),
});
const SettingsResponse = Schema.Struct({
  items: Schema.Array(Schema.Struct({ chat_id: Schema.NonEmptyString, is_muted: Schema.Boolean })),
});
const decodeResponse = <A>(schema: Schema.ConstraintDecoder<A>, stdout: string) =>
  Effect.try({
    try: () => parseCliOutput(stdout),
    catch: (cause) => new LarkChatQueryError({ message: "Invalid Lark chat response", cause }),
  }).pipe(
    Effect.flatMap(Schema.decodeUnknownEffect(schema)),
    Effect.catchTag("SchemaError", (cause) =>
      Effect.fail(new LarkChatQueryError({ message: "Invalid Lark chat response", cause })),
    ),
  );

// CLI search accepts whole seconds; local filtering restores the exact requested bounds.
const formatTime = (at: number) => new Date(at).toISOString().replace(".000Z", "Z");
const messageTime = (value: string | number) => {
  const parsed =
    typeof value === "number" || /^\d+$/.test(value) ? Number(value) : Date.parse(value);
  return Math.abs(parsed) < 100_000_000_000 ? parsed * 1000 : parsed;
};

const make = (run: CliRun) => {
  const getChatSettings = Effect.fn("LarkChatService.getChatSettings")(function* (
    chatIds: readonly string[],
  ) {
    const settings: ChatUserSetting[] = [];
    for (const ids of Array.chunksOf([...new Set(chatIds)], 10)) {
      if (ids.some((id) => !id.trim()))
        return yield* new LarkChatQueryError({ message: "Invalid chat id" });
      const stdout = yield* run([
        "im",
        "chat.user_setting",
        "batch_query",
        "--as",
        "user",
        "--data",
        JSON.stringify({ chat_ids: ids }),
        "--format",
        "json",
      ]);
      const { items } = yield* decodeResponse(SettingsResponse, stdout);
      const seen = new Set<string>();
      for (const item of items) {
        if (!ids.includes(item.chat_id) || seen.has(item.chat_id))
          return yield* new LarkChatQueryError({ message: "Invalid Lark mute status" });
        seen.add(item.chat_id);
        settings.push({ chatId: item.chat_id, isMuted: item.is_muted });
      }
      if (seen.size !== ids.length)
        return yield* new LarkChatQueryError({ message: "Incomplete Lark mute status" });
    }
    return settings;
  });
  const searchMessages = Effect.fn("LarkChatService.searchMessages")(function* (
    query: ChatMessageQuery,
  ) {
    const from = Date.parse(query.start),
      through = Date.parse(query.end);
    if (!Number.isFinite(from) || !Number.isFinite(through) || from >= through)
      return yield* new LarkChatQueryError({ message: "Invalid IM query window" });
    const tokens = new Set<string>();
    const rows = yield* Stream.paginate(undefined as string | undefined, (token) =>
      Effect.gen(function* () {
        const stdout = yield* run([
          "im",
          "+messages-search",
          "--as",
          "user",
          "--query",
          query.query ?? "",
          "--start",
          formatTime(Math.floor(from / 1000) * 1000),
          "--end",
          formatTime(Math.ceil(through / 1000) * 1000),
          "--page-size",
          "50",
          "--format",
          "json",
          "--no-reactions",
          ...(token ? ["--page-token", token] : []),
        ]);
        const page = yield* decodeResponse(MessagePage, stdout);
        if (!page.has_more) return [page.messages, Option.none<string>()] as const;
        if (!page.page_token || tokens.has(page.page_token) || tokens.size >= 999)
          return yield* new LarkChatQueryError({
            message: "Missing or repeated IM pagination token, or page limit exceeded",
          });
        tokens.add(page.page_token);
        return [page.messages, Option.some(page.page_token)] as const;
      }),
    ).pipe(Stream.runCollect);
    const grouped = new Map<
      string,
      { chat: ChatBatch["chat"]; messages: Map<string, ChatMessage> }
    >();
    for (const raw of rows) {
      const at = messageTime(raw.create_time);
      if (!Number.isFinite(at) || !Number.isFinite(new Date(at).getTime()))
        return yield* new LarkChatQueryError({ message: "Lark message has no valid timestamp" });
      if (at < from || at >= through) continue;
      const group = grouped.get(raw.chat_id) ?? {
        chat: {
          id: raw.chat_id,
          name: raw.chat_name || raw.chat_id,
          mode: raw.chat_type ?? raw.chat_mode ?? "",
          description: "",
        },
        messages: new Map<string, ChatMessage>(),
      };
      const message = yield* Schema.decodeUnknownEffect(ChatPublicMessage)({
        id: raw.message_id,
        at: new Date(at).toISOString(),
        content:
          typeof raw.content === "string"
            ? raw.content
            : string(object(raw.content).text) || JSON.stringify(raw.content ?? {}),
        sender: object(raw.sender),
        url: raw.message_app_link ?? "",
        deleted: raw.deleted === true,
      }).pipe(
        Effect.mapError(
          (cause) => new LarkChatQueryError({ message: "Invalid Lark message evidence", cause }),
        ),
      );
      group.messages.set(raw.message_id, message);
      grouped.set(raw.chat_id, group);
    }
    const settings = query.excludeMuted ? yield* getChatSettings([...grouped.keys()]) : [];
    const muted = new Set(settings.filter((item) => item.isMuted).map((item) => item.chatId));
    return [...grouped.values()]
      .filter((group) => !muted.has(group.chat.id))
      .map((group): ChatBatch => ({
        chat: group.chat,
        messages: [...group.messages.values()].sort(
          (a, b) => a.at.localeCompare(b.at) || a.id.localeCompare(b.id),
        ),
      }));
  });
  return { searchMessages, getChatSettings };
};

export class LarkChatService extends Context.Service<LarkChatService, ReturnType<typeof make>>()(
  "lark/LarkChatService",
) {
  /** Inject an Effect transport for local tests; only the live layer bridges the native CLI. */
  static readonly make = make;
  static readonly layer = Layer.effect(
    LarkChatService,
    Effect.gen(function* () {
      const { profile } = yield* LarkConfig;
      return make((args) =>
        Effect.tryPromise({
          try: (signal) => runLarkCli(args, profile, signal),
          catch: (cause) =>
            new LarkCliError({ cause, message: `lark-cli failed: ${String(cause)}` }),
        }),
      );
    }),
  );
}
