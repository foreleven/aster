import {
  Array,
  Context,
  Data,
  DateTime,
  Effect,
  Layer,
  Match,
  Option,
  Schema,
  Stream,
} from "effect";
import { LarkConfig } from "../../config.js";
import { runLarkCli } from "../../shared/cli.js";
import { LarkCliError } from "../../shared/errors.js";
import { object, parseCliOutput, string } from "../../shared/response.js";
import { ChatPublicMessage, ChatHistoryArgs, type ChatBatch, type ChatMessage } from "./model.js";

export class LarkChatQueryError extends Data.TaggedError("LarkChatQueryError")<{
  readonly message: string;
  readonly cause?: unknown;
  readonly kind?: "invalid-input" | "unavailable";
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
const CliMessage = Schema.Struct({
  message_id: Schema.NonEmptyString,
  chat_id: Schema.optional(Schema.NonEmptyString),
  create_time: Schema.Union([Schema.String, Schema.Number]),
  chat_name: Schema.optional(Schema.String),
  chat_type: Schema.optional(Schema.String),
  chat_mode: Schema.optional(Schema.String),
  content: Schema.optional(Schema.Unknown),
  sender: Schema.optional(Schema.Unknown),
  message_app_link: Schema.optional(Schema.String),
  deleted: Schema.optional(Schema.Boolean),
});
const MessagePage = Schema.Struct({
  messages: Schema.Array(CliMessage),
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

const messageEvidence = Effect.fnUntraced(function* (raw: typeof CliMessage.Type) {
  const at = messageTime(raw.create_time);
  if (!Number.isFinite(at) || !Number.isFinite(new Date(at).getTime()))
    return yield* new LarkChatQueryError({ message: "Lark message has no valid timestamp" });
  return yield* Schema.decodeUnknownEffect(ChatPublicMessage)({
    id: raw.message_id,
    at: DateTime.formatIso(DateTime.makeUnsafe(at)),
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
});

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
      if (!raw.chat_id)
        return yield* new LarkChatQueryError({ message: "Search message has no chat id" });
      const group = grouped.get(raw.chat_id) ?? {
        chat: {
          id: raw.chat_id,
          name: raw.chat_name || raw.chat_id,
          mode: raw.chat_type ?? raw.chat_mode ?? "",
          description: "",
        },
        messages: new Map<string, ChatMessage>(),
      };
      const message = yield* messageEvidence(raw);
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
  const listMessages = Effect.fn("LarkChatService.listMessages")(function* (
    input: ChatHistoryArgs,
  ) {
    const args = yield* Schema.decodeUnknownEffect(ChatHistoryArgs)(input).pipe(
      Effect.mapError(
        (cause) =>
          new LarkChatQueryError({
            kind: "invalid-input",
            message: "Invalid message query arguments",
            cause,
          }),
      ),
    );
    const target = yield* Match.value({ chatId: args.chatId, userId: args.userId }).pipe(
      Match.when({ chatId: Schema.is(Schema.NonEmptyString), userId: undefined }, ({ chatId }) =>
        Effect.succeed(["--chat-id", chatId]),
      ),
      Match.when({ chatId: undefined, userId: Schema.is(Schema.NonEmptyString) }, ({ userId }) =>
        Effect.succeed(["--user-id", userId]),
      ),
      Match.orElse(() =>
        Effect.fail(
          new LarkChatQueryError({
            kind: "invalid-input",
            message: "Provide exactly one of chatId or userId",
          }),
        ),
      ),
    );
    for (const time of [args.start, args.end]) {
      if (
        time !== undefined &&
        (!/^\d{4}-\d{2}-\d{2}(?:T.*(?:Z|[+-]\d{2}:\d{2}))?$/.test(time) ||
          Option.isNone(DateTime.make(time)))
      )
        return yield* new LarkChatQueryError({
          kind: "invalid-input",
          message: "Invalid message query time; use a date or ISO timestamp with timezone",
        });
    }
    if (args.start && args.end && Date.parse(args.start) >= Date.parse(args.end))
      return yield* new LarkChatQueryError({
        kind: "invalid-input",
        message: "start must precede end",
      });
    const stdout = yield* run([
      "im",
      "+chat-messages-list",
      "--as",
      "user",
      ...target,
      "--order",
      args.order ?? "desc",
      "--page-size",
      String(args.pageSize ?? 50),
      "--format",
      "json",
      "--no-reactions",
      ...(args.start ? ["--start", args.start] : []),
      ...(args.end ? ["--end", args.end] : []),
      ...(args.pageToken ? ["--page-token", args.pageToken] : []),
    ]);
    const page = yield* decodeResponse(MessagePage, stdout);
    if (page.has_more && (!page.page_token || page.page_token === args.pageToken))
      return yield* new LarkChatQueryError({
        message: "Missing or repeated history pagination token",
      });
    const items = yield* Effect.forEach(page.messages, messageEvidence);
    return {
      items,
      hasMore: page.has_more,
      nextPageToken: page.has_more ? page.page_token! : null,
      coverage: { source: "provider" as const, complete: !page.has_more },
    };
  });
  return { searchMessages, getChatSettings, listMessages };
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
