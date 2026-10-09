import { Command, type ActorContext, type ActorRef } from "@aster/actor";
import { ContextCommand, ContextQueryError, childActorName } from "@aster/core";
import { Effect, Schema } from "effect";
import { GetChatInfo, GetChatSummary } from "./chat/protocol.js";
import { ChatHistoryArgs, ChatInfo, ChatSummary, ChatPublicMessage } from "./service/model.js";

const Chats = Schema.Struct({
  items: Schema.Array(Schema.Struct({ path: Schema.String, ...ChatInfo.fields })),
  total: Schema.Int,
});
const Summary = Schema.Struct({ chat: ChatInfo, summary: Schema.NullOr(ChatSummary) });
const History = Schema.Struct({
  items: Schema.Array(ChatPublicMessage),
  hasMore: Schema.Boolean,
  nextPageToken: Schema.NullOr(Schema.String),
  coverage: Schema.Struct({ source: Schema.Literal("provider"), complete: Schema.Boolean }),
});
const HistoryError = Schema.TaggedUnion({
  LarkChatQueryError: { message: Schema.String, kind: Schema.optional(Schema.String) },
  LarkCliError: { message: Schema.String },
});

export class ListChats extends ContextCommand.Class<ListChats>()("list_chats", {
  description: "List active Chat Actors, filtering by chat name or description.",
  payload: { query: Schema.optional(Schema.String) },
  success: Chats,
  error: ContextQueryError,
}) {}
export class ReadChatSummary extends ContextCommand.Class<ReadChatSummary>()("summary", {
  description: "Read the Chat Actor's current rolling summary by chatId.",
  payload: { chatId: Schema.NonEmptyString.check(Schema.isPattern(/^[^/]+$/)) },
  success: Summary,
  error: ContextQueryError,
}) {}
export class ReadChatMessages extends ContextCommand.Class<ReadChatMessages>()("messages", {
  description:
    "Read provider message history for chatId or a DM userId (exactly one). Supports start/end ISO timestamps or dates, order asc/desc, pageSize (1–50), and pageToken. Follow nextPageToken while hasMore is true.",
  payload: ChatHistoryArgs.fields,
  success: History,
  error: HistoryError,
}) {}
export const LarkImCommands = [ListChats, ReadChatSummary, ReadChatMessages] as const;

export const listChats = Effect.fn("LarkIm.listChats")(
  function* (request: ListChats, actor: Pick<ActorContext<unknown>, "children">) {
    const children = (yield* actor.children()).filter((child) => {
      const name = child.path.split("/").at(-1)!;
      const relative = name.startsWith("~")
        ? Buffer.from(name.slice(1), "base64url").toString("utf8")
        : name;
      return /^chats\/[^/]+$/.test(relative);
    });
    const items = yield* Effect.forEach(
      children,
      (child) =>
        (child as ActorRef<GetChatInfo>)
          .ask<Command.Reply<typeof GetChatInfo>>((replyTo) => new GetChatInfo({ replyTo }))
          .pipe(Effect.map((chat) => ({ path: `/lark/im/chats/${chat.id}`, ...chat }))),
      { concurrency: 4 },
    );
    const query = request.query?.toLowerCase();
    const matching = items
      .filter((chat) => !query || `${chat.name} ${chat.description}`.toLowerCase().includes(query))
      .sort((a, b) => a.id.localeCompare(b.id));
    return { items: matching, total: matching.length };
  },
  Effect.catchTag("AskTimeoutError", () =>
    Effect.fail(new ContextQueryError({ kind: "unavailable", message: "Chat Actor unavailable" })),
  ),
);

export const readChatSummary = Effect.fn("LarkIm.readChatSummary")(
  function* (request: ReadChatSummary, actor: Pick<ActorContext<unknown>, "child">) {
    const child = yield* actor.child(childActorName(`chats/${request.chatId}`));
    if (!child)
      return yield* new ContextQueryError({
        kind: "unavailable",
        message: "Chat Actor unavailable",
      });
    return yield* (child as ActorRef<GetChatSummary>).ask<Command.Reply<typeof GetChatSummary>>(
      (replyTo) => new GetChatSummary({ replyTo }),
    );
  },
  Effect.catchTag("AskTimeoutError", () =>
    Effect.fail(new ContextQueryError({ kind: "unavailable", message: "Chat Actor unavailable" })),
  ),
);
