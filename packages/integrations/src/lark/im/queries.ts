import { ContextCommand, ContextQueryError, DurableContext } from "@aster/core";
import { Effect, Schema } from "effect";
import { ChatState } from "./chat/state.js";
import { isChatPath, chatPath, LarkId } from "../paths.js";
import { queryError } from "../errors.js";
import { ChatHistoryArgs, ChatInfo, ChatSummary, ChatMessage } from "./model.js";

const Chats = Schema.Struct({
  items: Schema.Array(Schema.Struct({ path: Schema.String, ...ChatInfo.fields })),
  total: Schema.Int,
});
const Summary = Schema.Struct({ chat: ChatInfo, summary: Schema.NullOr(ChatSummary) });
const History = Schema.Struct({
  items: Schema.Array(ChatMessage),
  hasMore: Schema.Boolean,
  nextPageToken: Schema.NullOr(Schema.String),
  coverage: Schema.Struct({ source: Schema.Literal("provider"), complete: Schema.Boolean }),
});
export class ListChats extends ContextCommand.Class<ListChats>()("list_chats", {
  description: "List locally observed conversations, filtering by chat name or description.",
  payload: { query: Schema.optional(Schema.String) },
  success: Chats,
  error: ContextQueryError,
}) {}
export class ReadChatSummary extends ContextCommand.Class<ReadChatSummary>()("summary", {
  description: "Read the committed rolling summary by chatId.",
  payload: { chatId: LarkId },
  success: Summary,
  error: ContextQueryError,
}) {}
export class ReadChatMessages extends ContextCommand.Class<ReadChatMessages>()("messages", {
  description:
    "Read provider message history for chatId or a DM userId (exactly one). Supports start/end ISO timestamps or dates, order asc/desc, pageSize (1–50), and pageToken. Follow nextPageToken while hasMore is true.",
  payload: ChatHistoryArgs.fields,
  success: History,
  error: ContextQueryError,
}) {}
export const LarkImCommands = [ListChats, ReadChatSummary, ReadChatMessages] as const;

export const listChats = Effect.fn("LarkIm.listChats")(function* (request: {
  readonly query?: string;
}) {
  const registry = yield* DurableContext;
  const items = yield* Effect.forEach(
    registry.directory().filter(({ path }) => isChatPath(path)),
    ({ path }) =>
      registry.readState(path, ChatState).pipe(
        Effect.flatMap(({ chat }) =>
          chatPath(chat.id) === path
            ? Effect.succeed({ path, ...chat })
            : Effect.fail(
                new ContextQueryError({
                  kind: "failed",
                  message: "Retained Chat identity mismatch",
                }),
              ),
        ),
        Effect.catchTag("ContextValidationError", (error) => Effect.fail(queryError(error))),
      ),
  );
  const query = request.query?.toLowerCase();
  const matching = items
    .filter((chat) => !query || `${chat.name} ${chat.description}`.toLowerCase().includes(query))
    .sort((a, b) => a.id.localeCompare(b.id));
  return { items: matching, total: matching.length };
});
export const readChatSummary = Effect.fn("LarkIm.readChatSummary")(function* (request: {
  readonly chatId: string;
}) {
  const registry = yield* DurableContext;
  const path = chatPath(request.chatId);
  if (!registry.get(path))
    return yield* new ContextQueryError({ kind: "unavailable", message: "Chat is not available" });
  const state = yield* registry
    .readState(path, ChatState)
    .pipe(Effect.catchTag("ContextValidationError", (error) => Effect.fail(queryError(error))));
  if (state.chat.id !== request.chatId)
    return yield* new ContextQueryError({
      kind: "failed",
      message: "Retained Chat identity mismatch",
    });
  return { chat: state.chat, summary: state.summary ?? null };
});
