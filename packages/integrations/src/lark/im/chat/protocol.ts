import { Command as ActorCommand, ReplyTo, type MailboxOf } from "@aster/actor";
import { Schema } from "effect";
import { ChatInfo, ChatMessage, ChatSummary } from "../service/model.js";
import { ChatSummaryError } from "../../shared/errors.js";
import type { ChatWork } from "./snapshot.js";
import { SummaryCommit } from "./snapshot.js";

export class Update extends ActorCommand.Class<Update>()("Update", {
  payload: {
    chat: ChatInfo,
    messages: Schema.Array(ChatMessage),
    replyTo: Schema.optional(ReplyTo<void>()),
  },
}) {}
export class Flush extends ActorCommand.Class<Flush>()("Flush", {
  payload: { date: Schema.String },
}) {}
export class GetChatInfo extends ActorCommand.Class<GetChatInfo>()("GetChatInfo", {
  payload: {},
  reply: ChatInfo,
}) {}
export class GetChatSummary extends ActorCommand.Class<GetChatSummary>()("GetChatSummary", {
  payload: {},
  reply: Schema.Struct({ chat: ChatInfo, summary: Schema.NullOr(ChatSummary) }),
}) {}
export const ChatCommands = [Update, Flush, GetChatInfo, GetChatSummary] as const;
export const ChatInternal = Schema.TaggedUnion({
  RetainReceipts: { from: Schema.String },
  GetSummaryMessages: {
    generation: Schema.String,
    replyTo: ReplyTo<ChatWork | undefined>(),
  },
  ApplySummary: {
    generation: Schema.String,
    value: SummaryCommit,
    replyTo: ReplyTo<ChatWork | undefined>(),
  },
  Summarized: {
    generation: Schema.String,
    result: Schema.TaggedUnion({
      Success: { value: Schema.Boolean },
      Failure: { error: Schema.instanceOf(ChatSummaryError) },
    }),
  },
});
export type ChatCommand = MailboxOf<typeof ChatCommands, typeof ChatInternal>;
