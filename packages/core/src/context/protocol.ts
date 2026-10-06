import { ReplyTo } from "@aster/actor";
import { ContextQueryInput } from "@aster/api-contracts";
import { Schema } from "effect";
import { QueryReply, queryCancelled, queryReplyTo } from "./query-protocol.js";

export const ContextsCommand = Schema.Union([
  Schema.TaggedStruct("Ready", { replyTo: ReplyTo<void>() }),
  Schema.TaggedStruct("SearchContexts", {
    query: Schema.String,
    offset: Schema.Int,
    replyTo: queryReplyTo,
  }),
  Schema.TaggedStruct("ReadContext", {
    path: Schema.String,
    offset: Schema.Int,
    pageCharacters: Schema.Int,
    revision: Schema.optional(Schema.Int),
    replyTo: queryReplyTo,
  }),
  Schema.TaggedStruct("QueryContext", {
    input: ContextQueryInput,
    owner: Schema.String,
    requestId: Schema.String,
    cancelled: queryCancelled,
    replyTo: queryReplyTo,
  }),
  Schema.TaggedStruct("ReadQueryResult", {
    owner: Schema.String,
    resultId: Schema.Int,
    offset: Schema.Int,
    cancelled: queryCancelled,
    replyTo: queryReplyTo,
  }),
  Schema.TaggedStruct("QuerySettled", { id: Schema.String, result: QueryReply }),
]);
export type ContextsCommand = typeof ContextsCommand.Type;
