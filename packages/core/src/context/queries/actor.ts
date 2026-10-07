import { Actor } from "@aster/actor";
import { AgentConversations } from "@aster/agent";
import { ApplicationError } from "../../operations.js";
import { ContextQueryInput } from "../contracts.js";
import { Deferred, Effect, Layer, Match, Schema } from "effect";
import { ContextRegistry } from "../registry.js";
import { ContextQueries } from "./routes.js";
import {
  QueryReply,
  queryCancelled,
  queryReplyTo,
  cancellableQuery,
} from "../../services/actors.js";
import { queryResults, textPage } from "./results.js";

export const ContextsCommand = Schema.Union([
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

type Pending = Extract<ContextsCommand, { _tag: "QueryContext" | "ReadQueryResult" }>;

/** Queries public projections, including Contexts without a live Actor. Owns no Context state. */
export class ContextsActor extends Actor.Service<
  ContextsActor,
  ContextRegistry | ContextQueries | AgentConversations
>()("context/QueriesActor", { command: ContextsCommand }) {
  static readonly layer = Layer.effect(
    ContextsActor,
    Effect.gen(function* () {
      const reader = (yield* ContextRegistry).reader;
      const results = queryResults(yield* AgentConversations, yield* ContextQueries);
      const pending = new Map<string, Pending>();
      return ContextsActor.of({
        receive: (command, actor) =>
          Match.value(command).pipe(
            Match.tag("SearchContexts", ({ query, offset, replyTo }) =>
              Effect.gen(function* () {
                const words = query.toLowerCase().split(/\s+/).filter(Boolean);
                const matches = reader
                  .directory()
                  .filter((record) =>
                    words.every((word) =>
                      `${record.path} ${record.description}`.toLowerCase().includes(word),
                    ),
                  )
                  .sort((a, b) => a.path.localeCompare(b.path));
                yield* replyTo.tell({
                  _tag: "Success",
                  value: {
                    total: matches.length,
                    items: matches.slice(offset, offset + 20).map((record) => ({
                      path: record.path,
                      description: record.description.slice(0, 240),
                    })),
                    nextOffset: offset + 20 < matches.length ? offset + 20 : null,
                  },
                });
              }),
            ),
            Match.tag("ReadContext", ({ path, offset, pageCharacters, revision, replyTo }) =>
              Effect.gen(function* () {
                const record = reader.get(path);
                if (!record)
                  return yield* replyTo.tell({
                    _tag: "Failure",
                    error: new ApplicationError({
                      kind: "not-found",
                      message: "Unknown Context path",
                    }),
                  });
                if (offset > 0 && revision !== (record.revision ?? 0))
                  return yield* replyTo.tell({
                    _tag: "Failure",
                    error: new ApplicationError({
                      kind: "conflict",
                      message: "Context changed; restart reading from the first page",
                    }),
                  });
                yield* replyTo.tell({
                  _tag: "Success",
                  value: {
                    path,
                    revision: record.revision ?? 0,
                    ...textPage(
                      JSON.stringify(record),
                      offset,
                      Math.min(12000, Math.max(1, pageCharacters)),
                    ),
                  },
                });
              }),
            ),
            Match.tag("QueryContext", "ReadQueryResult", (request) =>
              Effect.gen(function* () {
                if (yield* Deferred.isDone(request.cancelled)) return;
                if (
                  pending.size >= 4 ||
                  (request._tag === "QueryContext" &&
                    [...pending.values()].some(
                      (item) =>
                        item._tag === "QueryContext" &&
                        item.owner === request.owner &&
                        item.requestId === request.requestId,
                    ))
                )
                  return yield* request.replyTo.tell({
                    _tag: "Failure",
                    error: new ApplicationError({
                      kind: "unavailable",
                      message: "Context query already running or capacity reached",
                    }),
                  });
                pending.set(request.replyTo.path, request);
                const work =
                  request._tag === "QueryContext"
                    ? results.query(request.owner, request.requestId, request.input)
                    : results.page(request.owner, request.resultId, request.offset);
                yield* actor.pipeToSelf(cancellableQuery(work, request.cancelled), (result) => ({
                  _tag: "QuerySettled",
                  id: request.replyTo.path,
                  result,
                }));
              }),
            ),
            Match.tag("QuerySettled", ({ id, result }) =>
              Effect.gen(function* () {
                const request = pending.get(id);
                if (!request) return;
                pending.delete(id);
                yield* request.replyTo.tell(result);
              }),
            ),
            Match.exhaustive,
          ),
      });
    }),
  );
}
