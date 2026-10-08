import { Actor } from "@aster/actor";
import { AgentConversations } from "@aster/agent/harness";
import { ApplicationError } from "../../operations.js";
import { ContextQueryInput } from "../contracts.js";
import { Deferred, Effect, Layer, Match, Schema } from "effect";
import { ContextQueries } from "./routes.js";
import {
  QueryReply,
  queryCancelled,
  queryReplyTo,
  cancellableQuery,
} from "../../services/actors.js";
import { queryResults } from "./results.js";

export const ContextsCommand = Schema.Union([
  Schema.TaggedStruct("ListContexts", {
    parent: Schema.optional(Schema.String),
    offset: Schema.Int,
    replyTo: queryReplyTo,
  }),
  Schema.TaggedStruct("DescribeContext", { path: Schema.String, replyTo: queryReplyTo }),
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

/** Discovers registered capabilities and owns bounded Agent query execution. */
export class ContextsActor extends Actor.Service<
  ContextsActor,
  ContextQueries | AgentConversations
>()("context/QueriesActor", { command: ContextsCommand }) {
  static readonly layer = Layer.effect(
    ContextsActor,
    Effect.gen(function* () {
      const queries = yield* ContextQueries;
      const results = queryResults(yield* AgentConversations, queries);
      const pending = new Map<string, Pending>();
      return ContextsActor.of({
        receive: (command, actor) =>
          Match.value(command).pipe(
            Match.tag("ListContexts", ({ parent, offset, replyTo }) =>
              queries.list(parent, offset).pipe(
                Effect.matchEffect({
                  onSuccess: (value) => replyTo.tell({ _tag: "Success", value }),
                  onFailure: (error) =>
                    replyTo.tell({
                      _tag: "Failure",
                      error: new ApplicationError({
                        kind: "invalid-input",
                        message: error.message,
                      }),
                    }),
                }),
              ),
            ),
            Match.tag("DescribeContext", ({ path, replyTo }) =>
              queries.describe(path).pipe(
                Effect.matchEffect({
                  onSuccess: (value) => replyTo.tell({ _tag: "Success", value }),
                  onFailure: (error) =>
                    replyTo.tell({
                      _tag: "Failure",
                      error: new ApplicationError({ kind: "unavailable", message: error.message }),
                    }),
                }),
              ),
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
