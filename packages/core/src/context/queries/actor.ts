import type { MailboxOf } from "@aster/actor";
import { Actor, Command as ActorCommand } from "@aster/actor";
import { AgentConversations } from "@aster/agent/harness";
import { Deferred, Effect, Match, Schema } from "effect";
import { ApplicationError } from "../../operations.js";
import { QueryReply, cancellableQuery, queryCancelled } from "../../services/actors.js";
import { ContextQueryInput } from "../contracts.js";
import { queryResults } from "./results.js";
import { ContextQueries } from "./routes.js";

export class ListContexts extends ActorCommand.Class<ListContexts>()("ListContexts", {
  payload: { parent: Schema.optional(Schema.String), offset: Schema.Int },
  reply: QueryReply,
}) {}
export class DescribeContext extends ActorCommand.Class<DescribeContext>()("DescribeContext", {
  payload: { path: Schema.String },
  reply: QueryReply,
}) {}
export class QueryContext extends ActorCommand.Class<QueryContext>()("QueryContext", {
  payload: {
    input: ContextQueryInput,
    owner: Schema.String,
    requestId: Schema.String,
    cancelled: queryCancelled,
  },
  reply: QueryReply,
}) {}
export class ReadQueryResult extends ActorCommand.Class<ReadQueryResult>()("ReadQueryResult", {
  payload: {
    owner: Schema.String,
    resultId: Schema.Int,
    offset: Schema.Int,
    cancelled: queryCancelled,
  },
  reply: QueryReply,
}) {}
export const ContextsCommands = [
  ListContexts,
  DescribeContext,
  QueryContext,
  ReadQueryResult,
] as const;
export const ContextsInternal = Schema.TaggedUnion({
  QuerySettled: { id: Schema.String, result: QueryReply },
});
export type ContextsCommand = MailboxOf<typeof ContextsCommands, typeof ContextsInternal>;

type Pending = Extract<ContextsCommand, { _tag: "QueryContext" | "ReadQueryResult" }>;

/** Discovers registered capabilities and owns bounded Agent query execution. */
export const ContextsActor = Actor.define("context/QueriesActor", {
  commands: ContextsCommands,
  internal: ContextsInternal,
})(
  Effect.gen(function* () {
    const queries = yield* ContextQueries;
    const results = queryResults(yield* AgentConversations, queries);
    const pending = new Map<string, Pending>();
    return {
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
    };
  }),
);
