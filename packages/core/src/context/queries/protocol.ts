import { Command, ReplyTo } from "@aster/actor";
import { Deferred, Schema } from "effect";
import { ContextQueryError, ContextQueryInput, ContextQueryResult } from "../contracts.js";

/** Opt in to Context discovery; local Actor commands are never exposed implicitly. */
export const ContextCommand = {
  Class:
    <Self>() =>
    <
      const Tag extends string,
      const F extends Readonly<Record<string, Schema.Codec<unknown, unknown>>>,
    >(
      tag: Tag,
      options: { readonly payload: F; readonly description: string },
    ) =>
      Object.assign(
        Command.Class<Self>()(tag, {
          ...options,
          success: ContextQueryResult,
          error: ContextQueryError,
        }),
        { contextQuery: true as const },
      ),
};
export interface QueryCommand extends Command.Contract {
  readonly contextQuery: true;
  readonly payloadSchema: Schema.Codec<unknown, unknown>;
  readonly Type: { readonly _tag: string; readonly replyTo: ReplyTo<typeof QueryReply.Type> };
  readonly DecodingServices: never;
  readonly EncodingServices: never;
}
export const isQueryCommand = (schema: Schema.Top): schema is QueryCommand =>
  Command.isContract(schema) && "contextQuery" in schema && schema.contextQuery === true;

export const QueryReply = Schema.TaggedUnion({
  Success: { value: ContextQueryResult },
  Failure: { error: ContextQueryError },
});
export const QueryInvocation = Schema.TaggedUnion({
  ContextQueryRequested: {
    input: ContextQueryInput,
    replyTo: ReplyTo<typeof QueryReply.Type>(),
    cancelled: Schema.declare<Deferred.Deferred<void>>(Deferred.isDeferred),
  },
  ContextQueryCompleted: { id: Schema.String, result: QueryReply },
});
