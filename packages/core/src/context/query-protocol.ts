import { ReplyTo } from "@aster/actor";
import { ApplicationError } from "@aster/api-contracts";
import { Deferred, Effect, Schema } from "effect";

/** Transient query replies and cancellation are never persisted in Context state. */
export const QueryReply = Schema.Union([
  Schema.TaggedStruct("Success", { value: Schema.Unknown }),
  Schema.TaggedStruct("Failure", { error: ApplicationError }),
]);
export type QueryReply = typeof QueryReply.Type;
export const queryReplyTo = ReplyTo<QueryReply>();
export const queryCancelled = Schema.declare<Deferred.Deferred<void>>(Deferred.isDeferred);

export const cancellableQuery = <A, E>(
  work: Effect.Effect<A, E>,
  cancelled: Deferred.Deferred<void>,
) =>
  Effect.raceFirst(
    work,
    Deferred.await(cancelled).pipe(
      Effect.andThen(
        Effect.fail(new ApplicationError({ kind: "unavailable", message: "Query cancelled" })),
      ),
    ),
  );
