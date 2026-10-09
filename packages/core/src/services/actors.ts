import { ReplyTo, type ActorContext } from "@aster/actor";
import { ApplicationError } from "../operations.js";
import { Context, Deferred, Effect, Schema } from "effect";

/** The current runtime's addressing capability, provided by the owning execution. */
export class CurrentActors extends Context.Service<
  CurrentActors,
  Pick<ActorContext<unknown>, "select">
>()("services/CurrentActors") {}

/** Transient query replies and cancellation are never persisted in Context state. */
export const QueryReply = Schema.TaggedUnion({
  Success: { value: Schema.Unknown },
  Failure: { error: ApplicationError },
});
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
