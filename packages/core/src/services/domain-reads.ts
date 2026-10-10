import type { ActorRef, ReplyTo } from "@aster/actor";
import { Effect, Schema } from "effect";
import { ApplicationError } from "../operations.js";
import type { CurrentActors } from "./actors.js";

/** Domain query boundary; generic Context readers never decode owner state. */
export const readActor = Effect.fn("Domain.readActor")(function* <A, C>(
  actors: CurrentActors["Service"],
  path: string,
  command: (replyTo: ReplyTo<unknown>) => C,
  schema: Schema.ConstraintDecoder<A>,
) {
  const actor = yield* actors
    .select(`/user${path}`)
    .resolve()
    .pipe(
      Effect.mapError(
        (cause) => new ApplicationError({ kind: "unavailable", message: cause.message }),
      ),
    );
  const value = yield* (actor as ActorRef<C>)
    .ask<unknown>(command)
    .pipe(
      Effect.mapError(
        (cause) => new ApplicationError({ kind: "unavailable", message: cause.message }),
      ),
    );
  return yield* Schema.decodeUnknownEffect(schema)(value).pipe(
    Effect.mapError(
      () => new ApplicationError({ kind: "unavailable", message: "Invalid domain query response" }),
    ),
  );
});
