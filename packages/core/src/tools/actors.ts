import { type ActorRef, type ReplyTo } from "@aster/actor";
import { ApplicationError } from "@aster/api-contracts";
import { Deferred, Effect } from "effect";
import type { QueryReply } from "../commands/query.js";
import { CurrentActors } from "../services/actors.js";

/** Schema-owned local command types are restored only at the dynamic addressing boundary. */
export const ask = Effect.fn("Tools.ask")(function* <C, A>(
  path: string,
  command: (replyTo: ReplyTo<A>) => C,
) {
  const actors = yield* CurrentActors;
  const ref = yield* actors
    .select(path)
    .resolve()
    .pipe(
      Effect.mapError(
        () => new ApplicationError({ kind: "unavailable", message: `Actor unavailable: ${path}` }),
      ),
    );
  return yield* (ref as ActorRef<C>).ask(command, "3 minutes").pipe(
    Effect.mapError(
      () =>
        new ApplicationError({
          kind: "unavailable",
          message: "Acknowledgement missing; retain the original operation identity",
        }),
    ),
  );
});

export const askQuery = <C>(
  path: string,
  command: (replyTo: ReplyTo<QueryReply>, cancelled: Deferred.Deferred<void>) => C,
) =>
  Effect.acquireUseRelease(
    Deferred.make<void>(),
    (cancelled) =>
      ask(path, (replyTo: ReplyTo<QueryReply>) => command(replyTo, cancelled)).pipe(
        Effect.flatMap((reply) =>
          reply._tag === "Success" ? Effect.succeed(reply.value) : Effect.fail(reply.error),
        ),
      ),
    (cancelled) => Deferred.succeed(cancelled, undefined),
  );
