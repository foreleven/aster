import { type ActorRef, type ReplyTo } from "@aster/actor";
import { Effect } from "effect";
import { ApplicationError, operationUncertain } from "../operations.js";
import { CurrentActors } from "../services/actors.js";

const resolveOwner = Effect.fn("Domain.resolveOwner")(function* <C>(path: string) {
  const actors = yield* CurrentActors;
  const ref = yield* actors
    .select(path)
    .resolve()
    .pipe(
      Effect.mapError(
        () => new ApplicationError({ kind: "unavailable", message: `Owner unavailable: ${path}` }),
      ),
    );
  // Domain operations supply the closed command contract at the dynamic addressing boundary.
  return ref as ActorRef<C>;
});

export const readOwner = Effect.fn("Domain.readOwner")(function* <C, A>(
  path: string,
  command: (replyTo: ReplyTo<A>) => C,
) {
  const ref = yield* resolveOwner<C>(path);
  return yield* ref
    .ask(command, "3 minutes")
    .pipe(
      Effect.mapError(
        () => new ApplicationError({ kind: "unavailable", message: "Owner query unavailable" }),
      ),
    );
});

export const askOwner = Effect.fn("Domain.askOwner")(function* <C, A>(
  path: string,
  command: (replyTo: ReplyTo<A>) => C,
  operationId: string,
) {
  const ref = yield* resolveOwner<C>(path);
  return yield* ref.ask(command, "3 minutes").pipe(
    Effect.mapError(() =>
      operationUncertain({
        requestId: operationId,
        message: "Acknowledgement missing; reconcile the original operation",
      }),
    ),
  );
});
