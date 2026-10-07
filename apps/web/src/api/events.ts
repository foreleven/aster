import { Duration, Effect, Queue, Schedule, Stream } from "effect";
import { Atom, Reactivity } from "effect/reactivity";
import { QueryKeys } from "@aster/api";
import { ApplicationError } from "@aster/core/contracts";
import type { RpcClientError } from "effect/rpc/RpcClientError";
import { ApplicationClient } from "./client";

const recoverable = (error: ApplicationError | RpcClientError) => {
  if (error._tag === "ApplicationError") return error.kind === "unavailable";
  return (
    error.reason._tag !== "RpcClientDefect" ||
    error.reason.message === "HTTP response ended before RPC request completed"
  );
};

/** One read-only subscription in the same runtime as queries; mutations never use this retry policy. */
export const connection = ApplicationClient.runtime
  .atom(
    Stream.callback<
      boolean,
      ApplicationError | RpcClientError,
      ApplicationClient | Reactivity.Reactivity
    >(
      (queue) =>
        Effect.gen(function* () {
          const client = yield* ApplicationClient;
          return yield* client("SubscribeInvalidations", undefined).pipe(
            Stream.runForEach(({ keys }) =>
              Reactivity.invalidate(keys).pipe(Effect.andThen(Queue.offer(queue, true))),
            ),
            Effect.andThen(
              Effect.fail(
                new ApplicationError({
                  kind: "unavailable",
                  message: "Subscription ended; reconnecting",
                }),
              ),
            ),
            Effect.tapError(() => Queue.offer(queue, false)),
            Effect.retry({
              while: recoverable,
              schedule: Schedule.exponential("1 second").pipe(
                Schedule.modifyDelay(({ output }) =>
                  Effect.succeed(Duration.min(output, Duration.seconds(30))),
                ),
              ),
            }),
          );
        }).pipe(
          // Stream.callback owns a queue: forward the complete failure cause to its consumer.
          Effect.onExit((exit) =>
            exit._tag === "Failure" ? Queue.failCause(queue, exit.cause) : Queue.end(queue),
          ),
        ),
      { bufferSize: 1, strategy: "sliding" },
    ),
  )
  .pipe(Atom.setIdleTTL(0));

/** Telemetry has no Context commit; refresh only its query while mounted. */
export const telemetryRefresh = ApplicationClient.runtime
  .atom(Stream.tick("3 seconds").pipe(Stream.tap(() => Reactivity.invalidate([QueryKeys.runtime]))))
  .pipe(Atom.setIdleTTL(0));
