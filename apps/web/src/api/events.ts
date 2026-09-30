import { Data, Effect, Match, Queue, Schema, Stream } from "effect";
import { Atom, Reactivity } from "effect/unstable/reactivity";
import { QueryInvalidation, QueryKeys } from "@aster/api-contracts";
import { ApplicationClient } from "./client";

class EventProtocolError extends Data.TaggedError("EventProtocolError")<{
  readonly message: string;
}> {}
type Notification =
  | { readonly _tag: "Ready" }
  | { readonly _tag: "Disconnected" }
  | { readonly _tag: "Message"; readonly data: string };

/** EventSource is the native browser boundary. Its owning atom Scope closes it on unmount. */
const notifications = Stream.callback<Notification>((queue) =>
  Effect.acquireRelease(
    Effect.sync(() => {
      const source = new EventSource("/api/events");
      source.addEventListener("ready", () => Queue.offerUnsafe(queue, { _tag: "Ready" }));
      source.addEventListener("invalidate", (event) =>
        Queue.offerUnsafe(queue, { _tag: "Message", data: event.data }),
      );
      source.onerror = () => Queue.offerUnsafe(queue, { _tag: "Disconnected" });
      return source;
    }),
    (source) => Effect.sync(() => source.close()),
  ),
);

export const connection = ApplicationClient.runtime
  .atom(
    notifications.pipe(
      Stream.mapEffect((event) =>
        Match.value(event).pipe(
          Match.tag("Ready", () => Reactivity.invalidate([QueryKeys.all]).pipe(Effect.as(true))),
          Match.tag("Disconnected", () => Effect.succeed(false)),
          Match.tag("Message", ({ data }) =>
            Effect.gen(function* () {
              const notification = yield* Schema.decodeUnknownEffect(
                Schema.fromJsonString(QueryInvalidation),
              )(data).pipe(
                Effect.mapError(
                  () =>
                    new EventProtocolError({
                      message: "Invalid live update; refresh to reconnect",
                    }),
                ),
              );
              yield* Reactivity.invalidate(notification.keys);
              return true;
            }),
          ),
          Match.exhaustive,
        ),
      ),
    ),
  )
  .pipe(Atom.setIdleTTL(0));

/** Telemetry is transient and has no Context commit; poll only its query while the root view is mounted. */
export const telemetryRefresh = ApplicationClient.runtime
  .atom(Stream.tick("3 seconds").pipe(Stream.tap(() => Reactivity.invalidate([QueryKeys.runtime]))))
  .pipe(Atom.setIdleTTL(0));
