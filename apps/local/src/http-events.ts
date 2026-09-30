import { Data, Effect, Queue, Stream } from "effect";
import { HttpServerResponse } from "effect/unstable/http";
import type { ApplicationApi } from "@aster/core";

class SlowEventClient extends Data.TaggedError("SlowEventClient") {}

/** Register before ready. Slow clients reconnect and re-query; notifications are not an audit log. */
export const eventResponse = (application: ApplicationApi) =>
  Effect.gen(function* () {
    const changes = yield* application.subscribeInvalidations;
    const frames = yield* Queue.dropping<string, SlowEventClient>(64);
    yield* Stream.runForEach(changes, (change) =>
      Effect.gen(function* () {
        const accepted = yield* Queue.offer(
          frames,
          `event: invalidate\ndata: ${JSON.stringify(change)}\n\n`,
        );
        if (!accepted) return yield* new SlowEventClient();
      }),
    ).pipe(
      Effect.catchTag("SlowEventClient", (error) => Queue.fail(frames, error)),
      Effect.forkScoped,
    );
    const heartbeats = Stream.tick("15 seconds").pipe(Stream.map(() => ": heartbeat\n\n"));
    const body = Stream.make("event: ready\ndata: {}\n\n").pipe(
      Stream.concat(Stream.fromQueue(frames).pipe(Stream.merge(heartbeats))),
      Stream.encodeText,
    );
    return HttpServerResponse.stream(body, {
      contentType: "text/event-stream",
      headers: { "cache-control": "no-cache", "x-accel-buffering": "no" },
    });
  });
