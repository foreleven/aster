import { randomUUID } from "node:crypto";
import { Data, Duration, Effect, Option, Queue, type Scope } from "effect";
import type { ActorRef } from "./actor.js";
import { ActorRefImpl } from "./internal/ref.js";

export class UnexpectedMessageError extends Data.TaggedError("UnexpectedMessageError")<{
  readonly path: string;
}> {
  override get message() {
    return `Unexpected message at ${this.path}`;
  }
}

export interface ActorTestProbe<Command> {
  readonly ref: ActorRef<Command>;
  take(): Effect.Effect<Command>;
  expectNoMessage(duration: Duration.Input): Effect.Effect<void, UnexpectedMessageError>;
}

export const ActorTestKit = {
  probe: <Command>(): Effect.Effect<ActorTestProbe<Command>, never, Scope.Scope> =>
    Effect.acquireRelease(Queue.unbounded<Command>(), (queue) =>
      Queue.shutdown(queue).pipe(Effect.asVoid),
    ).pipe(
      Effect.map((queue) => {
        const path = `/system/test/${randomUUID()}`;
        const incarnation = randomUUID();
        // Probe refs use the same one-shot ask/cancellation path as production.
        // Probes have no system event bus, so discarded replies need no publication.
        const ref = new ActorRefImpl<Command>(
          path,
          incarnation,
          (command) => Queue.offer(queue, command).pipe(Effect.asVoid),
          { deadLetter: () => Effect.void },
        );
        return {
          ref,
          take: () => Queue.take(queue),
          expectNoMessage: (duration: Duration.Input) =>
            Effect.timeoutOption(Queue.take(queue), duration).pipe(
              Effect.flatMap((result) =>
                Option.isNone(result)
                  ? Effect.void
                  : Effect.fail(new UnexpectedMessageError({ path })),
              ),
            ),
        };
      }),
    ),
} as const;
