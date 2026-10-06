import { randomUUID } from "node:crypto";
import { Deferred, Effect, type Duration } from "effect";
import {
  AskTimeoutError,
  type ActorRef,
  type ActorStartupError,
  type FailureSummary,
} from "../actor.js";

/** Only the delivery/diagnostic capability is shared with temporary reply references. */
export interface RefRuntime {
  readonly deadLetter: (
    path: string,
    incarnation: string,
    command: unknown,
    reason: string,
  ) => Effect.Effect<void>;
}

export class ActorRefImpl<Command> implements ActorRef<Command> {
  terminated = false;
  terminalCause: FailureSummary | undefined;
  constructor(
    readonly path: string,
    readonly incarnation: string,
    private readonly send: (command: Command) => Effect.Effect<void>,
    private readonly system: RefRuntime,
    // Temporary reply references and test probes are immediately available.
    readonly awaitStarted: Effect.Effect<void, ActorStartupError> = Effect.void,
  ) {}

  tell(command: Command): Effect.Effect<void> {
    return this.send(command);
  }

  ask<Response>(
    makeCommand: (replyTo: ActorRef<Response>) => Command,
    timeout: Duration.Input = "30 seconds",
  ): Effect.Effect<Response, AskTimeoutError> {
    return Effect.gen({ self: this }, function* () {
      const deferred = yield* Deferred.make<Response>();
      const path = `/system/ask/${randomUUID()}`;
      const incarnation = randomUUID();
      let open = true;
      const reply = new ActorRefImpl<Response>(
        path,
        incarnation,
        (value) =>
          Effect.gen({ self: this }, function* () {
            if (!open) {
              yield* this.system.deadLetter(path, incarnation, value, "ask closed");
              return;
            }
            open = false;
            yield* Deferred.succeed(deferred, value);
          }),
        this.system,
      );
      return yield* Effect.raceFirst(
        this.tell(makeCommand(reply)).pipe(Effect.flatMap(() => Deferred.await(deferred))),
        Effect.sleep(timeout).pipe(
          Effect.flatMap(() => Effect.fail(new AskTimeoutError(this.path))),
        ),
      ).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            open = false;
          }),
        ),
      );
    });
  }
}
