import { randomUUID } from "node:crypto";
import { Deferred, Effect, Exit, Option, Scope, type Duration } from "effect";
import {
  AskTimeoutError,
  type ActorRef,
  type ActorStartupError,
  type FailureSummary,
  type AskOptions,
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
    readonly scope?: Scope.Scope,
  ) {}

  tell(command: Command): Effect.Effect<void> {
    return this.send(command);
  }

  ask<Response>(
    makeCommand: (replyTo: ActorRef<Response>) => Command,
    options?: Duration.Input | AskOptions,
  ): Effect.Effect<Response, AskTimeoutError> {
    return Effect.uninterruptibleMask((restore) =>
      Effect.gen({ self: this }, function* () {
        // An explicit empty object remains a zero Duration; only option fields opt in.
        const settings =
          typeof options === "object" &&
          options !== null &&
          ("timeout" in options || "scope" in options)
            ? (options as AskOptions)
            : { timeout: options as Duration.Input | undefined };
        const inherited = yield* Effect.serviceOption(Scope.Scope);
        const parent = settings.scope ?? Option.getOrUndefined(inherited);
        const scope = parent ? yield* Scope.fork(parent) : yield* Scope.make();
        const deferred = yield* Deferred.make<Response>();
        yield* Scope.addFinalizer(scope, Deferred.interrupt(deferred));
        const path = `/system/ask/${randomUUID()}`;
        const incarnation = randomUUID();
        const reply = new ActorRefImpl<Response>(
          path,
          incarnation,
          (value) =>
            Effect.gen({ self: this }, function* () {
              if (!(yield* Deferred.succeed(deferred, value))) {
                yield* this.system.deadLetter(path, incarnation, value, "ask closed");
                return;
              }
            }),
          this.system,
          Effect.void,
          scope,
        );
        return yield* restore(
          Effect.raceFirst(
            Effect.suspend(() =>
              scope.state._tag === "Closed"
                ? Deferred.await(deferred)
                : this.tell(makeCommand(reply)).pipe(
                    Effect.flatMap(() => Deferred.await(deferred)),
                  ),
            ),
            Effect.sleep(settings.timeout ?? "30 seconds").pipe(
              Effect.flatMap(() => Effect.fail(new AskTimeoutError(this.path))),
            ),
          ),
        ).pipe(
          Effect.ensuring(
            Deferred.interrupt(deferred).pipe(Effect.andThen(Scope.close(scope, Exit.void))),
          ),
        );
      }),
    );
  }
}
