import { Cause, Deferred, Effect, Exit, Fiber, FiberSet, Ref, Result, Scope } from "effect";
import { AgentError, ToolInvocationFailure } from "./contracts.js";

export interface AgentCallbackInvoker<R> {
  <A, E>(effect: Effect.Effect<A, E, R>, signal?: AbortSignal): Promise<A>;
  readonly signal: AbortSignal;
  readonly registerTerminalHandler: (
    handler: (failure: ToolInvocationFailure) => Effect.Effect<void, AgentError>,
  ) => Effect.Effect<void>;
}

const retentionCause = (
  failure: ToolInvocationFailure,
  saved: Exit.Exit<void, AgentError>,
  initial: Cause.Cause<AgentError>,
): Cause.Cause<AgentError> => {
  if (Exit.isSuccess(saved)) return initial;
  if (Result.isSuccess(Cause.findError(saved.cause)))
    return Cause.fail(
      new AgentError(failure.message, [], {
        cause: saved.cause,
        outcome: "unknown",
        operationId: failure.operationId,
      }),
    );
  return saved.cause;
};

/** One reasoning invocation owns its SDK callbacks, including their Context and failure path. */
export const withAgentCallbacks = <A, E, R>(
  use: (invoke: AgentCallbackInvoker<R>) => Effect.Effect<A, E, R>,
): Effect.Effect<A, E | AgentError, R> =>
  Effect.scoped(
    Effect.flatMap(Scope.Scope, (workerScope) =>
      // Close callback fibers before interrupting the SDK worker: its cleanup may await them.
      Effect.scoped(
        Effect.gen(function* () {
          const runPromise = yield* FiberSet.makeRuntimePromise<R>();
          const callbackFailure = yield* Deferred.make<never, AgentError>();
          const terminal = yield* Ref.make<Cause.Cause<AgentError> | undefined>(undefined);
          const terminalHandler = yield* Ref.make<
            (failure: ToolInvocationFailure) => Effect.Effect<void, AgentError>
          >(() => Effect.void);
          const controller = new AbortController();
          yield* Effect.addFinalizer(() => Effect.sync(() => controller.abort()));
          const freeze = (cause: Cause.Cause<AgentError>) =>
            Ref.modify(terminal, (current) => [current === undefined, current ?? cause]);
          const publish = (cause: Cause.Cause<AgentError>) =>
            Deferred.failCause(callbackFailure, cause).pipe(
              Effect.andThen(Effect.sync(() => controller.abort())),
              Effect.uninterruptible,
            );
          const latch = (cause: Cause.Cause<AgentError>) =>
            freeze(cause).pipe(Effect.flatMap((first) => (first ? publish(cause) : Effect.void)));
          const runCallback = <A, E>(effect: Effect.Effect<A, E, R>, signal?: AbortSignal) =>
            runPromise(
              Effect.gen(function* () {
                const failure = yield* Ref.get(terminal);
                if (failure) return yield* Effect.failCause(failure);
                return yield* effect;
              }).pipe(
                Effect.tapCause((cause) => {
                  const failure = Cause.findError(cause);
                  if (
                    Result.isSuccess(failure) &&
                    failure.success instanceof ToolInvocationFailure
                  ) {
                    const error = failure.success;
                    return Effect.gen(function* () {
                      const initial = Cause.fail(
                        new AgentError(error.message, [], {
                          cause: error,
                          outcome: error.outcome,
                          operationId: error.operationId,
                        }),
                      );
                      // Freeze new callbacks before persistence can yield to another invocation.
                      if (!(yield* freeze(initial))) return;
                      // Retain native provenance before aborting the SDK or exposing tool failure.
                      const record = yield* Ref.get(terminalHandler);
                      const saved = yield* Effect.exit(record(error));
                      const final = retentionCause(error, saved, initial);
                      yield* Ref.set(terminal, final);
                      return yield* publish(final);
                    }).pipe(Effect.uninterruptible);
                  }
                  // Pi may recover ordinary typed tool errors, but not uncertainty or defects.
                  const defect = Cause.findDefect(cause);
                  return Result.isSuccess(defect) ? latch(Cause.die(defect.success)) : Effect.void;
                }),
              ),
              { signal },
            );
          const invoke: AgentCallbackInvoker<R> = Object.assign(runCallback, {
            signal: controller.signal,
            registerTerminalHandler: (
              handler: (failure: ToolInvocationFailure) => Effect.Effect<void, AgentError>,
            ) => Ref.set(terminalHandler, handler),
          });
          const worker = yield* Effect.suspend(() => use(invoke)).pipe(Effect.forkIn(workerScope));
          return yield* Effect.raceFirst(Fiber.join(worker), Deferred.await(callbackFailure));
        }),
      ),
    ),
  );
