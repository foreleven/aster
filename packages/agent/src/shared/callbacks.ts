import { Cause, Deferred, Effect, Fiber, FiberSet, Result, Scope } from "effect";

export type AgentCallbackInvoker<R> = <A, E>(
  effect: Effect.Effect<A, E, R>,
  signal?: AbortSignal,
) => Promise<A>;

/** One reasoning invocation owns its SDK callbacks, including their Context and failure path. */
export const withAgentCallbacks = <A, E, R>(
  use: (invoke: AgentCallbackInvoker<R>) => Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R> =>
  Effect.scoped(
    Effect.flatMap(Scope.Scope, (workerScope) =>
      // Close callback fibers before interrupting the SDK worker: its cleanup may await them.
      Effect.scoped(
        Effect.gen(function* () {
          const runPromise = yield* FiberSet.makeRuntimePromise<R>();
          const callbackFailure = yield* Deferred.make<never>();
          const invoke: AgentCallbackInvoker<R> = (effect, signal) =>
            runPromise(
              effect.pipe(
                Effect.tapCause((cause) => {
                  // Pi may recover typed tool errors, but must not swallow Effect defects.
                  const defect = Cause.findDefect(cause);
                  return Result.isSuccess(defect)
                    ? Deferred.die(callbackFailure, defect.success)
                    : Effect.void;
                }),
              ),
              { signal },
            );
          const worker = yield* Effect.suspend(() => use(invoke)).pipe(Effect.forkIn(workerScope));
          return yield* Effect.raceFirst(Fiber.join(worker), Deferred.await(callbackFailure));
        }),
      ),
    ),
  );
