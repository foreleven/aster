import { Cause, Deferred, Effect, Fiber, Result } from "effect";

export type AgentCallbackInvoker = <A, E>(
  effect: Effect.Effect<A, E>,
  signal?: AbortSignal,
) => Promise<A>;

/** One reasoning invocation owns its SDK callbacks, including their Context and failure path. */
export const withAgentCallbacks = <A, E, R>(
  use: (invoke: AgentCallbackInvoker) => Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R> =>
  Effect.scoped(
    Effect.gen(function* () {
      const runPromise = Effect.runPromiseWith(yield* Effect.context<never>());
      const callbacks = new AbortController();
      const callbackFailure = yield* Deferred.make<never>();
      const invoke: AgentCallbackInvoker = (effect, signal) => {
        callbacks.signal.throwIfAborted();
        return runPromise(
          effect.pipe(
            Effect.tapCause((cause) => {
              // SDK tool-error recovery must not turn a domain defect into a successful plan.
              const defect = Cause.findDefect(cause);
              return Result.isSuccess(defect)
                ? Deferred.die(callbackFailure, defect.success)
                : Effect.void;
            }),
          ),
          { signal: signal ? AbortSignal.any([callbacks.signal, signal]) : callbacks.signal },
        );
      };
      // Register the worker and its callback finalizer atomically with respect to
      // interruption; cancellation between the two registrations could deadlock idle.
      return yield* Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const worker = yield* restore(Effect.suspend(() => use(invoke))).pipe(Effect.forkScoped);
          // Release callback waits before Agent.run's finalizer waits for SDK idle.
          yield* Effect.addFinalizer(() => Effect.sync(() => callbacks.abort()));
          return yield* restore(
            Effect.raceFirst(Fiber.join(worker), Deferred.await(callbackFailure)),
          );
        }),
      );
    }),
  );
