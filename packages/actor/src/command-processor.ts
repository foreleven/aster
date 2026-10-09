import { Effect, Fiber, Queue, type Scope } from "effect";
import type { ActorContext, ReplyTo } from "./actor.js";
import { reply, type Outcome } from "./command.js";

/** One Behavior-owned FIFO; only admitted work allocates a request fiber. */
export const CommandProcessor = {
  make: Effect.fn("CommandProcessor.make")(function* (options: { readonly concurrency: number }) {
    if (!Number.isSafeInteger(options.concurrency) || options.concurrency < 1)
      return yield* Effect.die(new Error("Command concurrency must be a positive integer"));
    const scope = yield* Effect.scope;
    const requests = yield* Effect.acquireRelease(
      Queue.unbounded<Effect.Effect<void>>(),
      Queue.shutdown,
    );
    const worker = Queue.take(requests).pipe(
      Effect.flatMap((run) => run),
      Effect.forever,
    );
    for (let index = 0; index < options.concurrency; index++) yield* Effect.forkIn(worker, scope);
    return {
      submit: <A, E, R, M>(
        command: { readonly replyTo: ReplyTo<Outcome<A, E>> },
        actor: ActorContext<M>,
        work: Effect.Effect<A, E, R>,
      ): Effect.Effect<void, never, Exclude<R, Scope.Scope>> =>
        Effect.gen(function* () {
          const requestScope = command.replyTo.scope;
          if (requestScope?.state._tag === "Closed") return;
          const environment = yield* Effect.context<Exclude<R, Scope.Scope>>();
          const run = Effect.gen(function* () {
            if (requestScope?.state._tag === "Closed") return;
            const fiber = yield* actor.fork(
              reply(command.replyTo, Effect.scoped(work)).pipe(Effect.provideContext(environment)),
              requestScope,
            );
            yield* Fiber.await(fiber);
          });
          yield* Queue.offer(requests, run);
        }),
    };
  }),
};
