import { Context, Effect, Fiber, Layer, Option, Scope, Semaphore } from "effect";
import { MemoryBackend, MemoryRecallError, type MemoryRecallOperations } from "./contracts.js";

/** Read workers belong to the runtime Scope, independently of durable capture admission. */
export class MemoryRecall extends Context.Service<MemoryRecall, MemoryRecallOperations>()(
  "memory/Recall",
) {
  static readonly layer = Layer.effect(
    MemoryRecall,
    Effect.gen(function* () {
      const scope = yield* Scope.Scope;
      const backend = yield* MemoryBackend;
      const slots = yield* Semaphore.make(4);
      const run = <A>(work: Effect.Effect<A, MemoryRecallError>) =>
        Effect.acquireUseRelease(
          work.pipe(slots.withPermitsIfAvailable(1), Effect.forkIn(scope)),
          (fiber) =>
            Fiber.join(fiber).pipe(
              Effect.flatMap((result) =>
                Option.isSome(result)
                  ? Effect.succeed(result.value)
                  : Effect.fail(
                      new MemoryRecallError({ message: "Memory recall capacity reached" }),
                    ),
              ),
            ),
          Fiber.interrupt,
        );
      return MemoryRecall.of({
        search: (query) => run(backend.recall.search(query)),
        expand: (items) => run(backend.recall.expand(items)),
      });
    }),
  );
}
