import { Deferred, Effect } from "effect";
import { acquireActorStoreLock, withActorStoreLock } from "../../src/storage/actor-store-lock.js";
if (process.argv[3] === "defective-shutdown") {
  await Effect.runPromise(
    Effect.gen(function* () {
      const stopped = yield* Deferred.make<void>();
      yield* withActorStoreLock(
        process.argv[2],
        Effect.gen(function* () {
          yield* Effect.addFinalizer(() => Effect.die(new Error("Writer drain failed")));
          yield* Deferred.succeed(stopped, undefined);
          return yield* Effect.never;
        }),
      ).pipe(Effect.raceFirst(Deferred.await(stopped)), Effect.exit);
    }),
  );
  process.send?.("retained");
  process.once("message", () => process.disconnect());
} else {
  try {
    const release = acquireActorStoreLock(process.argv[2]);
    process.send?.("acquired");
    process.once("message", () => {
      release();
      process.disconnect();
    });
  } catch {
    process.send?.("rejected");
    process.disconnect();
  }
}
