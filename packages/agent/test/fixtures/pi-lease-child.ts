import { Deferred, Effect } from "effect";
import { PiStorageLease } from "../../src/pi-storage-lease.js";

const release = Deferred.makeUnsafe<void>();
process.on("message", () => {
  Effect.runSync(Deferred.succeed(release, undefined));
});
await Effect.runPromise(
  Effect.scoped(
    Effect.gen(function* () {
      const lease = yield* PiStorageLease.acquire(process.argv[2]!, "child");
      process.send?.({ status: "acquired", identity: lease.identity });
      yield* Deferred.await(release);
      if (process.argv[3] === "quarantine") yield* lease.quarantine;
    }),
  ).pipe(
    Effect.match({
      onSuccess: () => process.send?.({ status: "released" }),
      onFailure: () => process.send?.({ status: "rejected" }),
    }),
  ),
);
if (process.argv[3] !== "quarantine") process.disconnect();
