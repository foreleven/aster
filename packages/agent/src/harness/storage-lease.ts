import { createHash, randomUUID } from "node:crypto";
import { mkdirSync, realpathSync, writeFileSync, renameSync, unlinkSync } from "node:fs";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { Data, Effect, Ref } from "effect";

export class PiStorageLeaseError extends Data.TaggedError("PiStorageLeaseError")<{
  readonly directory: string;
  readonly operation: "acquire" | "release";
  readonly cause: unknown;
}> {}

/** Retain a kernel lock when SDK shutdown cannot prove that its writer drained.
 * Releasing it would admit a second writer. Only process exit clears quarantine. */
const quarantined = new Map<string, DatabaseSync>();
type OwnerView = {
  readonly ownerId: string;
  readonly leaseId: string;
  readonly storageId: string;
  readonly pid: number;
  readonly status: "held" | "quarantined";
};
const owners = new Map<string, OwnerView>();

/** SQLite supplies the OS lock missing from Node/Effect FileSystem APIs. The
 * lock file is permanent: never unlink it, including after a crash. These short
 * synchronous native calls are confined to acquisition/release, with timeout 0.
 * This protocol is for local filesystems, not distributed/network storage. */
const acquire = Effect.fn("PiStorageLease.acquire")(function* (directory: string, ownerId: string) {
  const quarantine = yield* Ref.make(false);
  const resource = yield* Effect.acquireRelease(
    Effect.try({
      try: () => {
        mkdirSync(directory, { recursive: true, mode: 0o700 });
        const canonical = realpathSync(directory);
        const database = new DatabaseSync(join(canonical, ".aster-owner.sqlite"), { timeout: 0 });
        try {
          // A real table establishes the database header before acquiring the
          // long-lived exclusive transaction. SQLite recovers its own journal.
          database.exec(
            "PRAGMA journal_mode = DELETE; CREATE TABLE IF NOT EXISTS lease (id INTEGER PRIMARY KEY); BEGIN EXCLUSIVE;",
          );
          const identity = { directory: canonical, ownerId, token: randomUUID(), pid: process.pid };
          const metadata = join(canonical, ".aster-owner.json");
          const pending = `${metadata}.${identity.token}`;
          writeFileSync(pending, JSON.stringify(identity), { mode: 0o600 });
          renameSync(pending, metadata);
          owners.set(identity.token, {
            ownerId,
            leaseId: identity.token,
            storageId: createHash("sha256").update(canonical).digest("hex"),
            pid: process.pid,
            status: "held",
          });
          return { database, identity, metadata };
        } catch (cause) {
          database.close();
          throw cause;
        }
      },
      catch: (cause) => new PiStorageLeaseError({ directory, operation: "acquire", cause }),
    }),
    (resource) =>
      Effect.gen(function* () {
        if (yield* Ref.get(quarantine)) {
          quarantined.set(resource.identity.directory, resource.database);
          return;
        }
        const close = Effect.try({
          try: () => {
            resource.database.close();
            owners.delete(resource.identity.token);
          },
          catch: (cause) => new PiStorageLeaseError({ directory, operation: "release", cause }),
        }).pipe(
          Effect.tapError(() =>
            Effect.sync(() => {
              quarantined.set(resource.identity.directory, resource.database);
              const owner = owners.get(resource.identity.token);
              if (owner) owners.set(resource.identity.token, { ...owner, status: "quarantined" });
            }),
          ),
          Effect.orDie,
        );
        // Remove diagnostics while holding the lock. A failed diagnostic cleanup
        // must still close the native connection; preserve both failures as defects.
        yield* Effect.try({
          try: () => unlinkSync(resource.metadata),
          catch: (cause) => new PiStorageLeaseError({ directory, operation: "release", cause }),
        }).pipe(Effect.ensuring(close), Effect.orDie);
      }),
  );
  const held = yield* Ref.make(true);
  yield* Effect.addFinalizer(() => Ref.set(held, false));
  return {
    identity: resource.identity,
    assertHeld: Effect.all([Ref.get(held), Ref.get(quarantine)]).pipe(
      Effect.flatMap(([value, retained]) =>
        value && !retained && resource.database.isTransaction
          ? Effect.void
          : Effect.fail(
              new PiStorageLeaseError({
                directory,
                operation: "acquire",
                cause: "Storage owner is closed or quarantined",
              }),
            ),
      ),
    ),
    quarantine: Ref.set(quarantine, true).pipe(
      Effect.andThen(
        Effect.sync(() => {
          const owner = owners.get(resource.identity.token);
          if (owner) owners.set(resource.identity.token, { ...owner, status: "quarantined" });
        }),
      ),
    ),
  };
});

export const PiStorageLease = {
  acquire,
  inspect: Effect.sync(() => [...owners.values()].map((owner) => ({ ...owner }))),
};
