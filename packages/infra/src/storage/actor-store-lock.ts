import { Cause, Effect, Exit, type Scope } from "effect";
import { mkdirSync, realpathSync, unlinkSync, writeFileSync } from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";

/** SQLite supplies a local-filesystem kernel lock unavailable in Node/Effect's
 * filesystem API. Never unlink the database: its stable inode is the lock identity.
 * actors.pid is diagnostic only; process death releases ownership automatically. */
export const acquireActorStoreLock = (root = join(homedir(), ".aster")) => {
  mkdirSync(root, { recursive: true, mode: 0o700 });
  const directory = realpathSync(root);
  const database = new DatabaseSync(join(directory, ".actors-lock.sqlite"), { timeout: 0 });
  const marker = join(directory, "actors.pid");
  try {
    database.exec(
      "PRAGMA journal_mode = DELETE; CREATE TABLE IF NOT EXISTS lease (id INTEGER PRIMARY KEY); BEGIN EXCLUSIVE;",
    );
    writeFileSync(marker, String(process.pid), { mode: 0o600 });
  } catch (cause) {
    database.close();
    throw new Error("Cannot acquire actor store: another process may own it", { cause });
  }
  return () => {
    if (!database.isOpen) return;
    try {
      unlinkSync(marker);
    } finally {
      database.close();
    }
  };
};

// Keep failed owners reachable: garbage collection must not release a native
// lock while a defective runtime may still have live writers.
const retainedLocks = new Set<() => void>();

/** The runtime and all its finalizers finish before its host releases ownership. */
export const withActorStoreLock = <A, E, R>(
  root: string,
  use: Effect.Effect<A, E, R | Scope.Scope>,
) =>
  Effect.acquireUseRelease(
    Effect.try(() => acquireActorStoreLock(root)),
    () => Effect.scoped(use),
    (release, exit) =>
      Effect.sync(() => {
        if (Exit.isFailure(exit) && Cause.hasDies(exit.cause)) retainedLocks.add(release);
        else release();
      }),
  );
