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
