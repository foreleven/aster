import {
  closeSync,
  existsSync,
  mkdirSync,
  openSync,
  readFileSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";

/** The actor directory has one writer, independently of which memory configuration is selected. */
export const acquireActorStoreLock = (root = join(homedir(), ".aster")) => {
  mkdirSync(root, { recursive: true, mode: 0o700 });
  const path = join(root, "actors.pid");
  if (existsSync(path)) {
    const pid = Number(readFileSync(path, "utf8").trim());
    if (!Number.isSafeInteger(pid) || pid <= 0)
      throw new Error(`Invalid actor-store lock: ${path}`);
    let alive = true;
    try {
      process.kill(pid, 0);
    } catch (error) {
      alive = (error as NodeJS.ErrnoException).code !== "ESRCH";
    }
    if (alive) throw new Error(`Another Aster process (${pid}) owns the actor store`);
    // Do not reclaim a different lock installed while we checked the former owner.
    if (Number(readFileSync(path, "utf8").trim()) === pid) unlinkSync(path);
  }
  const fd = openSync(path, "wx", 0o600);
  try {
    writeFileSync(fd, String(process.pid));
  } finally {
    closeSync(fd);
  }
  return () => {
    if (existsSync(path) && Number(readFileSync(path, "utf8").trim()) === process.pid)
      unlinkSync(path);
  };
};
