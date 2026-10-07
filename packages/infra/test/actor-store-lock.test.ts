import assert from "node:assert/strict";
import { fork } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, readFile, rm, stat, symlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { acquireActorStoreLock } from "../src/storage/actor-store-lock.js";

const child = (directory: string) => {
  const process = fork(new URL("./fixtures/actor-lock-child.js", import.meta.url), [directory], {
    stdio: ["ignore", "ignore", "pipe", "ipc"],
  });
  return {
    process,
    reply: once(process, "message").then(([message]) => message),
    exit: once(process, "exit"),
  };
};

test("root lock excludes aliases, survives owner death and permits only one recovery writer", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "aster-root-lock-"));
  const alias = `${directory}-alias`;
  await symlink(directory, alias);
  t.after(() => rm(directory, { recursive: true, force: true }));
  t.after(() => rm(alias, { force: true }));
  const first = child(directory);
  t.after(() => first.process.kill("SIGKILL"));
  assert.equal(await first.reply, "acquired");
  const inode = (await stat(join(directory, ".actors-lock.sqlite"))).ino;
  assert.throws(() => acquireActorStoreLock(alias), /Cannot acquire actor store/);
  first.process.kill("SIGKILL");
  await first.exit;
  const replacements = [child(directory), child(alias)];
  for (const next of replacements) t.after(() => next.process.kill("SIGKILL"));
  const replies = await Promise.all(replacements.map((next) => next.reply));
  assert.ok(replies.filter((reply) => reply === "acquired").length <= 1);
  for (const [index, next] of replacements.entries()) {
    if (replies[index] === "acquired") {
      assert.equal(await readFile(join(directory, "actors.pid"), "utf8"), String(next.process.pid));
      assert.throws(() => acquireActorStoreLock(directory), /Cannot acquire actor store/);
      next.process.send("release");
    }
    await next.exit;
  }
  const release = acquireActorStoreLock(alias);
  assert.equal((await stat(join(directory, ".actors-lock.sqlite"))).ino, inode);
  release();
  // A stale disposer cannot release a replacement owner in this same process.
  const replacement = acquireActorStoreLock(directory);
  release();
  assert.throws(() => acquireActorStoreLock(alias), /Cannot acquire actor store/);
  replacement();
});
