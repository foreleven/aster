import assert from "node:assert/strict";
import { fork } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, readFile, realpath, rm, stat, symlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { Effect, Schema } from "effect";
import { PiStorageLease } from "../src/pi-storage-lease.js";

const Reply = Schema.Struct({ status: Schema.Literals(["acquired", "released", "rejected"]) });
const child = (directory: string, quarantine = false) => {
  const process = fork(
    new URL("./fixtures/pi-lease-child.js", import.meta.url),
    [directory, ...(quarantine ? ["quarantine"] : [])],
    { stdio: ["ignore", "ignore", "pipe", "ipc"] },
  );
  const reply = once(process, "message").then(([message]) =>
    Schema.decodeUnknownSync(Reply)(message),
  );
  const exit = once(process, "exit");
  return { process, reply, exit };
};

test("Pi lease excludes aliases and concurrent owners, and fences handles after Scope close", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "aster-lease-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const canonical = await realpath(root);
  const alias = `${root}-alias`;
  await symlink(root, alias);
  t.after(() => rm(alias, { force: true }));
  const lease = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const lease = yield* PiStorageLease.acquire(root, "goal:first");
        yield* lease.assertHeld;
        const conflict = yield* PiStorageLease.acquire(alias, "personal:second").pipe(Effect.flip);
        assert.equal(conflict._tag, "PiStorageLeaseError");
        yield* lease.assertHeld;
        return lease;
      }),
    ),
  );
  assert.equal(
    (await Effect.runPromise(PiStorageLease.inspect)).some(
      (owner) => owner.leaseId === lease.identity.token,
    ),
    false,
  );
  assert.equal(
    (await Effect.runPromise(lease.assertHeld.pipe(Effect.flip)))._tag,
    "PiStorageLeaseError",
  );
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const replacement = yield* PiStorageLease.acquire(alias, "goal:next");
        assert.notEqual(replacement.identity.token, lease.identity.token);
        assert.equal(replacement.identity.directory, canonical);
      }),
    ),
  );
});

test("Pi kernel lease excludes concurrent replacements and recovers after process death", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "aster-lease-process-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const first = child(directory);
  t.after(() => first.process.kill("SIGKILL"));
  assert.equal((await first.reply).status, "acquired");
  const inode = (await stat(join(directory, ".aster-owner.sqlite"))).ino;
  const contender = child(directory);
  assert.equal((await contender.reply).status, "rejected");
  await contender.exit;
  first.process.kill("SIGKILL");
  await first.exit;
  const replacements = [child(directory), child(directory)];
  for (const next of replacements) t.after(() => next.process.kill("SIGKILL"));
  const replies = await Promise.all(replacements.map((next) => next.reply));
  const acquired = replies.flatMap((reply, index) => (reply.status === "acquired" ? [index] : []));
  assert.ok(acquired.length <= 1, "At most one concurrent owner may acquire the lease");
  assert.ok(replies.every((reply) => ["acquired", "rejected"].includes(reply.status)));
  // SQLite timeout=0 is a try-lock. Two schema readers can both lose a lock
  // upgrade; this is safe rejection, not a guarantee that either caller wins.
  // Once those contenders exit, a fresh owner must acquire without stale locks.
  let winner: ReturnType<typeof child>;
  if (acquired.length === 0) {
    await Promise.all(replacements.map((next) => next.exit));
    winner = child(directory);
    t.after(() => winner.process.kill("SIGKILL"));
    assert.equal((await winner.reply).status, "acquired");
  } else {
    winner = replacements[acquired[0]!]!;
  }
  const excluded = child(directory);
  assert.equal((await excluded.reply).status, "rejected");
  await excluded.exit;
  const metadata = JSON.parse(await readFile(join(directory, ".aster-owner.json"), "utf8"));
  assert.equal(metadata.pid, winner.process.pid);
  assert.equal((await stat(join(directory, ".aster-owner.sqlite"))).ino, inode);
  const released = once(winner.process, "message");
  winner.process.send("release");
  assert.equal(Schema.decodeUnknownSync(Reply)((await released)[0]).status, "released");
  await Promise.all([...replacements.map((next) => next.exit), winner.exit]);
  await Effect.runPromise(Effect.scoped(PiStorageLease.acquire(directory, "parent")));
});

test("Uncertain writer shutdown retains the kernel lease until process exit", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "aster-lease-quarantine-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const owner = child(directory, true);
  t.after(() => owner.process.kill("SIGKILL"));
  assert.equal((await owner.reply).status, "acquired");
  const closed = once(owner.process, "message");
  owner.process.send("release");
  assert.equal(Schema.decodeUnknownSync(Reply)((await closed)[0]).status, "released");
  const conflict = await Effect.runPromise(
    Effect.scoped(PiStorageLease.acquire(directory, "replacement").pipe(Effect.flip)),
  );
  assert.equal(conflict._tag, "PiStorageLeaseError");
  owner.process.kill("SIGKILL");
  await owner.exit;
  await Effect.runPromise(Effect.scoped(PiStorageLease.acquire(directory, "replacement")));
});
