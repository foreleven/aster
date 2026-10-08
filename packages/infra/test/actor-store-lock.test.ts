import assert from "node:assert/strict";
import { fork } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, readFile, rm, stat, symlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { Data, Deferred, Effect, Exit, Fiber } from "effect";
import { acquireActorStoreLock, withActorStoreLock } from "../src/storage/actor-store-lock.js";

const child = (directory: string, mode = "normal") => {
  const process = fork(
    new URL("./fixtures/actor-lock-child.js", import.meta.url),
    [directory, mode],
    {
      stdio: ["ignore", "ignore", "pipe", "ipc"],
    },
  );
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

class TestFailure extends Data.TaggedError("TestFailure") {}

for (const outcome of ["success", "failure", "interruption"] as const) {
  test(`root lock stays held through finalizer drain on ${outcome}`, async (t) => {
    const directory = await mkdtemp(join(tmpdir(), "aster-root-drain-"));
    t.after(() => rm(directory, { recursive: true, force: true }));
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const started = yield* Deferred.make<void>();
          const closing = yield* Deferred.make<void>();
          const drained = yield* Deferred.make<void>();
          const use = Effect.gen(function* () {
            yield* Effect.addFinalizer(() =>
              Deferred.succeed(closing, undefined).pipe(Effect.andThen(Deferred.await(drained))),
            );
            yield* Deferred.succeed(started, undefined);
            if (outcome === "failure") return yield* new TestFailure();
            if (outcome === "interruption") return yield* Effect.never;
          });
          const owner = yield* withActorStoreLock(directory, use).pipe(Effect.forkScoped);
          yield* Deferred.await(started);
          if (outcome === "interruption") yield* Fiber.interrupt(owner).pipe(Effect.forkScoped);
          yield* Deferred.await(closing);
          assert.throws(() => acquireActorStoreLock(directory), /Cannot acquire actor store/);
          yield* Deferred.succeed(drained, undefined);
          const exit = yield* Fiber.await(owner);
          assert.equal(Exit.isSuccess(exit), outcome === "success");
          acquireActorStoreLock(directory)();
        }),
      ),
    );
  });
}

test("defective shutdown retains the root lock until process exit, including signal races", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "aster-root-defect-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const owner = child(directory, "defective-shutdown");
  t.after(() => owner.process.kill("SIGKILL"));
  assert.equal(await owner.reply, "retained");
  assert.throws(() => acquireActorStoreLock(directory), /Cannot acquire actor store/);
  owner.process.kill("SIGKILL");
  await owner.exit;
  acquireActorStoreLock(directory)();
});
