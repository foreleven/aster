import { PiStorageLease } from "@aster/agent";
import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { Effect } from "effect";
import type { ContextSnapshot } from "@aster/core";
import { makeFileContextStore } from "../src/storage/file-context-store.js";
import { migrateContextStorage } from "../src/storage/migration.js";
import { routingAuthorityStore, type StorageAuthority } from "../src/storage/routing.js";
import { PiDurableContext } from "../src/storage/pi-durable-context.js";
import { acquireActorStoreLock } from "../src/storage/actor-store-lock.js";

const setup = (t: { after: (cleanup: () => void) => void }) => {
  const root = mkdtempSync(join(tmpdir(), "aster-migration-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const authority: StorageAuthority = {
    version: 1,
    localDirectory: join(root, "actors"),
    pi: { directory: join(root, "pi"), ownerId: "test" },
    routes: [{ prefix: "/personal", backend: "pi" }],
  };
  return { root, authority, local: makeFileContextStore(authority.localDirectory) };
};
const original: ContextSnapshot = {
  path: "/personal",
  description: "Personal",
  revision: 7,
  state: {
    receipts: [{ id: "input-1", status: "accepted" }],
    outbox: [{ id: "delivery-1", status: "pending" }],
  },
  messages: [
    { sequence: 1, text: "Accepted input" },
    { sequence: 2, text: "Pending delivery" },
  ],
};
const migrate = (root: string, authority: StorageAuthority) =>
  Effect.runPromise(migrateContextStorage({ root, authority }).pipe(Effect.scoped));

test("offline migration preserves full snapshots, is repeatable, and rollback carries new Pi commits", async (t) => {
  const { root, authority, local } = setup(t);
  local.save({ snapshot: original, events: [] });
  local.save({ snapshot: { ...original, path: "/personal-archive" }, events: [] });
  const forward = await migrate(root, authority);
  assert.deepEqual(forward, { checked: 2, copied: 1, routes: authority.routes });
  assert.equal((await migrate(root, authority)).copied, 0);
  let newest = original;
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const backend = yield* PiDurableContext.directory({
          directory: authority.pi!.directory,
          shardId: "test",
        });
        assert.deepEqual(backend.get("/personal"), original);
        assert.equal(backend.get("/personal-archive"), undefined);
        newest = yield* backend.commit(
          {
            ...original,
            messages: [...original.messages, { sequence: 3, text: "Delivered" }],
            state: { receipts: ["input-1", "input-2"], outbox: [] },
          },
          { expectedRevision: 7 },
        );
        assert.equal(newest.revision, 8);
      }),
    ),
  );
  const reverse = { ...authority, routes: [] };
  assert.equal((await migrate(root, reverse)).copied, 1);
  assert.deepEqual(
    makeFileContextStore(authority.localDirectory)
      .loadAll()
      .find((record) => record.snapshot.path === "/personal")?.snapshot,
    newest,
  );
  assert.equal((await migrate(root, reverse)).copied, 0);
  await Effect.runPromise(
    Effect.gen(function* () {
      const store = yield* routingAuthorityStore(root);
      const saved = yield* store.read;
      assert.equal(saved?.revision, 2);
      assert.equal(saved?.messages.length, 2);
      assert.deepEqual(saved?.value, reverse);
      assert.equal((yield* store.verify(authority).pipe(Effect.flip))._tag, "StorageRoutingError");
    }),
  );
});

test("explicit zero-revision snapshots migrate at revision zero and accept the next CAS commit", async (t) => {
  const { root, authority, local } = setup(t);
  const zero = { ...original, revision: 0 };
  local.save({ snapshot: zero, events: [] });
  await migrate(root, authority);
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const backend = yield* PiDurableContext.directory({
          directory: authority.pi!.directory,
          shardId: "test",
        });
        assert.deepEqual(backend.get("/personal"), { ...zero, revision: 0 });
        const next = yield* backend.commit(
          { ...zero, messages: [...zero.messages, "next"] },
          { expectedRevision: 0 },
        );
        assert.equal(next.revision, 1);
      }),
    ),
  );
});

test("migration validates every copy before writing and leaves routing authority unchanged", async (t) => {
  const { root, authority, local } = setup(t);
  local.save({ snapshot: { ...original, path: "/a", revision: 1 }, events: [] });
  local.save({ snapshot: { ...original, path: "/z", revision: 1 }, events: [] });
  const previous = { ...authority, routes: [] };
  await migrate(root, previous);
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const pi = yield* PiDurableContext.directory({
          directory: authority.pi!.directory,
          shardId: "test",
        });
        yield* pi.commit(
          { ...original, path: "/z", messages: ["divergent"] },
          { expectedRevision: 0 },
        );
      }),
    ),
  );
  // Even a no-op route change must reject divergent unselected data.
  await assert.rejects(migrate(root, previous), /Stored copy diverges/);
  await assert.rejects(
    migrate(root, { ...authority, routes: [{ prefix: "/a", backend: "pi" }] }),
    /Stored copy diverges/,
  );
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const store = yield* routingAuthorityStore(root);
        assert.deepEqual((yield* store.read)?.value, previous);
        const pi = yield* PiDurableContext.directory({
          directory: authority.pi!.directory,
          shardId: "test",
        });
        assert.equal(pi.get("/a"), undefined);
        yield* pi.commit({ ...original, path: "/z", messages: ["newer"] }, { expectedRevision: 1 });
      }),
    ),
  );
  await assert.rejects(migrate(root, previous), /Stored copy diverges/);
});

test("migration refuses held root and Pi leases and cannot silently relocate existing stores", async (t) => {
  const { root, authority } = setup(t);
  for (const directory of [root]) {
    const release = acquireActorStoreLock(directory);
    try {
      await assert.rejects(migrate(root, authority), /Stop Aster|already owned/);
    } finally {
      release();
    }
  }
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        yield* PiStorageLease.acquire(authority.pi!.directory, "test");
        yield* Effect.promise(() => assert.rejects(migrate(root, authority), /already owned/));
      }),
    ),
  );
  await migrate(root, authority);
  await assert.rejects(
    migrate(root, { ...authority, pi: { ...authority.pi!, ownerId: "different" } }),
    /Keep existing storage/,
  );
});

test("interrupted rollback retains old authority and safely reconciles partial copies on retry", async (t) => {
  const { root, authority } = setup(t);
  await migrate(root, authority);
  const records = ["/personal/a", "/personal/z"].map((path) => ({
    ...original,
    path,
    revision: 1,
  }));
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const pi = yield* PiDurableContext.directory({
          directory: authority.pi!.directory,
          shardId: "test",
        });
        for (const record of records) yield* pi.commit(record, { expectedRevision: 0 });
      }),
    ),
  );
  // A directory at the destination message-file path produces a real rename failure
  // after the first record has copied and the second pending intent is durable.
  const obstacle = join(authority.localDirectory, "personal/z/messages.jsonl");
  mkdirSync(obstacle, { recursive: true });
  const reverse = { ...authority, routes: [] };
  await assert.rejects(migrate(root, reverse), /Cannot import Local Context/);
  const saved = await Effect.runPromise(
    routingAuthorityStore(root).pipe(Effect.flatMap((store) => store.read)),
  );
  assert.deepEqual(saved?.value, authority);
  rmSync(obstacle, { recursive: true });
  const retried = await migrate(root, reverse);
  assert.equal(retried.checked, 2);
  assert.deepEqual(
    makeFileContextStore(authority.localDirectory).loadAll(),
    records.map((snapshot) => ({ snapshot, events: [] })),
  );
  assert.equal((await migrate(root, reverse)).copied, 0);
});
