import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { Effect } from "effect";
import {
  ActorPersistence,
  InMemoryActorPersistence,
  SqliteActorPersistence,
} from "../src/index.js";

for (const backend of ["memory", "sqlite"] as const) {
  test(`${backend} persistence exposes conflicts without losing stream state`, async (t) => {
    const dir = await mkdtemp(join(tmpdir(), "aster-persistence-errors-"));
    t.after(() => rm(dir, { recursive: true, force: true }));
    const layer =
      backend === "memory"
        ? InMemoryActorPersistence.layer
        : SqliteActorPersistence.layer({ path: join(dir, "store.db") });
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const store = yield* ActorPersistence;
          yield* store.append("one", 0, ["event"]);
          const append = yield* store
            .append("one", 0, ["unexpected"])
            .pipe(Effect.catchTag("PersistenceConflict", (error) => Effect.succeed(error)));
          assert.equal(typeof append, "object");
          if (typeof append === "number") assert.fail("Expected an append conflict");
          assert.equal(append.id, "one");
          assert.equal(append.operation, "append");
          assert.equal(append.expected, 0);
          assert.equal(append.actual, 1);
          assert.equal(append.message, "Persistence sequence conflict for one");
          const snapshot = yield* store
            .saveSnapshot("one", 0, "state")
            .pipe(Effect.catchTag("PersistenceConflict", (error) => Effect.succeed(error)));
          assert.ok(snapshot);
          assert.equal(snapshot.operation, "snapshot");
          assert.equal(snapshot.expected, 0);
          assert.equal(snapshot.actual, 1);
          assert.deepEqual(yield* store.load("one"), {
            sequenceNumber: 1,
            snapshot: undefined,
            events: [{ sequenceNumber: 1, payload: "event" }],
          });
        }).pipe(Effect.provide(layer)),
      ),
    );
  });
}

test("SQLite acquisition retains the original error as a typed persistence failure", async (t) => {
  const dir = await mkdtemp(join(tmpdir(), "aster-persistence-open-"));
  t.after(() => rm(dir, { recursive: true, force: true }));
  const error = await Effect.runPromise(
    Effect.scoped(
      ActorPersistence.pipe(
        Effect.provide(SqliteActorPersistence.layer({ path: join(dir, "missing", "store.db") })),
        Effect.flip,
      ),
    ),
  );
  assert.equal(error._tag, "PersistenceError");
  if (error._tag !== "PersistenceError") assert.fail("Expected a storage error");
  assert.equal(error.operation, "open");
  assert.ok(error.cause instanceof Error);
  assert.equal(error.message, error.cause.message);
});
