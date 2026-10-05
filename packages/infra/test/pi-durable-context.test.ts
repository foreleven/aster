import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { BACKGROUND_CONTEXT, withAbortSignal } from "@earendil-works/chord/context";
import {
  createSession,
  MemoryStorage,
  StorageRejected,
  type StorageWrite,
} from "@earendil-works/pi-durable";
import { openNodeJsonlStorage } from "@earendil-works/pi-durable/storage/jsonl/node";
import {
  ContextRecoveryError,
  makeContextRegistryWithBackend,
  defineContext,
  type ContextRecord,
} from "@aster/core";
import { Cause, Deferred, Effect, Exit, Fiber, Schema } from "effect";
import { PiDurableContext } from "../src/storage/pi-durable-context.js";
import {
  PiContextCommit,
  PiContextDocument,
  PiContextIndex,
} from "../src/storage/pi-context-documents.js";

const input: ContextRecord = {
  path: "/personal",
  description: "Personal state",
  state: { receipt: "request-1", outbox: [{ id: "delivery-1", status: "pending" }] },
  messages: [{ sequence: 1, text: "Accepted input" }],
};
const openAt = (directory: string) =>
  Effect.tryPromise({
    try: (signal) =>
      openNodeJsonlStorage(directory, withAbortSignal(signal, BACKGROUND_CONTEXT), { fsync: true }),
    catch: (cause) => new ContextRecoveryError({ path: "/", cause }),
  });
const temp = (t: { after: (cleanup: () => void) => void }) => {
  const path = mkdtempSync(join(tmpdir(), "aster-pi-context-"));
  t.after(() => rmSync(path, { recursive: true, force: true }));
  return path;
};

test("Pi Context recovery reads every conversation and finds its commit past unrelated entry pages", async (t) => {
  const directory = temp(t);
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const backend = yield* PiDurableContext.directory({ directory, shardId: "test" });
        for (let i = 0; i < 103; i += 1)
          yield* backend.commit({ ...input, path: `/goals/goal-${i}` }, { expectedRevision: 0 });
      }),
    ),
  );
  const storage = await Effect.runPromise(openAt(directory));
  const session = createSession(storage);
  try {
    await session.commit(async (tx) => {
      const first = (await tx.scanConversations({}, 1)).items[0];
      for (let i = 0; i < 103; i += 1)
        await tx.appendEntry(first.id, { kind: "app.audit", data: { position: i } });
    }, BACKGROUND_CONTEXT);
  } finally {
    await session.close(BACKGROUND_CONTEXT);
  }
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const backend = yield* PiDurableContext.directory({ directory, shardId: "test" });
        assert.equal(Object.keys(backend.snapshot()).length, 103);
        for (let i = 0; i < 103; i += 1)
          assert.deepEqual(backend.get(`/goals/goal-${i}`), {
            ...input,
            path: `/goals/goal-${i}`,
            revision: 1,
          });
      }),
    ),
  );
});

test("Pi Context commits its entry, snapshot, receipts, outbox and mapping in one batch without a model", async () => {
  const storage = new MemoryStorage();
  const batches: (readonly StorageWrite[])[] = [];
  const commit = storage.commit.bind(storage);
  storage.commit = async (writes, context) => {
    batches.push(structuredClone(writes));
    return commit(writes, context);
  };
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const backend = yield* PiDurableContext.make({
          shardId: "test",
          openStorage: Effect.succeed(storage),
        });
        const registry = makeContextRegistryWithBackend(backend);
        yield* registry.register(
          input.path,
          defineContext({
            state: Schema.Record(Schema.String, Schema.Unknown),
            message: Schema.Unknown,
          }),
        );
        const saved = yield* registry.commit(input, { expectedRevision: 0 });
        assert.equal(saved.revision, 1);
        assert.equal(batches.length, 1);
        assert.deepEqual(
          batches[0].map((write) => write.type).sort(),
          ["conversation", "document.create", "document.create", "entry"].sort(),
        );
        const entryWrite = batches[0].find((write) => write.type === "entry");
        assert.ok(entryWrite);
        const conversations = yield* Effect.promise(() =>
          storage.scanConversations({}, 100, undefined, BACKGROUND_CONTEXT),
        );
        const entries = yield* Effect.promise(() =>
          storage.scanEntries(
            { conversationId: conversations.items[0].id },
            100,
            undefined,
            BACKGROUND_CONTEXT,
          ),
        );
        assert.equal(entries.items.length, 1);
        assert.equal(entries.items[0].kind, PiContextCommit.kind);
        assert.equal(entries.items[0].model, undefined);
        assert.deepEqual(entries.items[0].data, { mappingVersion: 1, record: saved });
        assert.deepEqual(yield* registry.commit(input, { expectedRevision: 1 }), saved);
        assert.equal(batches.length, 1);
        const stale = yield* registry.commit(input, { expectedRevision: 0 }).pipe(Effect.flip);
        assert.equal(stale._tag, "ContextConflict");
        assert.equal(batches.length, 1);
      }),
    ),
  );
});

test("Pi Context reopens snapshots and replacement message windows with stable conversation mapping", async (t) => {
  const directory = temp(t);
  const options = { directory, shardId: "personal" };
  const saved = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const backend = yield* PiDurableContext.directory(options);
        yield* backend.commit(input, { expectedRevision: 0 });
        return yield* backend.commit(
          { ...input, messages: [{ sequence: 2, text: "Compacted window" }] },
          { expectedRevision: 1 },
        );
      }),
    ),
  );
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const backend = yield* PiDurableContext.directory(options);
        assert.deepEqual(backend.get(input.path), saved);
        const next = yield* backend.commit(
          { ...saved, state: { receipt: "request-2" } },
          { expectedRevision: 2 },
        );
        assert.equal(next.revision, 3);
      }),
    ),
  );
  const storage = await Effect.runPromise(openAt(directory));
  const session = createSession(storage);
  try {
    const index = await session.snapshot(PiContextIndex, BACKGROUND_CONTEXT);
    assert.equal(index?.contexts.length, 1);
    assert.equal(index?.contexts[0].revision, 3);
    const conversations = await storage.scanConversations({}, 100, undefined, BACKGROUND_CONTEXT);
    assert.equal(conversations.items.length, 1);
    const entries = await storage.scanEntries(
      { conversationId: conversations.items[0].id },
      100,
      undefined,
      BACKGROUND_CONTEXT,
    );
    assert.equal(entries.items.length, 3);
    assert.deepEqual(entries.items[1].data, { mappingVersion: 1, record: saved });
    assert.equal(index?.contexts[0].entryId, entries.items[0].id);
  } finally {
    await session.close(BACKGROUND_CONTEXT);
  }
});

for (const outcome of ["rejected", "unknown"] as const) {
  test(`Pi Context fences a ${outcome} commit and reconciles by reopening before further writes`, async (t) => {
    const directory = temp(t);
    let fault = false;
    let opened = 0;
    let closed = 0;
    const openStorage = openAt(directory).pipe(
      Effect.map((storage) => {
        opened += 1;
        const commit = storage.commit.bind(storage);
        const close = storage.close.bind(storage);
        storage.close = async (context) => {
          closed += 1;
          return close(context);
        };
        storage.commit = async (writes, context) => {
          if (!fault) return commit(writes, context);
          fault = false;
          if (outcome === "rejected") throw new StorageRejected("injected rejection before write");
          await commit(writes, context);
          throw new Error("injected lost acknowledgement after write");
        };
        return storage;
      }),
    );
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const backend = yield* PiDurableContext.make({ shardId: "test", openStorage });
          const first = yield* backend.commit(input, { expectedRevision: 0 });
          fault = true;
          const next = { ...input, messages: [{ sequence: 2, text: "next" }] };
          const failed = yield* backend.commit(next, { expectedRevision: 1 }).pipe(Effect.flip);
          assert.equal(failed._tag, "ContextCommitError");
          assert.deepEqual(backend.get(input.path), first);
          assert.equal(
            (yield* backend.commit(next, { expectedRevision: 1 }).pipe(Effect.flip))._tag,
            "ContextCommitError",
          );
          // An uncertain Session is shard-wide: another Context cannot write through it.
          assert.equal(
            (yield* backend
              .commit({ ...input, path: "/goals/test" }, { expectedRevision: 0 })
              .pipe(Effect.flip))._tag,
            "ContextCommitError",
          );
          yield* backend.recover(input.path, (record) => record);
          assert.equal(opened, 2);
          assert.equal(closed, 1);
          const expectedRevision = outcome === "unknown" ? 2 : 1;
          assert.equal(backend.get(input.path)?.revision, expectedRevision);
          const saved = yield* backend.commit(next, { expectedRevision });
          assert.equal(saved.revision, 2);
          yield* backend.recover("/goals/test", (record) => record);
          yield* backend.commit({ ...input, path: "/goals/test" }, { expectedRevision: 0 });
        }),
      ),
    );
    assert.equal(closed, 2);
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const backend = yield* PiDurableContext.directory({ directory, shardId: "test" });
          assert.equal(backend.get(input.path)?.revision, 2);
          assert.equal(backend.get("/goals/test")?.revision, 1);
        }),
      ),
    );
  });
}

test("Pi Context rejects concurrent owners, wrong shard identity and inconsistent document mappings", async (t) => {
  const directory = temp(t);
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const backend = yield* PiDurableContext.directory({ directory, shardId: "test" });
        yield* backend.commit(input, { expectedRevision: 0 });
        const other = yield* PiDurableContext.directory({ directory, shardId: "test" }).pipe(
          Effect.scoped,
          Effect.flip,
        );
        assert.equal(other._tag, "ContextRecoveryError");
      }),
    ),
  );
  const wrong = await Effect.runPromise(
    PiDurableContext.directory({ directory, shardId: "wrong" }).pipe(Effect.scoped, Effect.flip),
  );
  assert.equal(wrong._tag, "ContextRecoveryError");
  const storage = await Effect.runPromise(openAt(directory));
  const session = createSession(storage);
  try {
    await session.commit(async (tx) => {
      const conversations = await tx.scanConversations({}, 100);
      const snapshot = await tx.doc(PiContextDocument, conversations.items[0].id);
      snapshot.entryId = 123456;
    }, BACKGROUND_CONTEXT);
  } finally {
    await session.close(BACKGROUND_CONTEXT);
  }
  const corrupt = await Effect.runPromise(
    PiDurableContext.directory({ directory, shardId: "test" }).pipe(Effect.scoped, Effect.flip),
  );
  assert.equal(corrupt._tag, "ContextRecoveryError");
});

test("Pi Context cancellation drains storage acknowledgement before closing its Session", async () => {
  const storage = new MemoryStorage();
  let closed = false;
  const close = storage.close.bind(storage);
  storage.close = async (context) => {
    closed = true;
    return close(context);
  };
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const commit = storage.commit.bind(storage);
        storage.commit = async (writes, context) => {
          await Effect.runPromise(Deferred.succeed(entered, undefined));
          await Effect.runPromise(Deferred.await(release));
          return commit(writes, context);
        };
        const fiber = yield* Effect.scoped(
          Effect.gen(function* () {
            const backend = yield* PiDurableContext.make({
              shardId: "test",
              openStorage: Effect.succeed(storage),
            });
            yield* backend.commit(input, { expectedRevision: 0 });
          }),
        ).pipe(Effect.forkScoped);
        yield* Deferred.await(entered);
        const interruption = yield* Fiber.interrupt(fiber).pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        assert.equal(closed, false);
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(interruption);
        assert.equal(closed, true);
        const exit = yield* Fiber.await(fiber);
        assert.ok(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause));
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});
