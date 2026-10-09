import { makeTestContextRegistryWithBackend } from "@aster/core/testing";
import assert from "node:assert/strict";
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  renameSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { Effect, Schema } from "effect";
import { ContextCommitError } from "@aster/core";
import { LocalDurableContext } from "../src/storage/local-durable.js";
import { makeContextRegistry } from "@aster/core/testing";
import { makeFileContextStore } from "../src/index.js";

test("JSON state and JSONL messages survive restart, append, and history compaction", async () => {
  const dir = mkdtempSync(join(tmpdir(), "signals-store-"));
  try {
    const definition = {
      state: Schema.Struct({ status: Schema.String }),
      message: Schema.Unknown,
    };
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* makeContextRegistry(makeFileContextStore(dir));
          yield* registry.register("/goals/test", definition);
          yield* registry.commit(
            {
              path: "/goals/test",
              description: "Goal",
              state: { status: "active" },
              messages: [{ text: "a\nb" }],
            },
            { expectedRevision: registry.get("/goals/test")?.revision ?? 0 },
          );
          yield* registry.commit(
            {
              ...registry.get("/goals/test")!,
              messages: [{ text: "a\nb" }, { text: "done" }],
              state: { status: "completed" },
            },
            { expectedRevision: registry.get("/goals/test")?.revision ?? 0 },
          );
        }),
      ),
    );
    const store = makeFileContextStore(dir);
    const records = store.loadAll();
    assert.equal(records[0]?.snapshot.messages.length, 2);
    assert.equal(records[0]?.snapshot.revision, 2);
    assert.deepEqual(records[0]?.snapshot.state, { status: "completed" });
    assert.equal(
      readFileSync(join(dir, "goals/test/messages.jsonl"), "utf8").trim().split("\n").length,
      2,
    );
    store.save({
      ...records[0]!,
      snapshot: { ...records[0]!.snapshot, messages: [{ text: "summary" }] },
    });
    assert.deepEqual(makeFileContextStore(dir).loadAll()[0]?.snapshot.messages, [
      { text: "summary" },
    ]);
    assert.throws(
      () =>
        store.save({ ...records[0]!, snapshot: { ...records[0]!.snapshot, path: "/../outside" } }),
      /snapshot.*path|Expected/s,
    );
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("interrupted two-file commit recovers its intended state and messages exactly once", () => {
  const dir = mkdtempSync(join(tmpdir(), "signals-recover-"));
  try {
    const store = makeFileContextStore(dir);
    const original = {
      path: "/signals/a",
      description: "test",
      state: { status: "running" },
      messages: [{ id: "one" }],
    };
    store.save({ snapshot: { ...original, revision: 0 }, events: [] });
    const intended = {
      ...original,
      revision: 3,
      state: { status: "completed" },
      messages: [...original.messages, { id: "two" }],
    };
    writeFileSync(
      join(dir, "signals/a/.pending.json"),
      JSON.stringify({ snapshot: intended, events: [] }),
    );
    writeFileSync(join(dir, "signals/a/messages.jsonl"), '{"id":"one"}\n{"id":');
    assert.deepEqual(makeFileContextStore(dir).loadAll(), [{ snapshot: intended, events: [] }]);
    assert.deepEqual(makeFileContextStore(dir).loadAll(), [{ snapshot: intended, events: [] }]);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("LocalDurableContext recovers state, message, receipt and outbox together after a native rename failure", async () => {
  const root = mkdtempSync(join(tmpdir(), "aster-local-rename-"));
  const original = {
    path: "/personal",
    description: "Personal",
    state: { receipts: [], outbox: [] },
    messages: [],
  };
  const intended = {
    ...original,
    state: {
      receipts: [{ requestId: "input-one", revision: 2 }],
      outbox: [{ requestId: "delivery-one", status: "pending" }],
    },
    messages: [{ requestId: "input-one", text: "Accepted input" }],
  };
  try {
    await Effect.runPromise(
      Effect.gen(function* () {
        const backend = yield* LocalDurableContext.fromStore(makeFileContextStore(root));
        const registry = makeTestContextRegistryWithBackend(backend);
        const definition = {
          state: Schema.Struct({
            receipts: Schema.Array(Schema.Unknown),
            outbox: Schema.Array(Schema.Unknown),
          }),
          message: Schema.Unknown,
        };
        yield* registry.register(original.path, definition);
        yield* registry.commit(original, { expectedRevision: 0 });
        const statePath = join(root, "personal/state.json");
        renameSync(statePath, `${statePath}.previous`);
        mkdirSync(statePath);
        const failed = yield* registry
          .commit(intended, { expectedRevision: 1 })
          .pipe(Effect.result);
        assert.ok(failed._tag === "Failure" && failed.failure instanceof ContextCommitError);
        assert.equal(existsSync(join(root, "personal/.pending.json")), true);
        assert.deepEqual(registry.get(original.path), { ...original, revision: 1 });
        rmSync(statePath, { recursive: true });
        // Reopen the real driver: the durable intent replaces both public files.
        const recoveredBackend = yield* LocalDurableContext.fromStore(makeFileContextStore(root));
        const recovered = makeTestContextRegistryWithBackend(recoveredBackend);
        yield* recovered.register(original.path, definition);
        const accepted = recovered.get(original.path)!;
        assert.deepEqual(accepted, { ...intended, revision: 2 });
        assert.equal(existsSync(join(root, "personal/.pending.json")), false);
        assert.deepEqual(makeFileContextStore(root).loadAll(), [
          { snapshot: accepted, events: [] },
        ]);
        // A no-op at the recovered revision neither duplicates messages nor receipts.
        assert.deepEqual(yield* recovered.commit(intended, { expectedRevision: 2 }), accepted);
      }),
    );
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("invalid pending recovery data is rejected before rewriting committed public files", () => {
  const root = mkdtempSync(join(tmpdir(), "aster-invalid-pending-"));
  try {
    const store = makeFileContextStore(root);
    const original = {
      path: "/context",
      description: "Original",
      state: {},
      messages: ["original"],
    };
    store.save({ snapshot: { ...original, revision: 0 }, events: [] });
    const state = readFileSync(join(root, "context/state.json"), "utf8");
    const messages = readFileSync(join(root, "context/messages.jsonl"), "utf8");
    writeFileSync(
      join(root, "context/.pending.json"),
      JSON.stringify({ snapshot: { ...original, revision: -1, messages: "invalid" }, events: [] }),
    );
    assert.throws(() => makeFileContextStore(root).loadAll());
    assert.equal(readFileSync(join(root, "context/state.json"), "utf8"), state);
    assert.equal(readFileSync(join(root, "context/messages.jsonl"), "utf8"), messages);
    assert.equal(existsSync(join(root, "context/.pending.json")), true);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});
