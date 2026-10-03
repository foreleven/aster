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
import {
  ContextCommitError,
  LocalDurableContext,
  defineContext,
  makeContextRegistry,
  makeContextRegistryWithBackend,
} from "@aster/core";
import { makeFileContextStore } from "../src/index.js";

test("JSON state and JSONL messages survive restart, append, and history compaction", async () => {
  const dir = mkdtempSync(join(tmpdir(), "signals-store-"));
  try {
    const definition = defineContext({
      identity: "Goal",
      state: Schema.Struct({ status: Schema.String }),
      message: Schema.Unknown,
    });
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
    assert.equal(records[0]?.messages.length, 2);
    assert.equal(records[0]?.revision, 2);
    assert.deepEqual(records[0]?.state, { status: "completed" });
    assert.equal(
      readFileSync(join(dir, "goals/test/messages.jsonl"), "utf8").trim().split("\n").length,
      2,
    );
    store.save({ ...records[0]!, messages: [{ text: "summary" }] });
    assert.deepEqual(makeFileContextStore(dir).loadAll()[0]?.messages, [{ text: "summary" }]);
    assert.throws(() => store.save({ ...records[0]!, path: "/../outside" }), /Invalid/);
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
    store.save(original);
    const intended = {
      ...original,
      revision: 3,
      state: { status: "completed" },
      messages: [...original.messages, { id: "two" }],
    };
    writeFileSync(join(dir, "signals/a/.pending.json"), JSON.stringify(intended));
    writeFileSync(join(dir, "signals/a/messages.jsonl"), '{"id":"one"}\n{"id":');
    assert.deepEqual(makeFileContextStore(dir).loadAll(), [intended]);
    assert.deepEqual(makeFileContextStore(dir).loadAll(), [intended]);
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
        const registry = makeContextRegistryWithBackend(backend);
        const definition = defineContext({
          identity: "Personal test",
          state: Schema.Struct({
            receipts: Schema.Array(Schema.Unknown),
            outbox: Schema.Array(Schema.Unknown),
          }),
          message: Schema.Unknown,
        });
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
        const recovered = makeContextRegistryWithBackend(recoveredBackend);
        yield* recovered.register(original.path, definition);
        const accepted = recovered.get(original.path)!;
        assert.deepEqual(accepted, { ...intended, revision: 2 });
        assert.equal(existsSync(join(root, "personal/.pending.json")), false);
        assert.deepEqual(makeFileContextStore(root).loadAll(), [accepted]);
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
    store.save(original);
    const state = readFileSync(join(root, "context/state.json"), "utf8");
    const messages = readFileSync(join(root, "context/messages.jsonl"), "utf8");
    writeFileSync(
      join(root, "context/.pending.json"),
      JSON.stringify({ ...original, revision: -1, messages: "invalid" }),
    );
    assert.throws(() => makeFileContextStore(root).loadAll());
    assert.equal(readFileSync(join(root, "context/state.json"), "utf8"), state);
    assert.equal(readFileSync(join(root, "context/messages.jsonl"), "utf8"), messages);
    assert.equal(existsSync(join(root, "context/.pending.json")), true);
  } finally {
    rmSync(root, { recursive: true, force: true });
  }
});

test("Goal full history pages survive restart and recover only an incomplete trailing append", async () => {
  const { makeFileGoalHistory } = await import("../src/index.js");
  const { appendFileSync } = await import("node:fs");
  const dir = mkdtempSync(join(tmpdir(), "aster-history-"));
  try {
    const history = makeFileGoalHistory(dir);
    for (let i = 0; i < 50; i++)
      await Effect.runPromise(
        history.append("project", { role: "user", content: `Evidence ${i}`, timestamp: i }),
      );
    assert.equal(await Effect.runPromise(history.count("project")), 50);
    assert.deepEqual(
      (await Effect.runPromise(history.read("project", { after: 10, before: 15, limit: 2 }))).map(
        (e) => e.seq,
      ),
      [11, 12],
    );
    appendFileSync(join(dir, "project", "history.jsonl"), '{"seq":51');
    const restored = makeFileGoalHistory(dir);
    assert.equal(await Effect.runPromise(restored.count("project")), 50);
    assert.equal(
      (
        await Effect.runPromise(
          restored.append("project", { role: "user", content: "Recovery", timestamp: 51 }),
        )
      ).seq,
      51,
    );
    assert.equal(
      (await Effect.runPromise(makeFileGoalHistory(dir).read("project", { after: 49 }))).length,
      2,
    );
    await assert.rejects(Effect.runPromise(restored.count("../unsafe")));
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test("Goal history deduplicates identified projection writes across concurrent calls and restart", async () => {
  const { makeFileGoalHistory } = await import("../src/index.js");
  const directory = mkdtempSync(join(tmpdir(), "aster-history-receipts-"));
  const message = { role: "user" as const, content: "Accepted Goal input", timestamp: 1 };
  try {
    const history = makeFileGoalHistory(directory);
    const entries = await Effect.runPromise(
      Effect.all(
        [
          history.append("project", message, "delivery-one"),
          history.append("project", message, "delivery-one"),
        ],
        { concurrency: "unbounded" },
      ),
    );
    assert.deepEqual(entries[0], entries[1]);
    const reopened = makeFileGoalHistory(directory);
    assert.deepEqual(
      await Effect.runPromise(reopened.append("project", message, "delivery-one")),
      entries[0],
    );
    await assert.rejects(
      Effect.runPromise(
        reopened.append("project", { ...message, content: "Different" }, "delivery-one"),
      ),
    );
    assert.equal(await Effect.runPromise(reopened.count("project")), 1);
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});

test("Goal history serializes concurrent durable appends and reads indexed tail pages", async () => {
  const { makeFileGoalHistory } = await import("../src/index.js");
  const { appendFile } = await import("node:fs/promises");
  const dir = mkdtempSync(join(tmpdir(), "aster-history-concurrent-"));
  try {
    const history = makeFileGoalHistory(dir);
    const appended = await Promise.all(
      Array.from({ length: 80 }, (_, i) =>
        Effect.runPromise(
          history.append("project", {
            role: "user",
            content: `Concurrent evidence ${i}`,
            timestamp: i,
          }),
        ),
      ),
    );
    assert.deepEqual(
      appended.map((e) => e.seq).sort((a, b) => a - b),
      Array.from({ length: 80 }, (_, i) => i + 1),
    );
    const restored = makeFileGoalHistory(dir);
    let yielded = false;
    const tick = new Promise<void>((resolve) =>
      setImmediate(() => {
        yielded = true;
        resolve();
      }),
    );
    const tail = await Effect.runPromise(restored.read("project", { after: 75, limit: 3 }));
    assert.equal(yielded, true, "loading history must yield the event loop");
    await tick;
    assert.deepEqual(
      tail.map((e) => e.seq),
      [76, 77, 78],
    );
    assert.equal(await Effect.runPromise(restored.count("project")), 80);
    assert.deepEqual(await Effect.runPromise(restored.read("project", { after: 80 })), []);
    // Complete corruption is never silently truncated as an incomplete tail.
    await appendFile(join(dir, "project", "history.jsonl"), '{"seq":999,"message":{}}\n');
    await assert.rejects(Effect.runPromise(makeFileGoalHistory(dir).count("project")));
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});
