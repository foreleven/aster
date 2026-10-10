import assert from "node:assert/strict";
import { test } from "node:test";
import {
  mkdtempSync,
  mkdirSync,
  renameSync,
  rmSync,
  existsSync,
  readFileSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Effect, Schema } from "effect";
import { type StoredContext } from "@aster/core";
import { LocalDurableContext } from "../src/storage/local-durable.js";
import { makeFileContextStore } from "../src/storage/file-context-store.js";
const definition = {
  path: "/source",
  state: Schema.Struct({ summary: Schema.String, credential: Schema.String }),
  message: Schema.String,
  events: "state" as const,
  serialize: (state: { summary: string }) => state.summary,
  initial: {
    description: "Source",
    state: { summary: "Before", credential: "PRIVATE_TRANSPORT_TOKEN" },
    messages: ["Evidence"],
  },
};
const assertHandoff = (record: StoredContext) => {
  assert.equal(record.events.length, 1);
  const event = record.events[0]!;
  assert.equal(event.record.path, definition.path);
  assert.equal(event.record.revision, 2);
  assert.equal(event.record.text, "Ready");
  assert.equal(JSON.stringify(event).includes("PRIVATE_TRANSPORT_TOKEN"), false);
  assert.equal("state" in event.record, false);
  assert.equal("messages" in event.record, false);
};

test("file pending recovery restores source State and frozen text after a native rename failure", async (t) => {
  const root = mkdtempSync(join(tmpdir(), "aster-reaction-file-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const store = makeFileContextStore(root);
        const durable = yield* LocalDurableContext.fromStore(store);
        const owner = yield* durable.open(definition);
        const statePath = join(root, "source/state.json");
        renameSync(statePath, `${statePath}.previous`);
        mkdirSync(statePath);
        assert.equal(
          (yield* owner.state.update((state) => ({ ...state, summary: "Ready" })).pipe(Effect.flip))
            ._tag,
          "ContextCommitError",
        );
        assert.equal(durable.journal().length, 0);
        assert.ok(existsSync(join(root, "source/.pending.json")));
        rmSync(statePath, { recursive: true });
        const recovered = yield* LocalDurableContext.fromStore(makeFileContextStore(root));
        assertHandoff(makeFileContextStore(root).loadAll()[0]!);
        const restored = yield* recovered.open(definition);
        assert.equal((yield* restored.snapshot).revision, 2);
        yield* restored.state.update((state) => state);
        assert.equal((yield* restored.snapshot).revision, 2);
      }),
    ),
  );
});

test("Local storage retains frozen text through later message commits and restart", async (t) => {
  const root = mkdtempSync(join(tmpdir(), "aster-reaction-reopen-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  let committed: StoredContext | undefined;
  for (const restart of [false, true])
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const store = makeFileContextStore(root);
          const durable = yield* LocalDurableContext.fromStore(store);
          const owner = yield* durable.open(definition);
          if (!restart) {
            yield* owner.state.update((state) => ({ ...state, summary: "Ready" }));
            committed = store.loadAll()[0];
          } else {
            assert.deepEqual(store.loadAll()[0], committed);
            yield* owner.messages.append(["new evidence"]);
            assert.equal((yield* owner.snapshot).revision, 3);
          }
          assertHandoff(store.loadAll()[0]!);
          assert.equal(durable.journal()[0]?.record.text, "Ready");
        }),
      ),
    );
});

test("corrupt pending source envelopes fail before rewriting committed files", async (t) => {
  const root = mkdtempSync(join(tmpdir(), "aster-reaction-corrupt-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const store = makeFileContextStore(root);
        const durable = yield* LocalDurableContext.fromStore(store);
        const owner = yield* durable.open(definition);
        yield* owner.state.update((state) => ({ ...state, summary: "Ready" }));
        const record = store.loadAll()[0]!;
        const event = record.events[0]!;
        const statePath = join(root, "source/state.json"),
          messagesPath = join(root, "source/messages.jsonl"),
          pendingPath = join(root, "source/.pending.json");
        const stateBefore = readFileSync(statePath, "utf8"),
          messagesBefore = readFileSync(messagesPath, "utf8");
        for (const events of [
          [{ ...event, record: { ...event.record, path: "/other" } }],
          [{ ...event, record: { ...event.record, revision: 4 } }],
          [{ ...event, id: "forged" }],
          [{ ...event, createdAt: "invalid" }],
          [event, event],
        ]) {
          const pending = JSON.stringify({
            snapshot: { ...record.snapshot, revision: 3, state: { summary: "Invalid overwrite" } },
            events,
          });
          writeFileSync(pendingPath, pending);
          const restored = yield* Effect.suspend(() =>
            LocalDurableContext.fromStore(makeFileContextStore(root)),
          ).pipe(Effect.exit);
          assert.equal(restored._tag, "Failure");
          assert.equal(readFileSync(statePath, "utf8"), stateBefore);
          assert.equal(readFileSync(messagesPath, "utf8"), messagesBefore);
          assert.equal(readFileSync(pendingPath, "utf8"), pending);
        }
      }),
    ),
  );
});
