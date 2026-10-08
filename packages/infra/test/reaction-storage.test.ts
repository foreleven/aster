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
import {
  contextView,
  defineContext,
  makeContextRegistryWithBackend,
  type StoredContext,
} from "@aster/core";
import { LocalDurableContext } from "../src/storage/local-durable.js";
import { makeFileContextStore } from "../src/storage/file-context-store.js";

const definition = defineContext({
  state: Schema.Struct({ summary: Schema.String, credential: Schema.String }),
  message: Schema.String,
  changes: "durable-state",
  view: contextView({ state: Schema.Struct({ summary: Schema.String }), message: Schema.String }),
});
const source = {
  path: "/source",
  description: "Source",
  state: { summary: "Ready", credential: "PRIVATE_TRANSPORT_TOKEN" },
  messages: ["Evidence"],
};
const assertHandoff = (record: StoredContext) => {
  assert.equal(record.events?.length, 1);
  const event = record.events![0]!;
  assert.equal(event.record.path, source.path);
  assert.equal(event.record.revision, record.snapshot.revision);
  assert.deepEqual(event.record.state, { summary: "Ready" });
  assert.deepEqual(event.record.messages, ["Evidence"]);
  assert.equal(JSON.stringify(event).includes("PRIVATE_TRANSPORT_TOKEN"), false);
};

test("file pending recovery restores the source and reaction handoff after a native rename failure", async (t) => {
  const root = mkdtempSync(join(tmpdir(), "aster-reaction-file-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  await Effect.runPromise(
    Effect.gen(function* () {
      const backend = yield* LocalDurableContext.fromStore(makeFileContextStore(root));
      const registry = makeContextRegistryWithBackend(backend);
      yield* registry.register(source.path, definition);
      yield* registry.commit(
        { ...source, state: { ...source.state, summary: "Before" } },
        { expectedRevision: 0, mode: "bootstrap" },
      );
      const statePath = join(root, "source/state.json");
      renameSync(statePath, `${statePath}.previous`);
      mkdirSync(statePath);
      const failed = yield* registry.commit(source, { expectedRevision: 1 }).pipe(Effect.result);
      assert.equal(failed._tag, "Failure");
      assert.equal(backend.journal().length, 0);
      assert.ok(existsSync(join(root, "source/.pending.json")));
      rmSync(statePath, { recursive: true });
      const recoveredBackend = yield* LocalDurableContext.fromStore(makeFileContextStore(root));
      const restored = makeContextRegistryWithBackend(recoveredBackend);
      yield* restored.register(source.path, definition);
      const accepted = restored.get(source.path)!;
      assertHandoff(recoveredBackend.exportRecords()[0]!);
      assert.deepEqual(makeFileContextStore(root).loadAll(), recoveredBackend.exportRecords());
      const duplicate = yield* restored.commit(source, { expectedRevision: 2 });
      assert.deepEqual(duplicate, accepted);
    }),
  );
});

test("Local storage reopens atomic source handoffs and preserves them through later owner writes", async (t) => {
  const root = mkdtempSync(join(tmpdir(), "aster-reaction-reopen-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  let committed: StoredContext | undefined;
  for (const restart of [false, true]) {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const backend = yield* LocalDurableContext.fromStore(makeFileContextStore(root));
          const registry = makeContextRegistryWithBackend(backend);
          yield* registry.register(source.path, definition);
          if (!restart) {
            yield* registry.commit(source, { expectedRevision: 0 });
            committed = backend.exportRecords()[0]!;
          } else assert.deepEqual(backend.exportRecords()[0], committed);
          assertHandoff(backend.exportRecords()[0]!);
          if (restart) {
            const forged = { ...source, messages: ["Evidence", "New message"], events: [] };
            const later = yield* registry.commit(forged, { expectedRevision: 1 });
            assert.equal(later.revision, 2);
            assert.deepEqual(backend.exportRecords()[0]!.events, committed?.events);
            assert.equal("events" in registry.views.project(later), false);
          }
        }),
      ),
    );
  }
});

test("corrupt pending reaction envelopes fail before recovery rewrites committed files", async (t) => {
  const root = mkdtempSync(join(tmpdir(), "aster-reaction-corrupt-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  await Effect.runPromise(
    Effect.gen(function* () {
      const backend = yield* LocalDurableContext.fromStore(makeFileContextStore(root));
      const registry = makeContextRegistryWithBackend(backend);
      yield* registry.register(source.path, definition);
      const committed = yield* registry.commit(source, { expectedRevision: 0 });
      const event = backend.exportRecords()[0]!.events![0]!;
      const statePath = join(root, "source/state.json");
      const messagesPath = join(root, "source/messages.jsonl");
      const pendingPath = join(root, "source/.pending.json");
      const stateBefore = readFileSync(statePath, "utf8");
      const messagesBefore = readFileSync(messagesPath, "utf8");
      for (const events of [
        [{ ...event, record: { ...event.record, path: "/other" } }],
        [{ ...event, record: { ...event.record, revision: 2 } }],
        [{ ...event, id: "forged" }],
        [{ ...event, createdAt: "invalid" }],
        [event, event],
      ]) {
        const pending = JSON.stringify({
          snapshot: {
            ...committed,
            revision: 2,
            state: { summary: "Must never replace the committed state" },
            messages: ["Must never replace committed evidence"],
          },
          events,
        });
        writeFileSync(pendingPath, pending);
        const restored = yield* LocalDurableContext.fromStore(makeFileContextStore(root)).pipe(
          Effect.result,
        );
        assert.equal(restored._tag, "Failure");
        assert.equal(readFileSync(statePath, "utf8"), stateBefore);
        assert.equal(readFileSync(messagesPath, "utf8"), messagesBefore);
        assert.equal(readFileSync(pendingPath, "utf8"), pending);
      }
    }),
  );
});
