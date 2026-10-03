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
  LocalDurableContext,
  makeContextRegistryWithBackend,
  type ContextRecord,
} from "@aster/core";
import { makeFileContextStore } from "../src/storage/file-context-store.js";
import { PiDurableContext } from "../src/storage/pi-durable-context.js";

const definition = defineContext({
  identity: "Source",
  state: Schema.Struct({ summary: Schema.String, credential: Schema.String }),
  message: Schema.String,
  signalSource: true,
  view: contextView({ state: Schema.Struct({ summary: Schema.String }), message: Schema.String }),
});
const source = {
  path: "/source",
  description: "Source",
  state: { summary: "Ready", credential: "PRIVATE_TRANSPORT_TOKEN" },
  messages: ["Evidence"],
};
const assertHandoff = (record: ContextRecord) => {
  assert.equal(record.reactionEvents?.length, 1);
  const event = record.reactionEvents![0]!;
  assert.equal(event.source, source.path);
  assert.equal(event.target, "/system-one");
  assert.equal(event.revision, record.revision);
  assert.equal(event.causationId, event.requestId);
  assert.deepEqual(event.record.state, { summary: "Ready" });
  assert.deepEqual(event.record.messages, ["Evidence"]);
  assert.equal(JSON.stringify(event).includes("PRIVATE_TRANSPORT_TOKEN"), false);
};

test("file pending recovery restores the source and reaction handoff after a native rename failure", async (t) => {
  const root = mkdtempSync(join(tmpdir(), "aster-reaction-file-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  await Effect.runPromise(
    Effect.gen(function* () {
      const registry = makeContextRegistryWithBackend(
        yield* LocalDurableContext.fromStore(makeFileContextStore(root)),
      );
      yield* registry.register(source.path, definition);
      yield* registry.commit(
        { ...source, state: { ...source.state, summary: "Before" } },
        { expectedRevision: 0, evaluate: false },
      );
      const statePath = join(root, "source/state.json");
      renameSync(statePath, `${statePath}.previous`);
      mkdirSync(statePath);
      const failed = yield* registry.commit(source, { expectedRevision: 1 }).pipe(Effect.result);
      assert.equal(failed._tag, "Failure");
      assert.equal(registry.get(source.path)?.reactionEvents, undefined);
      assert.ok(existsSync(join(root, "source/.pending.json")));
      rmSync(statePath, { recursive: true });
      const restored = makeContextRegistryWithBackend(
        yield* LocalDurableContext.fromStore(makeFileContextStore(root)),
      );
      yield* restored.register(source.path, definition);
      const accepted = restored.get(source.path)!;
      assertHandoff(accepted);
      assert.deepEqual(makeFileContextStore(root).loadAll(), [accepted]);
      const duplicate = yield* restored.commit(source, { expectedRevision: 2 });
      assert.deepEqual(duplicate, accepted);
    }),
  );
});

test("Pi reopens atomic source handoffs and preserves them through later owner writes", async (t) => {
  const root = mkdtempSync(join(tmpdir(), "aster-reaction-pi-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  let committed: ContextRecord | undefined;
  for (const restart of [false, true]) {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const registry = makeContextRegistryWithBackend(
            yield* PiDurableContext.directory({ directory: root, shardId: "reactions" }),
          );
          yield* registry.register(source.path, definition);
          if (!restart) committed = yield* registry.commit(source, { expectedRevision: 0 });
          else assert.deepEqual(registry.get(source.path), committed);
          assertHandoff(registry.get(source.path)!);
          if (restart) {
            const later = yield* registry.commit(
              { ...source, messages: ["Evidence", "New message"], reactionEvents: [] },
              { expectedRevision: 1 },
            );
            assert.equal(later.revision, 2);
            assert.deepEqual(later.reactionEvents, committed?.reactionEvents);
            assert.equal("reactionEvents" in registry.project(later), false);
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
      const registry = makeContextRegistryWithBackend(
        yield* LocalDurableContext.fromStore(makeFileContextStore(root)),
      );
      yield* registry.register(source.path, definition);
      const committed = yield* registry.commit(source, { expectedRevision: 0 });
      const event = committed.reactionEvents![0]!;
      const statePath = join(root, "source/state.json");
      const messagesPath = join(root, "source/messages.jsonl");
      const pendingPath = join(root, "source/.pending.json");
      const stateBefore = readFileSync(statePath, "utf8");
      const messagesBefore = readFileSync(messagesPath, "utf8");
      for (const events of [
        [{ ...event, source: "/other" }],
        [{ ...event, revision: 3 }],
        [{ ...event, record: { ...event.record, revision: 2 } }],
        [{ ...event, requestId: "forged" }],
        [{ ...event, causationId: "forged" }],
        [{ ...event, createdAt: "invalid" }],
        [event, event],
      ]) {
        const pending = JSON.stringify({
          ...committed,
          revision: 2,
          state: { summary: "Must never replace the committed state" },
          messages: ["Must never replace committed evidence"],
          reactionEvents: events,
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
