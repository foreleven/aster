import assert from "node:assert/strict";
import { test } from "node:test";
import { Effect, Schema } from "effect";
import { contextView, type StoredContext } from "../src/index.js";
import { makeContextRegistry } from "../src/testing/context.js";

const definition = {
  state: Schema.Struct({ summary: Schema.String, token: Schema.String }),
  message: Schema.String,
  changes: "durable-state" as const,
  view: contextView({ state: Schema.Struct({ summary: Schema.String }), message: Schema.String }),
};
const initial = {
  path: "/source",
  description: "Source",
  state: { summary: "first", token: "PRIVATE_SOURCE_TOKEN" },
  messages: [],
};

test("source state and public reaction handoff commit together and survive uncertain acknowledgement", async () => {
  let persisted: StoredContext | undefined;
  let fail = true;
  const store = {
    loadAll: () => (persisted ? [structuredClone(persisted)] : []),
    save: (record: StoredContext) => {
      persisted = structuredClone(record);
      if (fail) throw new Error("Commit acknowledgement lost");
    },
  };
  await Effect.runPromise(
    Effect.gen(function* () {
      const registry = yield* makeContextRegistry(store);
      yield* registry.register(initial.path, definition);
      const failed = yield* registry.commit(initial, { expectedRevision: 0 }).pipe(Effect.result);
      assert.equal(failed._tag, "Failure");
      assert.equal(registry.get(initial.path), undefined);
      assert.equal(persisted?.snapshot.revision, 1);
      const event = persisted?.events?.[0];
      assert.ok(event);
      assert.equal(event.record.revision, 1);
      assert.equal(JSON.stringify(event).includes("PRIVATE_SOURCE_TOKEN"), false);
      assert.equal((persisted?.snapshot.state as { token: string }).token, "PRIVATE_SOURCE_TOKEN");
      fail = false;
      // Same-process storage reconciliation preserves metadata even though domain validation omits it.
      yield* registry.register(initial.path, definition);
      assert.deepEqual(
        registry.backend.exportRecords().find((record) => record.snapshot.path === initial.path)
          ?.events,
        [event],
      );
      const restarted = yield* makeContextRegistry(store);
      yield* restarted.register(initial.path, definition);
      const current = restarted.get(initial.path)!;
      const forged = { ...current, events: [] };
      yield* restarted.commit(forged, { expectedRevision: 1 });
      assert.deepEqual(
        restarted.backend.exportRecords().find((record) => record.snapshot.path === initial.path)
          ?.events,
        [event],
      );
      const forgedNext = {
        ...initial,
        state: { ...initial.state, summary: "second" },
        events: [],
      };
      yield* restarted.commit(forgedNext, { expectedRevision: 1 });
      const next = restarted.backend
        .exportRecords()
        .find((record) => record.snapshot.path === initial.path)!;
      assert.equal(next.events?.length, 2);
      assert.deepEqual(
        next.events?.map((item) => item.record.revision),
        [1, 2],
      );
      assert.equal((next.events?.[0]?.record.state as { summary: string }).summary, "first");
      assert.equal((next.events?.[1]?.record.state as { summary: string }).summary, "second");
      assert.equal("events" in restarted.views.project(next.snapshot), false);
    }),
  );
});

test("bootstrap, message-only and description-only changes never create reaction work", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const registry = yield* makeContextRegistry();
      yield* registry.register(initial.path, definition);
      const first = yield* registry.commit(
        { ...initial, description: "" },
        { expectedRevision: 0, mode: "bootstrap" },
      );
      assert.equal("events" in first, false);
      assert.equal(registry.backend.journal().length, 0);
      yield* registry.commit({ ...first, description: "Source" }, { expectedRevision: 1 });
      const message = yield* registry.commit(
        { ...registry.get(initial.path)!, messages: ["evidence"] },
        { expectedRevision: 2 },
      );
      assert.equal("events" in message, false);
      const changed = yield* registry.commit(
        { ...message, state: { ...initial.state, summary: "changed" } },
        { expectedRevision: 3 },
      );
      assert.equal(changed.revision, 4);
      assert.equal(registry.backend.journal().length, 1);
      const stale = yield* registry.commit(initial, { expectedRevision: 3 }).pipe(Effect.result);
      assert.equal(stale._tag, "Failure");
      assert.equal(
        registry.backend.exportRecords().find((record) => record.snapshot.path === initial.path)
          ?.events?.length,
        1,
      );
    }),
  );
});

test("recovery rejects corrupted reaction envelopes instead of replaying another source", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const registry = yield* makeContextRegistry();
      yield* registry.register(initial.path, definition);
      const committed = yield* registry.commit(initial, { expectedRevision: 0 });
      const event = registry.backend.exportRecords()[0]!.events![0]!;
      for (const corrupt of [
        { ...event, record: { ...event.record, path: "/another-source" } },

        { ...event, record: { ...event.record, revision: 2 } },
        { ...event, id: "forged" },
        { ...event, createdAt: "invalid" },
      ]) {
        const recovered = yield* makeContextRegistry({
          loadAll: () => [{ snapshot: committed, events: [corrupt] }],
          save: () => assert.fail("Recovery must not rewrite corruption"),
        }).pipe(Effect.result);
        assert.equal(recovered._tag, "Failure");
      }
    }),
  );
});
