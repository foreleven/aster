import { serializeFixtureState } from "../src/testing/context.js";
import assert from "node:assert/strict";
import { test } from "node:test";
import { Effect, Schema } from "effect";
import { type StoredContext } from "../src/index.js";
import { makeTestContext } from "../src/testing/context.js";

const definition = {
  state: Schema.Struct({ summary: Schema.String, token: Schema.String }),
  message: Schema.String,
  events: "state" as const,
  serialize: (state: object) =>
    Schema.decodeUnknownSync(Schema.Struct({ summary: Schema.String }))(state).summary,
};
const initial = {
  text: serializeFixtureState({ summary: "first", token: "PRIVATE_SOURCE_TOKEN" }),
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
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeTestContext(store);
        yield* registry.register(initial.path, definition);
        const failed = yield* registry.commit(initial, { expectedRevision: 0 }).pipe(Effect.result);
        assert.equal(failed._tag, "Failure");
        assert.equal(registry.get(initial.path), undefined);
        assert.equal(persisted?.snapshot.revision, 1);
        const event = persisted?.events?.[0];
        assert.ok(event);
        assert.equal(event.record.revision, 1);
        assert.equal(JSON.stringify(event).includes("PRIVATE_SOURCE_TOKEN"), false);
        assert.equal(
          (persisted?.snapshot.state as { token: string }).token,
          "PRIVATE_SOURCE_TOKEN",
        );
        fail = false;
        // Same-process storage reconciliation preserves metadata even though domain validation omits it.
        yield* registry.register(initial.path, definition);
        yield* registry.commit(initial, { expectedRevision: 1 });
        assert.deepEqual(
          [...registry.records.values()].find((record) => record.snapshot.path === initial.path)
            ?.events,
          [event],
        );
        const restarted = yield* makeTestContext(store);
        yield* restarted.register(initial.path, definition);
        const current = restarted.get(initial.path)!;
        const forged = { ...current, events: [] };
        yield* restarted.commit(forged, { expectedRevision: 1 });
        assert.deepEqual(
          [...restarted.records.values()].find((record) => record.snapshot.path === initial.path)
            ?.events,
          [event],
        );
        const forgedNext = {
          ...initial,
          state: { ...initial.state, summary: "second" },
          events: [],
        };
        yield* restarted.commit(forgedNext, { expectedRevision: 1 });
        const next = [...restarted.records.values()].find(
          (record) => record.snapshot.path === initial.path,
        )!;
        assert.equal(next.events?.length, 2);
        assert.deepEqual(
          next.events?.map((item) => item.record.revision),
          [1, 2],
        );
        assert.equal(next.events![0]!.record.text, "first");
        assert.equal(next.events![1]!.record.text, "second");
        assert.equal("events" in restarted.backend.get(next.snapshot.path)!, false);
      }),
    ),
  );
});

test("bootstrap, message-only and description-only changes never create reaction work", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeTestContext();
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
          [...registry.records.values()].find((record) => record.snapshot.path === initial.path)
            ?.events?.length,
          1,
        );
      }),
    ),
  );
});

test("recovery rejects corrupted reaction envelopes instead of replaying another source", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeTestContext();
        yield* registry.register(initial.path, definition);
        const committed = yield* registry.commit(initial, { expectedRevision: 0 });
        const event = [...registry.records.values()][0]!.events![0]!;
        for (const corrupt of [
          { ...event, record: { ...event.record, path: "/another-source" } },

          { ...event, record: { ...event.record, revision: 2 } },
          { ...event, id: "forged" },
          { ...event, createdAt: "invalid" },
        ]) {
          const recovered = yield* makeTestContext({
            loadAll: () => [{ snapshot: committed, events: [corrupt] }],
            save: () => assert.fail("Recovery must not rewrite corruption"),
          }).pipe(Effect.result);
          assert.equal(recovered._tag, "Failure");
        }
      }),
    ),
  );
});
