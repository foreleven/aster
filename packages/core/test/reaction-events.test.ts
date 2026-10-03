import assert from "node:assert/strict";
import { test } from "node:test";
import { Effect, Schema } from "effect";
import {
  contextView,
  defineContext,
  makeContextRegistry,
  type ContextRecord,
} from "../src/index.js";

const definition = defineContext({
  identity: "Source",
  state: Schema.Struct({ summary: Schema.String, token: Schema.String }),
  message: Schema.String,
  signalSource: true,
  view: contextView({ state: Schema.Struct({ summary: Schema.String }), message: Schema.String }),
});
const initial = {
  path: "/source",
  description: "Source",
  state: { summary: "first", token: "PRIVATE_SOURCE_TOKEN" },
  messages: [],
};

test("source state and public reaction handoff commit together and survive uncertain acknowledgement", async () => {
  let persisted: ContextRecord | undefined;
  let fail = true;
  const store = {
    loadAll: () => (persisted ? [structuredClone(persisted)] : []),
    save: (record: ContextRecord) => {
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
      assert.equal(persisted?.revision, 1);
      const event = persisted?.reactionEvents?.[0];
      assert.ok(event);
      assert.equal(event.record.revision, 1);
      assert.equal(JSON.stringify(event).includes("PRIVATE_SOURCE_TOKEN"), false);
      assert.equal((persisted?.state as { token: string }).token, "PRIVATE_SOURCE_TOKEN");
      fail = false;
      // Same-process storage reconciliation preserves metadata even though domain validation omits it.
      yield* registry.register(initial.path, definition);
      assert.deepEqual(registry.get(initial.path)?.reactionEvents, [event]);
      const restarted = yield* makeContextRegistry(store);
      yield* restarted.register(initial.path, definition);
      const current = restarted.get(initial.path)!;
      yield* restarted.commit({ ...current, reactionEvents: [] }, { expectedRevision: 1 });
      assert.deepEqual(restarted.get(initial.path)?.reactionEvents, [event]);
      const next = yield* restarted.commit(
        { ...initial, state: { ...initial.state, summary: "second" }, reactionEvents: [] },
        { expectedRevision: 1 },
      );
      assert.equal(next.reactionEvents?.length, 2);
      assert.deepEqual(
        next.reactionEvents?.map((item) => item.record.revision),
        [1, 2],
      );
      assert.equal(
        (next.reactionEvents?.[0]?.record.state as { summary: string }).summary,
        "first",
      );
      assert.equal(
        (next.reactionEvents?.[1]?.record.state as { summary: string }).summary,
        "second",
      );
      assert.equal("reactionEvents" in restarted.project(next), false);
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
        { expectedRevision: 0, evaluate: false },
      );
      assert.equal(first.reactionEvents, undefined);
      yield* registry.describe(initial.path, "Source", 1);
      const message = yield* registry.commit(
        { ...registry.get(initial.path)!, messages: ["evidence"] },
        { expectedRevision: 2 },
      );
      assert.equal(message.reactionEvents, undefined);
      const changed = yield* registry.commit(
        { ...message, state: { ...initial.state, summary: "changed" } },
        { expectedRevision: 3 },
      );
      assert.equal(changed.reactionEvents?.length, 1);
      const stale = yield* registry.commit(initial, { expectedRevision: 3 }).pipe(Effect.result);
      assert.equal(stale._tag, "Failure");
      assert.equal(registry.get(initial.path)?.reactionEvents?.length, 1);
    }),
  );
});

test("recovery rejects corrupted reaction envelopes instead of replaying another source", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const registry = yield* makeContextRegistry();
      yield* registry.register(initial.path, definition);
      const committed = yield* registry.commit(initial, { expectedRevision: 0 });
      const event = committed.reactionEvents![0]!;
      for (const corrupt of [
        { ...event, source: "/another-source" },
        { ...event, revision: 2 },
        { ...event, record: { ...event.record, revision: 2 } },
        { ...event, requestId: "forged" },
        { ...event, causationId: "forged" },
      ]) {
        const recovered = yield* makeContextRegistry({
          loadAll: () => [{ ...committed, reactionEvents: [corrupt] }],
          save: () => assert.fail("Recovery must not rewrite corruption"),
        }).pipe(Effect.result);
        assert.equal(recovered._tag, "Failure");
      }
    }),
  );
});
