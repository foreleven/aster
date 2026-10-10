import assert from "node:assert/strict";
import { test } from "node:test";
import { Effect, Schema } from "effect";
import { makeDurableContext, type StoredContext } from "../src/index.js";
const options = {
  path: "/source",
  state: Schema.Struct({ summary: Schema.String, privateReceipt: Schema.String }),
  message: Schema.String,
  serialize: (state: { summary: string }) => state.summary,
  events: "state" as const,
  initial: {
    description: "Evidence",
    state: { summary: "first", privateReceipt: "private" },
    messages: ["pending private message"],
  },
};
test("generic readers and source events expose serialized State only", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const durable = yield* makeDurableContext({
          load: Effect.succeed([]),
          save: () => Effect.void,
        });
        const owner = yield* durable.open(options);
        assert.deepEqual(durable.get(options.path), {
          path: options.path,
          revision: 1,
          description: "Evidence",
          text: "first",
        });
        yield* owner.state.update((state) => ({
          ...state,
          privateReceipt: "other private receipt",
        }));
        assert.equal(durable.journal().length, 0);
        yield* owner.state.update((state) => ({ ...state, summary: "second" }));
        const event = durable.journal()[0]!;
        assert.equal(event.record.text, "second");
        assert.equal(JSON.stringify(event).includes("privateReceipt"), false);
        assert.equal(JSON.stringify(event).includes("pending private message"), false);
        yield* owner.state.update((state) => ({ ...state, summary: "bootstrap" }), {
          mode: "bootstrap",
        });
        assert.equal(durable.journal().length, 1);
      }),
    ),
  );
});
test("dormant Context serialization is durable and owner reopening refreshes formatter output", async () => {
  let stored: readonly StoredContext[] = [];
  const persistence = {
    load: Effect.sync(() => stored),
    save: (value: StoredContext) =>
      Effect.sync(() => {
        stored = [value];
      }),
  };
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const durable = yield* makeDurableContext(persistence);
        yield* durable.open(options);
      }),
    ),
  );
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const durable = yield* makeDurableContext(persistence);
        assert.equal(yield* durable.serializeState(options.path), "first");
        const owner = yield* durable.open({
          ...options,
          serialize: (state) => `Summary: ${state.summary}`,
        });
        assert.equal(yield* owner.serializeState, "Summary: first");
        assert.equal((yield* owner.snapshot).revision, 2);
        assert.equal(durable.journal().length, 0);
      }),
    ),
  );
});
test("a broken serializer preserves defects and does not accept partial State", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const durable = yield* makeDurableContext({
          load: Effect.succeed([]),
          save: () => Effect.void,
        });
        const owner = yield* durable.open({
          ...options,
          serialize: (state) => {
            if (state.summary === "broken") throw new Error("formatter defect");
            return state.summary;
          },
        });
        const before = yield* owner.snapshot;
        const exit = yield* owner.state
          .update((state) => ({ ...state, summary: "broken" }))
          .pipe(Effect.exit);
        assert.equal(exit._tag, "Failure");
        assert.deepEqual(yield* owner.snapshot, before);
      }),
    ),
  );
});
