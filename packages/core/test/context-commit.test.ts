import assert from "node:assert/strict";
import { test } from "node:test";
import { Effect, Schema, Stream } from "effect";
import {
  ContextCommitError,
  ContextConflict,
  ContextValidationError,
  defineContext,
  makeContextRegistry,
  type ContextRecord,
} from "../src/index.js";

const definition = defineContext({
  identity: "Versioned Context",
  state: Schema.Struct({ value: Schema.Number }),
  message: Schema.String,
});
const initial = { path: "/versioned", description: "Stable", state: { value: 1 }, messages: [] };

test("competing Context commits have one winner and stale identical writes conflict", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const saved: ContextRecord[] = [];
      const registry = yield* makeContextRegistry({
        loadAll: () => [],
        save: (record) => {
          saved.push(record);
        },
      });
      yield* registry.register(initial.path, definition);
      const results = yield* Effect.all(
        [
          registry.commit(initial, { expectedRevision: 0 }).pipe(Effect.result),
          registry
            .commit({ ...initial, state: { value: 2 } }, { expectedRevision: 0 })
            .pipe(Effect.result),
        ],
        { concurrency: "unbounded" },
      );
      assert.equal(results.filter((result) => result._tag === "Success").length, 1);
      const losing = results.find((result) => result._tag === "Failure");
      assert.ok(losing?._tag === "Failure" && losing.failure instanceof ContextConflict);
      assert.equal(saved.length, 1);
      const current = registry.get(initial.path)!;
      const stale = yield* registry.commit(current, { expectedRevision: 0 }).pipe(Effect.result);
      assert.ok(stale._tag === "Failure" && stale.failure instanceof ContextConflict);
      const unchanged = yield* registry.commit(current, { expectedRevision: 1 });
      assert.equal(unchanged.revision, 1);
      assert.equal(saved.length, 1);
      const invalid = yield* registry
        .commit({ ...current, state: {} }, { expectedRevision: 1 })
        .pipe(Effect.result);
      assert.ok(invalid._tag === "Failure" && invalid.failure instanceof ContextValidationError);
      assert.deepEqual(registry.get(initial.path), current);
      assert.equal(saved.length, 1);
    }),
  );
});

test("commit notifications follow durable writes; an uncertain storage failure fences this owner", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        let attempts = 0;
        const saved: ContextRecord[] = [];
        const registry = yield* makeContextRegistry({
          loadAll: () => [],
          save: (record) => {
            attempts++;
            if (attempts === 2) throw new Error("Crash after pending file commit");
            saved.push(record);
          },
        });
        yield* registry.register(initial.path, definition);
        const changes = yield* registry.subscribe;
        const observed: number[] = [];
        yield* Stream.runForEach(changes, (change) =>
          Effect.sync(() => {
            assert.deepEqual(saved.at(-1), change.record);
            observed.push(change.record.revision!);
          }),
        ).pipe(Effect.forkScoped);
        yield* registry.commit(initial, { expectedRevision: 0 });
        const failed = yield* registry
          .commit({ ...initial, messages: ["new"] }, { expectedRevision: 1 })
          .pipe(Effect.result);
        assert.ok(failed._tag === "Failure" && failed.failure instanceof ContextCommitError);
        const again = yield* registry
          .commit({ ...initial, state: { value: 3 } }, { expectedRevision: 1 })
          .pipe(Effect.result);
        assert.ok(again._tag === "Failure" && again.failure === failed.failure);
        yield* Effect.yieldNow;
        assert.equal(attempts, 2);
        assert.deepEqual(observed, [1]);
        assert.deepEqual(registry.get(initial.path), { ...initial, revision: 1 });
      }),
    ),
  );
});

test("legacy revision zero is upgraded on change and recovered revision guards the next writer", async () => {
  let persisted: ContextRecord = initial;
  const store = {
    loadAll: () => [persisted],
    save: (record: ContextRecord) => {
      persisted = record;
    },
  };
  await Effect.runPromise(
    Effect.gen(function* () {
      const registry = yield* makeContextRegistry(store);
      yield* registry.register(initial.path, definition);
      assert.equal((yield* registry.commit(initial, { expectedRevision: 0 })).revision, undefined);
      const first = yield* registry.commit(
        { ...initial, messages: ["first"] },
        { expectedRevision: 0 },
      );
      assert.equal(first.revision, 1);
      const recovered = yield* makeContextRegistry(store);
      yield* recovered.register(initial.path, definition);
      const second = yield* recovered.commit(
        { ...first, messages: ["first", "second"] },
        { expectedRevision: 1 },
      );
      assert.equal(second.revision, 2);
      const stale = yield* recovered.commit(first, { expectedRevision: 1 }).pipe(Effect.result);
      assert.ok(stale._tag === "Failure" && stale.failure instanceof ContextConflict);
      assert.deepEqual(persisted.messages, ["first", "second"]);
    }),
  );
});

test("owner restart reconciles a commit persisted before its acknowledgement failed", async () => {
  let persisted: ContextRecord = initial;
  let fail = true;
  await Effect.runPromise(
    Effect.gen(function* () {
      const registry = yield* makeContextRegistry({
        loadAll: () => [persisted],
        save: (record) => {
          persisted = structuredClone(record);
          if (fail) {
            fail = false;
            throw new Error("Lost commit acknowledgement");
          }
        },
      });
      yield* registry.register(initial.path, definition);
      const failed = yield* registry
        .commit({ ...initial, messages: ["accepted"] }, { expectedRevision: 0 })
        .pipe(Effect.result);
      assert.ok(failed._tag === "Failure" && failed.failure instanceof ContextCommitError);
      assert.deepEqual(registry.get(initial.path), initial);
      yield* registry.register(initial.path, definition);
      assert.deepEqual(registry.get(initial.path), persisted);
      const stale = yield* registry.commit(initial, { expectedRevision: 0 }).pipe(Effect.result);
      assert.ok(stale._tag === "Failure" && stale.failure instanceof ContextConflict);
      const next = yield* registry.commit(
        { ...persisted, messages: [...persisted.messages, "next"] },
        { expectedRevision: 1 },
      );
      assert.equal(next.revision, 2);
      assert.deepEqual(next.messages, ["accepted", "next"]);
    }),
  );
});

test("description initialization requires its observed revision and never overwrites newer content", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const registry = yield* makeContextRegistry();
      yield* registry.register(initial.path, definition);
      const first = yield* registry.commit(
        { ...initial, description: "" },
        { expectedRevision: 0 },
      );
      const newer = yield* registry.commit(
        { ...first, state: { value: 2 }, messages: ["new"] },
        { expectedRevision: 1 },
      );
      const conflict = yield* registry.describe(initial.path, "Identity", 1).pipe(Effect.flip);
      assert.equal(conflict._tag, "ContextConflict");
      assert.deepEqual(registry.get(initial.path), newer);
      yield* registry.describe(initial.path, "Identity", 2);
      assert.deepEqual(registry.get(initial.path), {
        ...newer,
        description: "Identity",
        revision: 3,
      });
    }),
  );
});
