import assert from "node:assert/strict";
import { test } from "node:test";
import { Deferred, Effect, Fiber, Schema, Stream } from "effect";
import {
  makeDurableContext,
  ContextCommitError,
  ContextConflict,
  ContextValidationError,
  contextView,
  type StoredContext,
} from "../src/index.js";
import { makeContextRegistry } from "../src/testing/context.js";

const definition = {
  state: Schema.Struct({ value: Schema.Number }),
  message: Schema.String,
};
const initial = { path: "/versioned", description: "Stable", state: { value: 1 }, messages: [] };

test("competing Context commits have one winner and stale identical writes conflict", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const saved: StoredContext[] = [];
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
        const saved: StoredContext[] = [];
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
            assert.deepEqual(saved.at(-1)?.snapshot, change.record);
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

test("explicit revision zero is upgraded on change and recovered revision guards the next writer", async () => {
  let persisted: StoredContext = { snapshot: { ...initial, revision: 0 }, events: [] };
  const store = {
    loadAll: () => [persisted],
    save: (record: StoredContext) => {
      persisted = record;
    },
  };
  await Effect.runPromise(
    Effect.gen(function* () {
      const registry = yield* makeContextRegistry(store);
      yield* registry.register(initial.path, definition);
      assert.equal((yield* registry.commit(initial, { expectedRevision: 0 })).revision, 0);
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
      assert.deepEqual(persisted.snapshot.messages, ["first", "second"]);
    }),
  );
});

test("owner restart reconciles a commit persisted before its acknowledgement failed", async () => {
  let persisted: StoredContext = { snapshot: { ...initial, revision: 0 }, events: [] };
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
      assert.deepEqual(registry.get(initial.path), { ...initial, revision: 0 });
      yield* registry.register(initial.path, definition);
      assert.deepEqual(registry.get(initial.path), persisted.snapshot);
      const stale = yield* registry.commit(initial, { expectedRevision: 0 }).pipe(Effect.result);
      assert.ok(stale._tag === "Failure" && stale.failure instanceof ContextConflict);
      const next = yield* registry.commit(
        { ...persisted.snapshot, messages: [...persisted.snapshot.messages, "next"] },
        { expectedRevision: 1 },
      );
      assert.equal(next.revision, 2);
      assert.deepEqual(next.messages, ["accepted", "next"]);
    }),
  );
});

test("owner metadata commits require their observed revision and never overwrite newer content", async () => {
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
      const conflict = yield* registry
        .commit({ ...first, description: "Identity" }, { expectedRevision: 1 })
        .pipe(Effect.flip);
      assert.equal(conflict._tag, "ContextConflict");
      assert.deepEqual(registry.get(initial.path), newer);
      yield* registry.commit({ ...newer, description: "Identity" }, { expectedRevision: 2 });
      assert.deepEqual(registry.get(initial.path), {
        ...newer,
        description: "Identity",
        revision: 3,
      });
    }),
  );
});

test("a slow Context commit does not block another path; admitted writes publish only after persistence", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const backend = yield* makeDurableContext({
          load: Effect.succeed([]),
          save: (record) =>
            record.snapshot.path === "/slow"
              ? Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release)))
              : Effect.void,
        });
        const slow = yield* backend
          .commit({ ...initial, path: "/slow" }, { expectedRevision: 0 })
          .pipe(Effect.forkScoped);
        yield* Deferred.await(entered);
        yield* Effect.gen(function* () {
          const fast = yield* backend.commit(
            { ...initial, path: "/fast" },
            { expectedRevision: 0 },
          );
          assert.equal(fast.revision, 1);
          assert.equal(backend.get("/slow"), undefined);
          assert.equal(backend.get("/fast")?.revision, 1);
        }).pipe(Effect.timeout("2 seconds"), Effect.ensuring(Deferred.succeed(release, undefined)));
        yield* Fiber.join(slow);
        assert.equal(backend.get("/slow")?.revision, 1);
      }),
    ),
  );
});

test("owner commits update descriptions", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const registry = yield* makeContextRegistry();
      yield* registry.register(initial.path, definition);
      yield* registry.commit(initial, { expectedRevision: 0 });
      yield* registry.commit(
        { ...initial, description: "Updated by owner" },
        { expectedRevision: 1 },
      );
      assert.equal(registry.get(initial.path)?.description, "Updated by owner");
      assert.equal(registry.get(initial.path)?.revision, 2);
    }),
  );
});

test("uncertain source recovery republishes only newly durable events for live consumers", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        let stored: StoredContext | undefined;
        let loseAcknowledgement = false;
        const registry = yield* makeContextRegistry({
          loadAll: () => (stored ? [stored] : []),
          save: (record) => {
            stored = structuredClone(record);
            if (loseAcknowledgement) throw new Error("Commit acknowledgement lost");
          },
        });
        const source = {
          changes: "durable-state" as const,
          state: Schema.Struct({ value: Schema.Number }),
          message: Schema.String,
          view: contextView({ state: Schema.Struct({ value: Schema.Number }) }),
        };
        yield* registry.register(initial.path, source);
        yield* registry.commit(initial, { expectedRevision: 0 });
        const changes = yield* registry.subscribe;
        loseAcknowledgement = true;
        const failed = yield* registry
          .commit({ ...initial, state: { value: 2 } }, { expectedRevision: 1 })
          .pipe(Effect.result);
        assert.equal(failed._tag, "Failure");
        assert.equal(registry.get(initial.path)!.revision, 1);
        loseAcknowledgement = false;
        yield* registry.register(initial.path, source);
        const notifications = yield* changes.pipe(Stream.take(1), Stream.runCollect);
        assert.deepEqual(
          notifications[0]!.events?.map((event) => event.record.revision),
          [2],
        );
        assert.equal(notifications[0]!.record.revision, 2);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});
