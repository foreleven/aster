import assert from "node:assert/strict";
import { test } from "node:test";
import { Cause, Deferred, Effect, Exit, Fiber, Schema, Stream } from "effect";
import {
  ContextCommitError,
  ContextConflict,
  ContextRecoveryError,
  makeContextRegistryWithBackend,
  defineContext,
  type ContextSnapshot,
  type StoredContext,
} from "@aster/core";
import { makeDurableContext } from "@aster/core";

const initial: ContextSnapshot = {
  revision: 0,
  path: "/local",
  description: "Local state",
  state: { value: 1 },
  messages: [],
};
const validate = Schema.decodeUnknownSync(
  Schema.Struct({
    path: Schema.String,
    revision: Schema.optional(Schema.Number),
    description: Schema.String,
    state: Schema.Struct({ value: Schema.Number }),
    messages: Schema.Array(Schema.String),
  }),
);

test("LocalDurableContext drains an accepted commit on cancellation and cancels a waiting writer", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const observed = yield* Deferred.make<ContextSnapshot>();
        const saved: StoredContext[] = [];
        const backend = yield* makeDurableContext({
          load: Effect.succeed([]),
          save: (record) =>
            Effect.gen(function* () {
              yield* Deferred.succeed(entered, undefined);
              yield* Deferred.await(release);
              saved.push(structuredClone(record));
            }),
        });
        const changes = yield* backend.subscribe;
        yield* changes.pipe(
          Stream.take(1),
          Stream.runForEach((change) => Deferred.succeed(observed, change.record)),
          Effect.forkScoped,
        );
        const active = yield* backend
          .commit(initial, { expectedRevision: 0 })
          .pipe(Effect.forkScoped);
        yield* Deferred.await(entered);
        const waiter = yield* backend
          .commit({ ...initial, messages: ["must not start"] }, { expectedRevision: 1 })
          .pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        yield* Fiber.interrupt(waiter);
        const interrupt = yield* Fiber.interrupt(active).pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        assert.equal(backend.get(initial.path), undefined);
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(interrupt);
        const published = yield* Deferred.await(observed);
        assert.equal(saved.length, 1);
        assert.deepEqual(backend.get(initial.path), saved[0]?.snapshot);
        assert.deepEqual(published, saved[0]?.snapshot);
        assert.equal(published.revision, 1);
        const exit = yield* Fiber.await(active);
        assert.ok(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause));
        const next = yield* backend.commit(
          { ...initial, messages: ["next"] },
          { expectedRevision: 1 },
        );
        assert.equal(next.revision, 2);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("LocalDurableContext preserves storage defects and fences later commits until reconciliation", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const defect = new Error("Driver defect after durable commit");
      let persisted: StoredContext = { snapshot: initial, events: [] };
      let crash = true;
      const backend = yield* makeDurableContext({
        load: Effect.sync(() => [persisted]),
        save: (record) =>
          Effect.gen(function* () {
            persisted = structuredClone(record);
            if (crash) {
              crash = false;
              return yield* Effect.die(defect);
            }
          }),
      });
      const failed = yield* backend
        .commit({ ...initial, messages: ["accepted"] }, { expectedRevision: 0 })
        .pipe(Effect.exit);
      assert.ok(Exit.isFailure(failed));
      assert.equal(Cause.squash(failed.cause), defect);
      const fenced = yield* backend
        .commit({ ...initial, messages: ["other"] }, { expectedRevision: 0 })
        .pipe(Effect.result);
      assert.ok(fenced._tag === "Failure" && fenced.failure instanceof ContextCommitError);
      assert.deepEqual(backend.get(initial.path), { ...initial, revision: 0 });
      yield* backend.recover(initial.path, validate);
      assert.deepEqual(backend.get(initial.path), persisted.snapshot);
      const stale = yield* backend.commit(initial, { expectedRevision: 0 }).pipe(Effect.result);
      assert.ok(stale._tag === "Failure" && stale.failure instanceof ContextConflict);
      assert.equal(
        (yield* backend.commit(
          { ...persisted.snapshot, messages: ["accepted", "next"] },
          { expectedRevision: 1 },
        )).revision,
        2,
      );
    }),
  );
});

test("recovery refuses missing, regressed and invalid snapshots without releasing the storage fence", async () => {
  for (const restored of [
    [],
    [{ ...initial, revision: 0 }],
    [{ ...initial, revision: 2, state: { value: "invalid" } }],
  ]) {
    await Effect.runPromise(
      Effect.gen(function* () {
        let saved: readonly StoredContext[] = [
          { snapshot: { ...initial, revision: 1 }, events: [] },
        ];
        const backend = yield* makeDurableContext({
          load: Effect.sync(() => saved),
          save: () =>
            Effect.fail(
              new ContextCommitError({ path: initial.path, cause: new Error("Unknown write") }),
            ),
        });
        yield* backend
          .commit({ ...initial, messages: ["pending"] }, { expectedRevision: 1 })
          .pipe(Effect.result);
        saved = restored.map((snapshot) => ({ snapshot, events: [] }));
        const recovery = yield* backend.recover(initial.path, validate).pipe(Effect.result);
        assert.ok(recovery._tag === "Failure" && recovery.failure instanceof ContextRecoveryError);
        assert.equal(backend.get(initial.path)?.revision, 1);
        const fenced = yield* backend.commit(initial, { expectedRevision: 1 }).pipe(Effect.result);
        assert.ok(fenced._tag === "Failure" && fenced.failure instanceof ContextCommitError);
      }),
    );
  }
});

test("the registry validates domain state while a supplied DurableContext owns canonical commits", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const writes: StoredContext[] = [];
      const backend = yield* makeDurableContext({
        load: Effect.succeed([]),
        save: (record) =>
          Effect.sync(() => {
            writes.push(record);
          }),
      });
      const registry = makeContextRegistryWithBackend(backend);
      yield* registry.register(
        initial.path,
        defineContext({
          state: Schema.Struct({ value: Schema.Number }),
          message: Schema.String,
        }),
      );
      yield* registry.commit(
        { ...initial, state: { value: 2, privateField: "not public" } },
        { expectedRevision: 0 },
      );
      assert.deepEqual(writes[0]?.snapshot.state, { value: 2 });
      assert.deepEqual(registry.snapshot(), backend.snapshot());
    }),
  );
});

test("LocalDurableContext rejects duplicate identities and invalid public paths on open", async () => {
  for (const records of [
    [initial, initial],
    [{ ...initial, path: "/../outside" }],
    [{ ...initial, path: "/trailing/" }],
  ]) {
    const result = await Effect.runPromise(
      makeDurableContext({
        load: Effect.succeed(records.map((snapshot) => ({ snapshot, events: [] }))),
        save: () => Effect.void,
      }).pipe(Effect.result),
    );
    assert.ok(result._tag === "Failure" && result.failure instanceof ContextRecoveryError);
  }
});
