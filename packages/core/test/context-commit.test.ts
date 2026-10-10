import assert from "node:assert/strict";
import { test } from "node:test";
import { Cause, Deferred, Effect, Exit, Fiber, Schema, Stream } from "effect";
import {
  ContextCommitError,
  makeDurableContext,
  type ContextText,
  type StoredContext,
} from "../src/index.js";
const options = {
  path: "/versioned",
  state: Schema.Struct({ value: Schema.Int }),
  message: Schema.String,
  serialize: (state: { value: number }) => String(state.value),
  events: "state" as const,
  initial: { description: "Counter", state: { value: 0 } },
};

test("competing typed Context transitions serialize and enforce observed revisions", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const writes: StoredContext[] = [];
        const durable = yield* makeDurableContext({
          load: Effect.succeed([]),
          save: (record) =>
            Effect.sync(() => {
              writes.push(record);
            }),
        });
        const context = yield* durable.open(options);
        const results = yield* Effect.all(
          [
            context.state.update(() => ({ value: 1 }), { expectedRevision: 1 }).pipe(Effect.result),
            context.state.update(() => ({ value: 2 }), { expectedRevision: 1 }).pipe(Effect.result),
          ],
          { concurrency: "unbounded" },
        );
        assert.equal(results.filter((value) => value._tag === "Success").length, 1);
        assert.equal(
          results.find((value) => value._tag === "Failure")?.failure._tag,
          "ContextConflict",
        );
        const before = yield* context.snapshot;
        yield* context.state.update((state) => state);
        assert.equal(writes.length, 2);
        assert.deepEqual(yield* context.snapshot, before);
      }),
    ),
  );
});

test("admitted persistence drains on interruption while waiting writers remain cancellable", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const observed = yield* Deferred.make<ContextText>();
        const writes: StoredContext[] = [];
        const durable = yield* makeDurableContext({
          load: Effect.succeed([]),
          save: (record) =>
            Effect.gen(function* () {
              if (record.snapshot.revision === 2) {
                yield* Deferred.succeed(entered, undefined);
                yield* Deferred.await(release);
              }
              writes.push(record);
            }),
        });
        const context = yield* durable.open(options);
        const changes = yield* durable.subscribe;
        yield* changes.pipe(
          Stream.take(1),
          Stream.runForEach((change) => Deferred.succeed(observed, change.record)),
          Effect.forkScoped,
        );
        const active = yield* context.state.update(() => ({ value: 1 })).pipe(Effect.forkScoped);
        yield* Deferred.await(entered);
        const waiter = yield* context.state.update(() => ({ value: 99 })).pipe(Effect.forkScoped);
        yield* Fiber.interrupt(waiter);
        const interrupt = yield* Fiber.interrupt(active).pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        assert.equal(durable.get(options.path)?.text, "0");
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(interrupt);
        assert.equal(writes.length, 2);
        assert.equal((yield* Deferred.await(observed)).text, "1");
        const exit = yield* Fiber.await(active);
        assert.ok(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause));
        assert.equal((yield* context.state.get).value, 1);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("slow writes to one Context do not block unrelated owners", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const durable = yield* makeDurableContext({
          load: Effect.succeed([]),
          save: (record) =>
            record.snapshot.path === "/slow" && record.snapshot.revision === 2
              ? Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release)))
              : Effect.void,
        });
        const slow = yield* durable.open({ ...options, path: "/slow" });
        const fast = yield* durable.open({ ...options, path: "/fast" });
        const writing = yield* slow.state.update(() => ({ value: 1 })).pipe(Effect.forkScoped);
        yield* Deferred.await(entered);
        yield* fast.state.update(() => ({ value: 1 }));
        assert.equal(durable.get("/fast")?.revision, 2);
        assert.equal(durable.get("/slow")?.revision, 1);
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(writing);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("uncertain commits fence the owner and recovery republishes only newly durable evidence", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        let saved: StoredContext | undefined;
        let crash = true;
        const durable = yield* makeDurableContext({
          load: Effect.sync(() => (saved ? [saved] : [])),
          save: (record) =>
            Effect.gen(function* () {
              saved = structuredClone(record);
              if (record.snapshot.revision === 2 && crash) {
                crash = false;
                return yield* new ContextCommitError({
                  path: options.path,
                  cause: new Error("Lost acknowledgement"),
                });
              }
            }),
        });
        yield* Effect.scoped(
          Effect.gen(function* () {
            const context = yield* durable.open(options);
            assert.equal(
              (yield* context
                .commit(() => ({ state: { value: 1 }, messages: { append: ["accepted"] } }))
                .pipe(Effect.flip))._tag,
              "ContextCommitError",
            );
            assert.equal(durable.get(options.path)?.revision, 1);
            assert.equal(
              (yield* context.state.update(() => ({ value: 2 })).pipe(Effect.flip))._tag,
              "ContextCommitError",
            );
          }),
        );
        const changes = yield* durable.subscribe;
        const restored = yield* durable.open(options);
        const recovered = yield* changes.pipe(Stream.take(1), Stream.runCollect);
        assert.equal(recovered[0]?.events?.length, 1);
        assert.equal((yield* restored.snapshot).revision, 2);
        assert.deepEqual(yield* restored.messages.list, ["accepted"]);
        yield* restored.messages.append(["accepted"]);
        assert.equal((yield* restored.snapshot).revision, 2);
      }),
    ),
  );
});

test("storage defects remain defects and invalid recovery keeps the owner fenced", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        let saved: StoredContext | undefined;
        let missing = false;
        const defect = new Error("disk defect");
        const durable = yield* makeDurableContext({
          load: Effect.sync(() => (saved && !missing ? [saved] : [])),
          save: (record) =>
            Effect.gen(function* () {
              saved = record;
              if (record.snapshot.revision === 2) return yield* Effect.die(defect);
            }),
        });
        yield* Effect.scoped(
          Effect.gen(function* () {
            const owner = yield* durable.open(options);
            const exit = yield* owner.state.update(() => ({ value: 1 })).pipe(Effect.exit);
            assert.ok(Exit.isFailure(exit));
            assert.equal(Cause.squash(exit.cause), defect);
          }),
        );
        missing = true;
        assert.equal(
          (yield* Effect.scoped(durable.open(options)).pipe(Effect.flip))._tag,
          "ContextRecoveryError",
        );
        missing = false;
        const recovered = yield* durable.open(options);
        assert.equal((yield* recovered.state.get).value, 1);
      }),
    ),
  );
});

test("failed initial bootstrap can reopen after recovery confirms no record was persisted", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const records: StoredContext[] = [];
        let reject = true;
        const durable = yield* makeDurableContext({
          load: Effect.sync(() => records),
          save: (record) =>
            Effect.gen(function* () {
              if (reject)
                return yield* new ContextCommitError({
                  path: record.snapshot.path,
                  cause: new Error("Unavailable"),
                });
              records.push(record);
            }),
        });
        assert.equal(
          (yield* Effect.scoped(durable.open(options)).pipe(Effect.flip))._tag,
          "ContextCommitError",
        );
        assert.equal(durable.get(options.path), undefined);
        reject = false;
        const owner = yield* durable.open(options);
        assert.equal((yield* owner.state.get).value, 0);
        assert.equal(records.length, 1);
        assert.equal(records[0]?.snapshot.revision, 1);
      }),
    ),
  );
});
