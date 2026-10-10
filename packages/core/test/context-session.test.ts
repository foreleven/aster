import assert from "node:assert/strict";
import { test } from "node:test";
import { Effect, Fiber, Option, Queue, Schema, Stream } from "effect";
import {
  ContextClosed,
  ContextConflict,
  ContextMessageConflict,
  makeDurableContext,
} from "../src/index.js";
const definition = {
  path: "/test/context",
  state: Schema.Struct({ count: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)) }),
  message: Schema.Struct({ id: Schema.String, text: Schema.String }),
  keyOf: (message: { id: string }) => message.id,
  serialize: (state: { count: number }) => `Count: ${state.count}`,
  events: "state" as const,
  initial: { description: "Counter", state: { count: 0 } },
};
const setup = () => makeDurableContext({ load: Effect.succeed([]), save: () => Effect.void });

test("typed Context handles append in order, reject updates, and delete without retaining identities", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const durable = yield* setup();
        const context = yield* durable.open(definition);
        const messages = [
          { id: "z", text: "first" },
          { id: "2", text: "second" },
          { id: "__proto__", text: "third" },
        ];
        yield* context.messages.append(messages);
        assert.deepEqual(yield* context.messages.list, messages);
        const revision = (yield* context.snapshot).revision;
        yield* context.messages.append([messages[0]!]);
        assert.equal((yield* context.snapshot).revision, revision);
        assert.ok(
          (yield* context.messages
            .append([{ id: "z", text: "changed" }])
            .pipe(Effect.flip)) instanceof ContextMessageConflict,
        );
        assert.equal(
          (yield* context.messages.append([messages[0]!, messages[0]!]).pipe(Effect.flip))._tag,
          "ContextValidationError",
        );
        yield* context.commit((current) => ({
          state: { count: current.state.count + 1 },
          messages: { remove: ["z", "missing"] },
        }));
        assert.deepEqual(yield* context.messages.get("z"), Option.none());
        assert.equal(yield* context.serializeState, "Count: 1");
        assert.equal(durable.journal().length, 1);
        yield* context.messages.append([{ id: "z", text: "reused after deletion" }]);
        assert.equal((yield* context.messages.list).at(-1)?.text, "reused after deletion");
        const before = yield* context.snapshot;
        assert.equal(
          (yield* context
            .commit(() => ({ state: { count: -1 }, messages: { remove: ["z"] } }))
            .pipe(Effect.flip))._tag,
          "ContextValidationError",
        );
        assert.deepEqual(yield* context.snapshot, before);
        assert.ok(
          (yield* context.state
            .update((state) => state, { expectedRevision: 0 })
            .pipe(Effect.flip)) instanceof ContextConflict,
        );
        assert.equal(
          (yield* context
            .commit(() => ({ messages: { append: [messages[0]!], remove: ["z"] } }))
            .pipe(Effect.flip))._tag,
          "ContextValidationError",
        );
      }),
    ),
  );
});

test("Context watches hydrate at consumption, publish committed values and terminate on retirement", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const durable = yield* setup();
        const context = yield* durable.open(definition);
        const beforeAppend = context.messages.watch("m");
        yield* context.messages.append([{ id: "m", text: "accepted" }]);
        const values = yield* Queue.unbounded<Option.Option<{ id: string; text: string }>>();
        const watcher = yield* beforeAppend.pipe(
          Stream.runForEach((value) => Queue.offer(values, value)),
          Effect.forkScoped,
        );
        assert.deepEqual(yield* Queue.take(values), Option.some({ id: "m", text: "accepted" }));
        yield* context.commit(() => ({ state: { count: 1 }, messages: { remove: ["m"] } }));
        assert.deepEqual(yield* Queue.take(values), Option.none());
        yield* Fiber.join(watcher);
        assert.deepEqual(yield* context.messages.watch("absent").pipe(Stream.runCollect), [
          Option.none(),
        ]);
        assert.deepEqual(yield* context.state.watch.pipe(Stream.take(1), Stream.runCollect), [
          { count: 1 },
        ]);
      }),
    ),
  );
});

test("owner leases drain scoped work and revoke retired handles", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const durable = yield* setup();
        const old = yield* Effect.scoped(
          Effect.gen(function* () {
            const owner = yield* durable.open(definition);
            assert.equal(
              (yield* Effect.scoped(durable.open(definition)).pipe(Effect.flip))._tag,
              "ContextRecoveryError",
            );
            yield* Effect.forEach(
              Array.from({ length: 10 }),
              () => owner.state.update((state) => ({ count: state.count + 1 })),
              { concurrency: "unbounded" },
            );
            return owner;
          }),
        );
        const next = yield* durable.open(definition);
        assert.equal((yield* next.state.get).count, 10);
        assert.ok(
          (yield* old.state.update(() => ({ count: 99 })).pipe(Effect.flip)) instanceof
            ContextClosed,
        );
      }),
    ),
  );
});

test("daily Contexts reject invalid dates, timezones and unqualified paths", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const durable = yield* setup();
        for (const persistence of [
          { layout: "daily" as const, date: "2026-02-30", timeZone: "Asia/Shanghai" },
          { layout: "daily" as const, date: "2026-10-09", timeZone: "invalid" },
          { layout: "daily" as const, date: "2026-10-09", timeZone: "Asia/Shanghai" },
        ])
          assert.equal(
            (yield* Effect.scoped(durable.open({ ...definition, persistence })).pipe(Effect.flip))
              ._tag,
            "ContextValidationError",
          );
      }),
    ),
  );
});

test("nullable messages remain present until removed", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const durable = yield* setup();
        const context = yield* durable.open({
          ...definition,
          message: Schema.Null,
          keyOf: () => "null",
        });
        yield* context.messages.append([null]);
        assert.deepEqual(yield* context.messages.get("null"), Option.some(null));
        const values = yield* Queue.unbounded<Option.Option<null>>();
        const watcher = yield* context.messages.watch("null").pipe(
          Stream.runForEach((value) => Queue.offer(values, value)),
          Effect.forkScoped,
        );
        assert.deepEqual(yield* Queue.take(values), Option.some(null));
        yield* context.messages.remove(["null"]);
        assert.deepEqual(yield* Queue.take(values), Option.none());
        yield* Fiber.join(watcher);
      }),
    ),
  );
});

test("committed owner reads are detached, schema checked and independent of writer ownership", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        let saves = 0;
        const durable = yield* makeDurableContext({
          load: Effect.succeed([]),
          save: () =>
            Effect.sync(() => {
              saves++;
            }),
        });
        assert.equal(
          yield* durable.readOwnerSnapshot(definition.path, {
            state: definition.state,
            message: definition.message,
          }),
          undefined,
        );
        const owner = yield* durable.open(definition);
        yield* owner.commit(() => ({
          state: { count: 7 },
          messages: { append: [{ id: "m", text: "committed" }] },
        }));
        const before = saves;
        const read = yield* durable.readOwnerSnapshot(definition.path, {
          state: definition.state,
          message: definition.message,
        });
        assert.deepEqual(read, yield* owner.snapshot);
        Object.assign(read!.state, { count: 99 });
        assert.equal(
          (yield* durable.readOwnerSnapshot(definition.path, {
            state: definition.state,
            message: definition.message,
          }))?.state.count,
          7,
        );
        const invalid = yield* durable
          .readOwnerSnapshot(definition.path, {
            state: Schema.Struct({ missing: Schema.Boolean }),
            message: definition.message,
          })
          .pipe(Effect.flip);
        assert.equal(invalid._tag, "ContextValidationError");
        assert.equal(saves, before);
      }),
    ),
  );
});
