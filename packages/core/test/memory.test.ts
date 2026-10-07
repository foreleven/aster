import { testConversations } from "./conversation-fixtures.js";
import { retainedTask } from "./task-fixtures.js";
import { ContextCaptures } from "../src/memory/capture.js";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem } from "@aster/actor";
import { Clock, Deferred, Effect, Fiber, Layer, Option, Queue, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";
import {
  ContextRegistry,
  defineContext,
  contextView,
  MemoryActor,
  MemoryBackend,
  MemoryCaptureError,
  type ContextCapture,
  type StoredContext,
} from "../src/index.js";
import { makeContextRegistry, type ContextStore } from "../src/testing/context.js";

const input: ContextCapture = {
  sessionId: "/goals/project/tasks/one:outcome:completed",
  records: [
    {
      revision: 0,
      path: "/goals/project/tasks/one",
      description: "Result",
      state: { status: "completed" },
      messages: ["Result"],
    },
  ],
};
const backend = (capture: MemoryBackend["Service"]["capture"]): MemoryBackend["Service"] => ({
  description: "Memory",
  retrieval: "bm25",
  capture,
  drain: Effect.void,
  recall: { search: () => Effect.succeed([]), expand: () => Effect.succeed([]) },
});
const Stored = Schema.Struct({
  pending: Schema.optional(
    Schema.Array(
      Schema.Struct({ sessionId: Schema.String, records: Schema.Array(Schema.Unknown) }),
    ),
  ),
  captured: Schema.optional(Schema.Array(Schema.String)),
});
const stored = (record: StoredContext) => Schema.decodeUnknownSync(Stored)(record.snapshot.state);
const harness = Effect.fnUntraced(function* (initial: readonly StoredContext[] = []) {
  const records = new Map(initial.map((record) => [record.snapshot.path, record]));
  const writes = yield* Queue.unbounded<StoredContext>();
  const store: ContextStore = {
    loadAll: () => structuredClone([...records.values()]),
    save: (record) => {
      records.set(record.snapshot.path, structuredClone(record));
      Effect.runSync(Queue.offer(writes, record));
    },
  };
  const committed = Effect.fnUntraced(function* (predicate: (record: StoredContext) => boolean) {
    while (true) {
      const record = yield* Queue.take(writes);
      if (predicate(record)) return record;
    }
  });
  return { records, store, committed };
});
const boot = Effect.fnUntraced(function* (
  store: ContextStore,
  impl: MemoryBackend["Service"],
  clock?: Clock.Clock,
) {
  const registry = yield* makeContextRegistry(store);
  const system = yield* ActorSystem.make().pipe(
    ActorSystem.provide(
      ContextCaptures.layer,
      Layer.succeed(ContextRegistry, registry),
      Layer.succeed(MemoryBackend, impl),
      ...(clock ? [Layer.succeed(Clock.Clock, clock)] : []),
    ),
  );
  const memory = yield* system.spawn("memory", MemoryActor);
  yield* memory.awaitStarted;
  return { registry, system, memory };
});
const run = <A, E>(effect: Effect.Effect<A, E, import("effect").Scope.Scope>) =>
  Effect.runPromise(Effect.scoped(effect).pipe(Effect.timeout("5 seconds")));

test("Memory persists admission before backend delivery and acknowledgement, then deduplicates", async () => {
  await run(
    Effect.gen(function* () {
      const h = yield* harness();
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let calls = 0;
      const { memory } = yield* boot(
        h.store,
        backend(() =>
          Effect.gen(function* () {
            calls++;
            assert.equal(
              stored(h.records.get("/memory")!).pending?.[0]?.sessionId,
              input.sessionId,
            );
            yield* Deferred.succeed(entered, undefined);
            yield* Deferred.await(release);
          }),
        ),
      );
      yield* memory.ask<void>((replyTo) => ({ _tag: "Capture", input, replyTo }));
      assert.equal(stored(h.records.get("/memory")!).pending?.length, 1);
      yield* Deferred.await(entered);
      yield* memory.ask<void>((replyTo) => ({ _tag: "Capture", input, replyTo }));
      assert.equal(calls, 1);
      yield* Deferred.succeed(release, undefined);
      yield* h.committed((record) => stored(record).captured?.includes(input.sessionId) === true);
      yield* memory.ask<void>((replyTo) => ({ _tag: "Capture", input, replyTo }));
      assert.equal(calls, 1);
      assert.deepEqual(stored(h.records.get("/memory")!).pending, []);
    }),
  );
});

test("failed captures stay durable and recover after restart", async () => {
  await run(
    Effect.gen(function* () {
      const h = yield* harness();
      const attempted = yield* Deferred.make<void>();
      let attempts = 0;
      yield* Effect.scoped(
        Effect.gen(function* () {
          const { memory } = yield* boot(
            h.store,
            backend(() =>
              Effect.gen(function* () {
                attempts++;
                yield* Deferred.succeed(attempted, undefined);
                return yield* new MemoryCaptureError({ message: "offline" });
              }),
            ),
          );
          yield* memory.ask<void>((replyTo) => ({ _tag: "Capture", input, replyTo }));
          yield* Deferred.await(attempted);
        }),
      );
      assert.equal(stored(h.records.get("/memory")!).pending?.length, 1);
      yield* Effect.scoped(
        Effect.gen(function* () {
          const { memory } = yield* boot(
            h.store,
            backend(() =>
              Effect.sync(() => {
                attempts++;
              }),
            ),
          );
          yield* h.committed(
            (record) => stored(record).captured?.includes(input.sessionId) === true,
          );
          yield* memory.ask<void>((replyTo) => ({ _tag: "Capture", input, replyTo }));
          assert.equal(attempts, 2);
        }),
      );
    }),
  );
});

test("Memory retries typed failures on its Clock and retains mailbox ownership", async () => {
  await run(
    Effect.gen(function* () {
      const h = yield* harness();
      const clock = yield* TestClock.make();
      const failed = yield* Deferred.make<void>();
      let attempts = 0;
      const { memory } = yield* boot(
        h.store,
        backend(() =>
          Effect.gen(function* () {
            attempts++;
            if (attempts === 1) {
              yield* Deferred.succeed(failed, undefined);
              return yield* new MemoryCaptureError({ message: "temporary" });
            }
          }),
        ),
        clock,
      );
      yield* memory.ask<void>((replyTo) => ({ _tag: "Capture", input, replyTo }));
      yield* Deferred.await(failed);
      // Let the failure command re-enter the mailbox before advancing the retry clock.
      yield* Effect.yieldNow;
      yield* memory.awaitStarted;
      yield* clock.adjust("30 seconds");
      yield* h.committed((record) => stored(record).captured?.includes(input.sessionId) === true);
      assert.equal(attempts, 2);
    }),
  );
});

test("recovered captures exclude private source data and Memory queue state stays private", async () => {
  await run(
    Effect.gen(function* () {
      const secret = "PRIVATE_CAPTURE_SENTINEL";
      const retained = yield* retainedTask(testConversations(), "completed");
      const pending = {
        sessionId: "retained",
        records: [
          {
            path: "/tasks/" + "a".repeat(64),
            revision: 1,
            description: "Result",
            state: {
              ...retained.snapshot.state,
              source: { token: secret },
            },
            messages: [{ type: "Completed", text: "Done", provider: secret }],
          },
        ],
      };
      const h = yield* harness([
        {
          snapshot: {
            path: "/memory",
            revision: 4,
            description: "Memory",
            state: { status: "ready", retrieval: "bm25", pending: [pending] },
            messages: [],
          },
          events: [],
        },
      ]);
      const captured = yield* Deferred.make<void>();
      const { registry } = yield* boot(
        h.store,
        backend((received) =>
          Effect.gen(function* () {
            assert.equal(JSON.stringify(received).includes(secret), false);
            assert.equal(
              Schema.decodeUnknownSync(Schema.Struct({ status: Schema.String }))(
                received.records[0]?.state,
              ).status,
              "completed",
            );
            yield* Deferred.succeed(captured, undefined);
          }),
        ),
      );
      yield* Deferred.await(captured);
      const view = registry.views.project(registry.get("/memory")!);
      assert.equal(view.projection?.visibility, "public");
      assert.equal("pending" in view.state, false);
    }),
  );
});

test("stopping a Memory Actor interrupts local work but retains pending recovery", async () => {
  await run(
    Effect.gen(function* () {
      const h = yield* harness();
      const started = yield* Deferred.make<void>();
      const stopped = yield* Deferred.make<void>();
      const { memory, system } = yield* boot(
        h.store,
        backend(() =>
          Deferred.succeed(started, undefined).pipe(
            Effect.andThen(Effect.never),
            Effect.ensuring(Deferred.succeed(stopped, undefined)),
          ),
        ),
      );
      yield* memory.ask<void>((replyTo) => ({ _tag: "Capture", input, replyTo }));
      yield* Deferred.await(started);
      yield* system.stop(memory);
      yield* Deferred.await(stopped);
      assert.equal(stored(h.records.get("/memory")!).pending?.length, 1);
      assert.equal(stored(h.records.get("/memory")!).captured?.length ?? 0, 0);
    }),
  );
});

test("backend defects reach Actor supervision without becoming capture success", async () => {
  await run(
    Effect.gen(function* () {
      const h = yield* harness();
      const registry = yield* makeContextRegistry(h.store);
      const release = yield* Deferred.make<void>();
      const system = yield* ActorSystem.make().pipe(
        ActorSystem.provide(
          ContextCaptures.layer,
          Layer.succeed(ContextRegistry, registry),
          Layer.succeed(
            MemoryBackend,
            backend(() =>
              Deferred.await(release).pipe(Effect.andThen(Effect.die("backend defect"))),
            ),
          ),
        ),
      );
      const memory = yield* system.spawn("memory", MemoryActor, { supervision: () => "stop" });
      yield* memory.awaitStarted;
      const stopped = yield* Stream.runHead(
        Stream.filter(
          system.events,
          (event) => event._tag === "ActorStopped" && event.path === memory.path,
        ),
      ).pipe(Effect.forkScoped);
      yield* memory.ask<void>((replyTo) => ({ _tag: "Capture", input, replyTo }));
      yield* Deferred.succeed(release, undefined);
      const event = yield* Fiber.join(stopped);
      assert.ok(Option.isSome(event));
      assert.equal(event.value._tag, "ActorStopped");
      assert.equal(stored(h.records.get("/memory")!).pending?.length, 1);
      assert.equal(stored(h.records.get("/memory")!).captured?.length ?? 0, 0);
    }),
  );
});

test("slow capture evidence never blocks recall and captured sessions skip subsequent reads", async () => {
  await run(
    Effect.gen(function* () {
      const h = yield* harness();
      const registry = yield* makeContextRegistry(h.store);
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const clock = yield* TestClock.make();
      let reads = 0;
      const captures = yield* ContextCaptures.pipe(Effect.provide(ContextCaptures.layer));
      yield* captures.register([
        {
          matches: (path) => path === "/source",
          capture: (record) => ({
            sessionId: "source",
            records: Effect.gen(function* () {
              reads++;
              yield* Deferred.succeed(entered, undefined);
              yield* Deferred.await(release);
              return [record];
            }),
          }),
        },
      ]);
      yield* registry.register(
        "/source",
        defineContext({
          state: Schema.Struct({ value: Schema.Int }),
          message: Schema.Never,
          view: contextView({ state: Schema.Struct({ value: Schema.Int }) }),
        }),
      );
      const system = yield* ActorSystem.make().pipe(
        ActorSystem.provide(
          Layer.succeed(ContextRegistry, registry),
          Layer.succeed(ContextCaptures, captures),
          Layer.succeed(
            MemoryBackend,
            backend(() => Effect.void),
          ),
          Layer.succeed(Clock.Clock, clock),
        ),
      );
      const memory = yield* system.spawn("memory", MemoryActor);
      yield* memory.awaitStarted;
      yield* registry.commit(
        { path: "/source", description: "Source", state: { value: 1 }, messages: [] },
        { expectedRevision: 0 },
      );
      yield* Deferred.await(entered);
      const cancelled = yield* Deferred.make<void>();
      const reply = yield* memory.ask((replyTo) => ({
        _tag: "Search",
        query: "hello",
        cancelled,
        replyTo,
      }));
      assert.deepEqual(reply, { _tag: "Success", value: [] });
      yield* Deferred.succeed(release, undefined);
      yield* h.committed(
        (record) =>
          record.snapshot.path === "/memory" &&
          stored(record).captured?.includes("source") === true,
      );
      yield* clock.adjust("30 seconds");
      // A later source notification and the reconciliation scan share the durable session identity.
      yield* registry.commit(
        { path: "/source", description: "Source", state: { value: 2 }, messages: [] },
        { expectedRevision: 1 },
      );
      yield* memory.ask((replyTo) => ({ _tag: "Search", query: "after", cancelled, replyTo }));
      assert.equal(reads, 1);
    }),
  );
});
