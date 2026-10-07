import assert from "node:assert/strict";
import { test } from "node:test";
import { Effect, Fiber, Schema, Stream } from "effect";
import {
  type StoredContext,
  childActorName,
  childContextPath,
  defineContext,
  makeContextMaintenance,
} from "../src/index.js";
import { makeContextRegistry } from "../src/testing/context.js";

const definition = defineContext({
  state: Schema.Struct({ value: Schema.Number }),
  message: Schema.String,
});

test("virtual Context path segments map to a direct Actor name", () => {
  assert.equal(childContextPath("/lark/mail", "me/message-1"), "/lark/mail/me/message-1");
  assert.equal(childActorName("me/message-1").includes("/"), false);
  assert.equal(childActorName("email"), "email");
});

test("only schema fields notify automatically and owner snapshots are detached", async () => {
  const result = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        yield* registry.register("/x", definition);
        const events: unknown[] = [];
        const listener = yield* Stream.runForEach(registry.changes, (event) =>
          Effect.sync(() => {
            events.push(event);
          }),
        ).pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        const state = { value: 1, cursor: "private" };
        const input = {
          path: "/x",
          description: "My test",
          state,
          messages: ["one"],
          token: "private",
        };
        yield* registry.commit(input, {
          expectedRevision: registry.get(input.path)?.revision ?? 0,
        });
        state.value = 9;
        const copy = registry.snapshot();
        (copy["/x"]!.state as { value: number }).value = 99;
        yield* registry.commit(
          {
            ...input,
            state: { value: 1, cursor: "another cursor" },
          },
          { expectedRevision: registry.get(input.path)?.revision ?? 0 },
        );
        yield* Effect.sleep(10);
        yield* Fiber.interrupt(listener);
        return { events, record: registry.get("/x") };
      }),
    ),
  );
  assert.deepEqual(result.record, {
    path: "/x",
    revision: 1,
    description: "My test",
    state: { value: 1 },
    messages: ["one"],
  });
  assert.deepEqual(result.events, [{ record: result.record }]);
});

test("dynamic description initializes once and preserves content updated during Agent work", async () => {
  const record = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        yield* registry.register("/x", definition);
        yield* registry.commit(
          { path: "/x", description: "", state: { value: 1 }, messages: [] },
          { expectedRevision: registry.get("/x")?.revision ?? 0 },
        );
        yield* registry.commit(
          {
            path: "/x",
            description: "",
            state: { value: 2 },
            messages: ["new"],
          },
          { expectedRevision: registry.get("/x")?.revision ?? 0 },
        );
        yield* registry.initializeDescription(
          "/x",
          "Fixed identity",
          registry.get("/x")?.revision ?? 0,
        );
        yield* registry.initializeDescription(
          "/x",
          "Must not overwrite",
          registry.get("/x")?.revision ?? 0,
        );
        return registry.get("/x");
      }),
    ),
  );
  assert.equal(record?.description, "Fixed identity");
  assert.deepEqual(record?.state, { value: 2 });
  assert.deepEqual(record?.messages, ["new"]);
});

test("invalid public state and unregistered Context paths are rejected", async () => {
  for (const record of [
    { path: "/x", description: "x", state: [], messages: [] },
    { path: "/missing", description: "x", state: {}, messages: [] },
  ]) {
    await assert.rejects(
      Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const registry = yield* makeContextRegistry();
            yield* registry.register("/x", definition);
            yield* registry.commit(record, {
              expectedRevision: registry.get(record.path)?.revision ?? 0,
            });
          }),
        ),
      ),
    );
  }
});

test("every changed commit publishes its detached revision", async () => {
  const changes = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        yield* registry.register("/x", definition);
        const changes: number[] = [];
        yield* Stream.runForEach(registry.changes, (change) =>
          Effect.sync(() => {
            changes.push(change.record.revision);
          }),
        ).pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        const record = {
          path: "/x",
          description: "",
          state: { value: 1 },
          messages: [] as string[],
        };
        yield* registry.commit(record, {
          expectedRevision: registry.get(record.path)?.revision ?? 0,
        });
        yield* registry.commit(
          { ...record, messages: ["new message"] },
          { expectedRevision: registry.get(record.path)?.revision ?? 0 },
        );
        yield* registry.initializeDescription(
          "/x",
          "Fixed description",
          registry.get("/x")?.revision ?? 0,
        );
        yield* registry.commit(
          { ...registry.get("/x")!, state: { value: 2 } },
          { expectedRevision: registry.get("/x")?.revision ?? 0 },
        );
        yield* Effect.sleep(10);
        return changes;
      }),
    ),
  );
  assert.deepEqual(changes, [1, 2, 3, 4]);
});

test("storage adapters cannot mutate the registry through loaded or saved record references", async () => {
  const loaded = { path: "/x", description: "x", state: { value: 1 }, messages: [] };
  let saved: StoredContext | undefined;
  const registry = await Effect.runPromise(
    makeContextRegistry({
      loadAll: () => [{ snapshot: { ...loaded, revision: 0 }, events: [] }],
      save: (record) => {
        saved = record;
      },
    }),
  );
  loaded.state.value = 99;
  assert.deepEqual(registry.get("/x")?.state, { value: 1 });
  await Effect.runPromise(registry.register("/x", definition));
  await Effect.runPromise(
    registry.commit(
      { ...loaded, state: { value: 2 } },
      { expectedRevision: registry.get(loaded.path)?.revision ?? 0 },
    ),
  );
  (saved!.snapshot.state as { value: number }).value = 100;
  assert.deepEqual(registry.get("/x")?.state, { value: 2 });
});

test("memory handoff failures remain retryable; the durable sink owns deduplication", async () => {
  let attempts = 0;
  await Effect.runPromise(
    Effect.gen(function* () {
      const registry = yield* makeContextRegistry();
      yield* registry.register("/x", definition);
      const record = { path: "/x", description: "x", state: { value: 1 }, messages: [] };
      yield* registry.commit(record, {
        expectedRevision: registry.get(record.path)?.revision ?? 0,
      });
      const process = makeContextMaintenance({
        registry,
        capture: () =>
          Effect.sync(() => {
            if (++attempts === 1) throw new Error("capture handoff failed");
          }),
        captures: {
          select: (record) => Effect.succeed({ sessionId: "session", records: [record] }),
        },
        descriptions: { identity: () => undefined },
        describe: () => Effect.succeed("unused"),
      });
      const change = { record: registry.get("/x")! };
      assert.equal((yield* Effect.exit(process(change)))._tag, "Failure");
      yield* process(change);
      yield* process(change);
      assert.equal(attempts, 3);
    }),
  );
});
