import assert from "node:assert/strict";
import { test } from "node:test";
import { Effect, Fiber, Schema, Stream } from "effect";
import {
  childActorName,
  childContextPath,
  defineContext,
  makeContextRegistry,
  makeContextProcessor,
} from "../src/index.js";

const definition = defineContext({
  identity: "Test Context",
  state: Schema.Struct({ value: Schema.Number }),
  message: Schema.String,
});

test("virtual Context path segments map to a direct Actor name", () => {
  assert.equal(childContextPath("/lark/mail", "me/message-1"), "/lark/mail/me/message-1");
  assert.equal(childActorName("me/message-1").includes("/"), false);
  assert.equal(childActorName("email"), "email");
});

test("only public Schema fields notify automatically; snapshots and descriptions are isolated", async () => {
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
        yield* registry.set(input);
        state.value = 9;
        const copy = registry.snapshot();
        (copy["/x"]!.state as { value: number }).value = 99;
        yield* registry.set({
          ...input,
          description: "changed",
          state: { value: 1, cursor: "another cursor" },
        });
        yield* Effect.sleep(10);
        yield* Fiber.interrupt(listener);
        return { events, record: registry.get("/x") };
      }),
    ),
  );
  assert.deepEqual(result.record, {
    path: "/x",
    description: "My test",
    state: { value: 1 },
    messages: ["one"],
  });
  assert.deepEqual(result.events, [
    { path: "/x", created: true, stateChanged: true, record: result.record },
  ]);
});

test("dynamic description initializes once and preserves content updated during Agent work", async () => {
  const record = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        yield* registry.register("/x", definition);
        yield* registry.set({ path: "/x", description: "", state: { value: 1 }, messages: [] });
        yield* registry.set({
          path: "/x",
          description: "",
          state: { value: 2 },
          messages: ["new"],
        });
        yield* registry.describe("/x", "Fixed identity");
        yield* registry.describe("/x", "Must not overwrite");
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
            yield* registry.set(record);
          }),
        ),
      ),
    );
  }
});

test("message-only and description-only updates do not mark state as changed", async () => {
  const changes = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        yield* registry.register("/x", definition);
        const changes: boolean[] = [];
        yield* Stream.runForEach(registry.changes, (change) =>
          Effect.sync(() => {
            changes.push(change.stateChanged);
          }),
        ).pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        const record = {
          path: "/x",
          description: "",
          state: { value: 1 },
          messages: [] as string[],
        };
        yield* registry.set(record);
        yield* registry.set({ ...record, messages: ["new message"] });
        yield* registry.describe("/x", "Fixed description");
        yield* registry.set({ ...registry.get("/x")!, state: { value: 2 } });
        yield* Effect.sleep(10);
        return changes;
      }),
    ),
  );
  assert.deepEqual(changes, [true, false, false, true]);
});

test("storage adapters cannot mutate the registry through loaded or saved record references", async () => {
  const loaded = { path: "/x", description: "x", state: { value: 1 }, messages: [] };
  let saved: typeof loaded | undefined;
  const registry = await Effect.runPromise(
    makeContextRegistry({
      loadAll: () => [loaded],
      save: (record) => {
        saved = record as typeof loaded;
      },
    }),
  );
  loaded.state.value = 99;
  assert.deepEqual(registry.get("/x")?.state, { value: 1 });
  await Effect.runPromise(registry.register("/x", definition));
  await Effect.runPromise(registry.set({ ...loaded, state: { value: 2 } }));
  saved!.state.value = 100;
  assert.deepEqual(registry.get("/x")?.state, { value: 2 });
});

test("a failed memory handoff is retried on the next change before capture deduplication", async () => {
  let attempts = 0;
  await Effect.runPromise(
    Effect.gen(function* () {
      const registry = yield* makeContextRegistry();
      yield* registry.register("/x", {
        ...definition,
        capture: (record) => ({ sessionId: "session", records: [record] }),
      });
      const record = { path: "/x", description: "x", state: { value: 1 }, messages: [] };
      yield* registry.set(record);
      const process = makeContextProcessor(
        registry,
        () =>
          Effect.sync(() => {
            if (++attempts === 1) throw new Error("capture handoff failed");
          }),
        () => Effect.void,
        () => Effect.sync(() => "unused"),
      );
      const change = { path: "/x", record, created: false, stateChanged: true };
      assert.equal((yield* Effect.exit(process(change)))._tag, "Failure");
      yield* process(change);
      yield* process(change);
      assert.equal(attempts, 2);
    }),
  );
});
