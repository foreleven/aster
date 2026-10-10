import assert from "node:assert/strict";
import { test } from "node:test";
import { mkdtempSync, readdirSync, readFileSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Effect, Fiber, Option, Queue, Schema, Stream } from "effect";
import { ContextCommitError, makeDurableContext } from "@aster/core";

import { makeContextSessionPersistence } from "../src/storage/context-sessions.js";
const definition = {
  state: Schema.Struct({ count: Schema.Number }),
  message: Schema.Struct({ id: Schema.String, text: Schema.String }),
  keyOf: (message: { id: string }) => message.id,
  serialize: (state: { count: number }) => `Count: ${state.count}`,
};
const totalBytes = (root: string) =>
  readdirSync(root, { recursive: true, withFileTypes: true })
    .filter((file) => file.isFile())
    .reduce((sum, file) => sum + statSync(join(file.parentPath, file.name)).size, 0);
const setup = (root: string) =>
  makeContextSessionPersistence(join(root, "sessions")).pipe(Effect.flatMap(makeDurableContext));

test("message family append, retirement, key reuse and State checkpoints survive native restart", async (t) => {
  const root = mkdtempSync(join(tmpdir(), "aster-context-family-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const initial = {
    description: "Test",
    state: { count: 0 },
    messages: Array.from({ length: 1000 }, (_, i) => ({ id: `m${i}`, text: "x".repeat(200) })),
  };
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const durable = yield* setup(root);
        const context = yield* durable.open({ path: "/chat", ...definition, initial });
        const before = totalBytes(join(root, "sessions"));
        yield* context.messages.append([{ id: "new", text: "new" }]);
        assert.ok(
          totalBytes(join(root, "sessions")) - before < 20000,
          "append must not rewrite unchanged message bodies",
        );
        yield* context.messages.append([{ id: "__proto__", text: "safe" }]);
        yield* context.messages.remove(initial.messages.map((message) => message.id));
        yield* context.messages.remove(["new"]);
        yield* context.messages.append([{ id: "new", text: "reused key" }]);
        for (let i = 0; i < 70; i++)
          yield* context.state.update((state) => ({ count: state.count + 1 }));
      }),
    ),
  );
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const durable = yield* setup(root);
        assert.equal(yield* durable.serializeState("/chat"), "Count: 70");
        const context = yield* durable.open({ path: "/chat", ...definition, initial });
        assert.deepEqual(yield* context.messages.list, [
          { id: "__proto__", text: "safe" },
          { id: "new", text: "reused key" },
        ]);
        assert.equal((yield* context.state.get).count, 70);
      }),
    ),
  );
  const lines = readdirSync(join(root, "sessions"), { recursive: true, withFileTypes: true })
    .filter((file) => file.isFile() && file.name === "main.jsonl")
    .flatMap((file) => readFileSync(join(file.parentPath, file.name), "utf8").trim().split("\n"));
  assert.ok(lines.length > 70);
  assert.ok(lines.every((line) => !line.includes('"entry.append"')));
  assert.ok(lines.some((line) => line.includes("aster.context.message")));
});

test("family message watches hydrate, retire and complete without observing a later incarnation", async (t) => {
  const root = mkdtempSync(join(tmpdir(), "aster-context-watch-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const durable = yield* setup(root);
        const context = yield* durable.open({
          path: "/watch",
          ...definition,
          initial: {
            description: "Watch",
            state: { count: 0 },
            messages: [{ id: "m", text: "one" }],
          },
        });
        const values = yield* Queue.unbounded<Option.Option<{ id: string; text: string }>>();
        const watching = yield* context.messages.watch("m").pipe(
          Stream.runForEach((value) => Queue.offer(values, value)),
          Effect.forkScoped,
        );
        assert.deepEqual(yield* Queue.take(values), Option.some({ id: "m", text: "one" }));
        yield* context.messages.remove(["m"]);
        assert.deepEqual(yield* Queue.take(values), Option.none());
        yield* Fiber.join(watching);
        yield* context.messages.append([{ id: "m", text: "two" }]);
        assert.deepEqual(yield* context.messages.get("m"), Option.some({ id: "m", text: "two" }));
      }),
    ),
  );
});

test("daily partitions remain independent and reject changed timezone after restart", async (t) => {
  const root = mkdtempSync(join(tmpdir(), "aster-context-daily-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const initial = { description: "Daily", state: { count: 0 } };
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const durable = yield* setup(root);
        const first = yield* durable.open({
          path: "/days/2026-10-09",
          ...definition,
          initial,
          persistence: { layout: "daily", date: "2026-10-09", timeZone: "Asia/Shanghai" },
        });
        const next = yield* durable.open({
          path: "/days/2026-10-10",
          ...definition,
          initial,
          persistence: { layout: "daily", date: "2026-10-10", timeZone: "Asia/Shanghai" },
        });
        yield* first.state.update(() => ({ count: 9 }));
        assert.equal((yield* next.state.get).count, 0);
      }),
    ),
  );
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const durable = yield* setup(root);
        const error = yield* durable
          .open({
            path: "/days/2026-10-09",
            ...definition,
            initial,
            persistence: { layout: "daily", date: "2026-10-09", timeZone: "UTC" },
          })
          .pipe(Effect.flip);
        assert.equal(error._tag, "ContextRecoveryError");
      }),
    ),
  );
});

test("native uncertain commit recovers State, Messages and serialized text with its original revision", async (t) => {
  const root = mkdtempSync(join(tmpdir(), "aster-context-uncertain-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const persistence = yield* makeContextSessionPersistence(join(root, "sessions"));
        let crash = true;
        const durable = yield* makeDurableContext({
          ...persistence,
          save: (record, owner) =>
            persistence.save(record, owner).pipe(
              Effect.andThen(
                Effect.suspend(() => {
                  if (record.snapshot.revision !== 2 || !crash) return Effect.void;
                  crash = false;
                  return Effect.fail(
                    new ContextCommitError({
                      path: record.snapshot.path,
                      cause: new Error("Lost commit acknowledgement"),
                    }),
                  );
                }),
              ),
            ),
        });
        const options = {
          path: "/uncertain",
          ...definition,
          initial: { state: { count: 0 }, description: "Test" },
        };
        yield* Effect.scoped(
          Effect.gen(function* () {
            const context = yield* durable.open(options);
            assert.equal(
              (yield* context
                .commit(() => ({
                  state: { count: 1 },
                  messages: { append: [{ id: "m", text: "accepted" }] },
                }))
                .pipe(Effect.flip))._tag,
              "ContextCommitError",
            );
            assert.equal(durable.get(options.path)?.revision, 1);
            assert.equal(
              (yield* context.state.update(() => ({ count: 2 })).pipe(Effect.flip))._tag,
              "ContextCommitError",
            );
          }),
        );
        const recovered = yield* durable.open(options);
        assert.equal((yield* recovered.snapshot).revision, 2);
        assert.equal(yield* recovered.serializeState, "Count: 1");
        assert.deepEqual(yield* recovered.messages.list, [{ id: "m", text: "accepted" }]);
      }),
    ),
  );
});

test("lost acknowledgement restores State, receipt, outbox, Messages and source evidence together after restart", async (t) => {
  const root = mkdtempSync(join(tmpdir(), "aster-context-handoff-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const options = {
    path: "/handoff",
    state: Schema.Struct({
      summary: Schema.String,
      receipts: Schema.Array(Schema.Struct({ requestId: Schema.String })),
      outbox: Schema.Array(Schema.Struct({ requestId: Schema.String, status: Schema.String })),
    }),
    message: definition.message,
    keyOf: definition.keyOf,
    events: "state" as const,
    serialize: (state: { summary: string }) => state.summary,
    initial: { description: "Handoff", state: { summary: "Before", receipts: [], outbox: [] } },
  };
  const accepted = {
    summary: "Accepted",
    receipts: [{ requestId: "input" }],
    outbox: [{ requestId: "delivery", status: "pending" }],
  };
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const persistence = yield* makeContextSessionPersistence(join(root, "sessions"));
        const durable = yield* makeDurableContext({
          ...persistence,
          save: (record, owner) =>
            persistence.save(record, owner).pipe(
              Effect.andThen(
                record.snapshot.revision === 2
                  ? Effect.fail(
                      new ContextCommitError({
                        path: options.path,
                        cause: new Error("Lost acknowledgement"),
                      }),
                    )
                  : Effect.void,
              ),
            ),
        });
        const context = yield* durable.open(options);
        const result = yield* context
          .commit(() => ({
            state: accepted,
            messages: { append: [{ id: "input", text: "Accepted input" }] },
          }))
          .pipe(Effect.result);
        assert.equal(result._tag, "Failure");
        assert.deepEqual((yield* context.snapshot).state, options.initial.state);
        assert.equal(durable.journal().length, 0);
      }),
    ),
  );
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const durable = yield* setup(root);
        const context = yield* durable.open(options);
        assert.equal((yield* context.snapshot).revision, 2);
        assert.deepEqual(yield* context.state.get, accepted);
        assert.deepEqual(yield* context.messages.list, [{ id: "input", text: "Accepted input" }]);
        assert.equal(yield* context.serializeState, "Accepted");
        const events = durable.journal();
        assert.equal(events.length, 1);
        assert.equal(events[0]?.record.revision, 2);
        assert.equal(events[0]?.record.text, "Accepted");
        // Reapplying the same accepted state at its recovered revision must not duplicate the handoff.
        yield* context.state.set(accepted, { expectedRevision: 2 });
        assert.equal((yield* context.snapshot).revision, 2);
        assert.deepEqual(durable.journal(), events);
        assert.deepEqual(yield* context.messages.list, [{ id: "input", text: "Accepted input" }]);
      }),
    ),
  );
});
