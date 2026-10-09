import assert from "node:assert/strict";
import { test } from "node:test";
import { mkdtempSync, readdirSync, readFileSync, rmSync, statSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Effect, Schema } from "effect";
import {
  ContextRegistry,
  ContextCommitError,
  ContextSession,
  makeContextRegistryWithBackend,
  makeDurableContext,
} from "@aster/core";
import { makeFileContextStore } from "../src/storage/file-context-store.js";
import { makeContextSessionPersistence } from "../src/storage/context-sessions.js";

const definition = ContextSession.define({
  state: Schema.Struct({ count: Schema.Number }),
  message: Schema.Struct({ id: Schema.String, text: Schema.String }),
  messageKey: (m) => m.id,
  compareMessages: (a, b) => a.id.localeCompare(b.id),
});
const totalBytes = (root: string) =>
  readdirSync(root, { recursive: true, withFileTypes: true })
    .filter((f) => f.isFile())
    .reduce((sum, f) => sum + statSync(join(f.parentPath, f.name)).size, 0);
const setup = (root: string) =>
  makeContextSessionPersistence(
    makeFileContextStore(join(root, "actors")),
    join(root, "sessions"),
  ).pipe(Effect.flatMap(makeDurableContext), Effect.map(makeContextRegistryWithBackend));

test("Pi Documents write message deltas, checkpoint and recover ordered data without entries", async (t) => {
  const root = mkdtempSync(join(tmpdir(), "aster-session-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const initial = {
    description: "Test",
    state: { count: 0 },
    messages: Array.from({ length: 1000 }, (_, i) => ({ id: `m${i}`, text: "x".repeat(200) })),
  };
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* setup(root);
        yield* Effect.gen(function* () {
          const session = yield* ContextSession.open({ path: "/chat", definition, initial });
          const before = totalBytes(join(root, "sessions"));
          yield* session.messages.upsert([{ id: "new", text: "new" }]);
          assert.ok(
            totalBytes(join(root, "sessions")) - before < 20_000,
            "one insertion must not rewrite 200 KB of unchanged message bodies",
          );
          yield* session.messages.upsert([{ id: "__proto__", text: "safe" }]);
          for (let i = 0; i < 70; i++)
            yield* session.messages.upsert([{ id: "new", text: `edit ${i}` }]);
          yield* session.messages.removeUnchanged(initial.messages);
          yield* session.state.update(() => ({ count: 7 }));
        }).pipe(Effect.provideService(ContextRegistry, registry));
      }),
    ),
  );
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* setup(root);
        assert.equal(registry.get("/chat")?.messages.length, 2, "dormant Context is discoverable");
        yield* Effect.gen(function* () {
          const session = yield* ContextSession.open({ path: "/chat", definition, initial });
          assert.deepEqual(yield* session.messages.list, [
            { id: "__proto__", text: "safe" },
            { id: "new", text: "edit 69" },
          ]);
          assert.equal((yield* session.state.get).count, 7);
        }).pipe(Effect.provideService(ContextRegistry, registry));
      }),
    ),
  );
  const lines = readdirSync(join(root, "sessions"), { recursive: true, withFileTypes: true })
    .filter((f) => f.isFile() && f.name === "main.jsonl")
    .flatMap((f) => readFileSync(join(f.parentPath, f.name), "utf8").trim().split("\n"));
  assert.ok(lines.length > 70);
  assert.ok(lines.every((line) => !line.includes('"entry.append"')));
});

test("daily directories remain independent and reopening refuses a changed timezone", async (t) => {
  const root = mkdtempSync(join(tmpdir(), "aster-daily-session-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const initial = { description: "Daily", state: { count: 0 }, messages: [] };
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* setup(root);
        yield* Effect.gen(function* () {
          const first = yield* ContextSession.open({
            path: "/progress/days/2026-10-09",
            definition,
            initial,
            persistence: { layout: "daily", date: "2026-10-09", timeZone: "Asia/Shanghai" },
          });
          const next = yield* ContextSession.open({
            path: "/progress/days/2026-10-10",
            definition,
            initial,
            persistence: { layout: "daily", date: "2026-10-10", timeZone: "Asia/Shanghai" },
          });
          yield* first.state.update(() => ({ count: 9 }));
          assert.equal((yield* next.state.get).count, 0);
        }).pipe(Effect.provideService(ContextRegistry, registry));
      }),
    ),
  );
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* setup(root);
        const result = yield* ContextSession.open({
          path: "/progress/days/2026-10-09",
          definition,
          initial,
          persistence: { layout: "daily", date: "2026-10-09", timeZone: "UTC" },
        }).pipe(Effect.provideService(ContextRegistry, registry), Effect.flip);
        assert.equal(result._tag, "ContextRecoveryError");
      }),
    ),
  );
});

test("an uncertain Pi commit is recovered before reuse, retaining its original revision", async (t) => {
  const root = mkdtempSync(join(tmpdir(), "aster-session-uncertain-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const persistence = yield* makeContextSessionPersistence(
          makeFileContextStore(join(root, "actors")),
          join(root, "sessions"),
        );
        let uncertain = true;
        const backend = yield* makeDurableContext({
          ...persistence,
          save: (record, config) =>
            Effect.gen(function* () {
              yield* persistence.save(record, config);
              if (record.snapshot.revision === 2 && uncertain) {
                uncertain = false;
                return yield* new ContextCommitError({
                  path: record.snapshot.path,
                  cause: new Error("Commit succeeded but acknowledgement was lost"),
                });
              }
            }),
        });
        const registry = makeContextRegistryWithBackend(backend);
        const options = {
          path: "/uncertain",
          definition,
          initial: { state: { count: 0 }, messages: [], description: "Test" },
        };
        yield* Effect.scoped(
          Effect.gen(function* () {
            const session = yield* ContextSession.open(options);
            const error = yield* session
              .commit(() => ({
                state: { count: 1 },
                messages: { upsert: [{ id: "m", text: "accepted" }] },
              }))
              .pipe(Effect.flip);
            assert.equal(error._tag, "ContextCommitError");
            assert.equal(registry.get(options.path)?.revision, 1);
            assert.equal(
              (yield* session.state.update(() => ({ count: 2 })).pipe(Effect.flip))._tag,
              "ContextCommitError",
            );
          }).pipe(Effect.provideService(ContextRegistry, registry)),
        );
        yield* Effect.scoped(
          Effect.gen(function* () {
            const restored = yield* ContextSession.open(options);
            assert.equal((yield* restored.snapshot).revision, 2);
            assert.equal((yield* restored.state.get).count, 1);
            assert.deepEqual(yield* restored.messages.list, [{ id: "m", text: "accepted" }]);
            yield* restored.messages.upsert([{ id: "m", text: "accepted" }]);
            assert.equal((yield* restored.snapshot).revision, 2);
          }).pipe(Effect.provideService(ContextRegistry, registry)),
        );
      }),
    ),
  );
});
