import assert from "node:assert/strict";
import { test } from "node:test";
import { Effect, Option, Schema } from "effect";
import {
  ContextRegistry,
  ContextSession,
  ContextSessionClosed,
  contextView,
} from "../src/index.js";
import { makeContextRegistry } from "../src/testing/context.js";

const State = Schema.Struct({ count: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)) });
const Message = Schema.Struct({ id: Schema.String, text: Schema.String, at: Schema.Number });
const definition = ContextSession.define({
  state: State,
  message: Message,
  messageKey: (m) => m.id,
  compareMessages: (a, b) => a.at - b.at || a.id.localeCompare(b.id),
  changes: "durable-state",
  view: contextView({ state: State, message: Message }),
});
const options = {
  path: "/test/session",
  definition,
  initial: { description: "Test", state: { count: 0 }, messages: [] },
};
const message = { id: "__proto__", text: "original", at: 1 };

test("Session commits state and conditional message removals atomically and retains edits", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        yield* Effect.gen(function* () {
          const session = yield* ContextSession.open(options);
          yield* session.messages.upsert([message], { mode: "bootstrap" });
          const before = yield* session.snapshot;
          yield* session.messages.upsert([message]);
          assert.equal((yield* session.snapshot).revision, before.revision);
          const edited = { ...message, text: "edited" };
          yield* session.messages.upsert([edited, { id: "later", text: "later", at: 2 }]);
          yield* session.commit(() => ({
            state: { count: 1 },
            messages: { removeUnchanged: [message] },
          }));
          assert.deepEqual(yield* session.messages.get(message.id), Option.some(edited));
          assert.equal(registry.backend.journal().length, 1);
          yield* session.commit(() => ({
            state: { count: 2 },
            messages: { removeUnchanged: [edited] },
          }));
          assert.deepEqual(yield* session.messages.list, [{ id: "later", text: "later", at: 2 }]);
          assert.equal(registry.backend.journal().at(-1)?.record.messages.length, 1);
          const committed = yield* session.snapshot;
          const invalid = yield* session
            .commit(() => ({
              state: { count: -1 },
              messages: { removeUnchanged: [{ id: "later", text: "later", at: 2 }] },
            }))
            .pipe(Effect.flip);
          assert.equal(invalid._tag, "ContextValidationError");
          assert.deepEqual(yield* session.snapshot, committed);
          const conflict = yield* session.messages
            .upsert([message], { expectedRevision: 0 })
            .pipe(Effect.flip);
          assert.equal(conflict._tag, "ContextConflict");
          const duplicate = yield* session.messages.upsert([message, message]).pipe(Effect.flip);
          assert.equal(duplicate._tag, "ContextValidationError");
        }).pipe(Effect.provideService(ContextRegistry, registry));
      }),
    ),
  );
});

test("Session ownership is scoped; closed handles cannot write to a replacement owner", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        yield* Effect.gen(function* () {
          const retired = yield* Effect.scoped(
            Effect.gen(function* () {
              const session = yield* ContextSession.open(options);
              const duplicate = yield* Effect.scoped(ContextSession.open(options)).pipe(
                Effect.flip,
              );
              assert.equal(duplicate._tag, "ContextRecoveryError");
              yield* Effect.forEach(
                Array.from({ length: 10 }),
                () => session.state.update((s) => ({ count: s.count + 1 })),
                { concurrency: "unbounded" },
              );
              return session;
            }),
          );
          const next = yield* ContextSession.open(options);
          assert.equal((yield* next.state.get).count, 10);
          assert.ok(
            (yield* retired.state.update(() => ({ count: 99 })).pipe(Effect.flip)) instanceof
              ContextSessionClosed,
          );
          assert.equal((yield* next.state.get).count, 10);
        }).pipe(Effect.provideService(ContextRegistry, registry));
      }),
    ),
  );
});

test("daily Sessions validate their calendar date, timezone and public identity", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        for (const persistence of [
          { layout: "daily" as const, date: "2026-02-30", timeZone: "Asia/Shanghai" },
          { layout: "daily" as const, date: "2026-10-09", timeZone: "invalid" },
          { layout: "daily" as const, date: "2026-10-09", timeZone: "Asia/Shanghai" },
        ]) {
          const result = yield* Effect.scoped(
            ContextSession.open({ ...options, persistence }),
          ).pipe(Effect.provideService(ContextRegistry, registry), Effect.flip);
          assert.equal(result._tag, "ContextValidationError");
        }
      }),
    ),
  );
});
