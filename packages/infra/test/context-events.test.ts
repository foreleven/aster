import assert from "node:assert/strict";
import { test } from "node:test";
import { mkdtempSync, readdirSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Effect, Schema } from "effect";
import { makeDurableContext, type ContextEvent } from "@aster/core";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import type { JsonValue } from "@earendil-works/chord";
import { createSession, defineDoc } from "@earendil-works/pi-durable";
import { openNodeJsonlStorage } from "@earendil-works/pi-durable/storage/jsonl/node";
import { makeContextSessionPersistence } from "../src/context/session-store.js";

const definition = {
  path: "/source",
  state: Schema.Struct({ summary: Schema.String, credential: Schema.String }),
  message: Schema.String,
  events: "state" as const,
  serialize: (state: { summary: string }) => state.summary,
  initial: {
    description: "Source",
    state: { summary: "Before", credential: "PRIVATE_TRANSPORT_TOKEN" },
    messages: ["Evidence"],
  },
};
const setup = (root: string) =>
  makeContextSessionPersistence(root).pipe(Effect.flatMap(makeDurableContext));
const assertHandoff = (events: readonly ContextEvent[]) => {
  assert.equal(events.length, 1);
  const event = events[0]!;
  assert.equal(event.record.path, definition.path);
  assert.equal(event.record.revision, 2);
  assert.equal(event.record.text, "Ready");
  assert.equal(JSON.stringify(event).includes("PRIVATE_TRANSPORT_TOKEN"), false);
  assert.equal("state" in event.record, false);
  assert.equal("messages" in event.record, false);
};

test("native storage retains frozen public evidence through message commits and restart", async (t) => {
  const root = mkdtempSync(join(tmpdir(), "aster-reaction-session-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  let committed: readonly ContextEvent[] = [];
  for (const restart of [false, true]) {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const durable = yield* setup(root);
          const owner = yield* durable.open(definition);
          if (!restart) {
            yield* owner.state.update((state) => ({ ...state, summary: "Ready" }));
            committed = durable.journal();
          } else {
            assert.deepEqual(durable.journal(), committed);
            yield* owner.messages.append(["new evidence"]);
            assert.equal((yield* owner.snapshot).revision, 3);
          }
          assertHandoff(durable.journal());
          assert.equal((yield* owner.state.get).credential, "PRIVATE_TRANSPORT_TOKEN");
        }),
      ),
    );
  }
});

// Deliberately inject invalid native document contents beneath the adapter's decoding boundary.
const PersistedEvents = defineDoc<{ events: Record<string, JsonValue> }>({
  kind: "aster.context.events",
  version: 1,
  scope: "session",
  initial: () => ({ events: {} }),
});

test("corrupt native source evidence fails recovery without rewriting storage", async (t) => {
  const root = mkdtempSync(join(tmpdir(), "aster-reaction-corrupt-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const event = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const durable = yield* setup(root);
        const owner = yield* durable.open(definition);
        yield* owner.state.update((state) => ({ ...state, summary: "Ready" }));
        return durable.journal()[0]!;
      }),
    ),
  );
  const log = readdirSync(root, { recursive: true, withFileTypes: true }).find(
    (file) => file.isFile() && file.name === "main.jsonl",
  )!;
  const filePath = join(log.parentPath, log.name);
  for (const events of [
    [{ ...event, record: { ...event.record, path: "/other" } }],
    [{ ...event, record: { ...event.record, revision: 4 } }],
    [{ ...event, id: "forged" }],
    [{ ...event, createdAt: "invalid" }],
    [event, event],
  ]) {
    const session = createSession(
      await openNodeJsonlStorage(log.parentPath, BACKGROUND_CONTEXT, { fsync: true }),
    );
    try {
      await session.commit(async (tx) => {
        const doc = await tx.doc(PersistedEvents);
        doc.events = Object.fromEntries(events.map((value, index) => [String(index), value]));
      }, BACKGROUND_CONTEXT);
    } finally {
      await session.close(BACKGROUND_CONTEXT);
    }
    const before = readFileSync(filePath, "utf8");
    const result = await Effect.runPromise(Effect.scoped(setup(root)).pipe(Effect.result));
    assert.equal(result._tag, "Failure");
    if (result._tag === "Failure") assert.equal(result.failure._tag, "ContextRecoveryError");
    assert.equal(readFileSync(filePath, "utf8"), before);
  }
});
