import assert from "node:assert/strict";
import { test } from "node:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Effect } from "effect";
import { memory } from "./memory-fixtures.js";
test("malformed observation receipts fail before provenance or session completion", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "aster-memory-protocol-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const calls: string[] = [];
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const backend = yield* memory(root, async (url) => {
          calls.push(String(url));
          return Response.json({});
        });
        assert.equal(
          (yield* backend
            .capture!.capture({
              sessionId: "one",
              records: [{ path: "/source", text: "Evidence", revision: 1, description: "Source" }],
            })
            .pipe(Effect.flip))._tag,
          "MemoryCaptureError",
        );
        assert.equal(calls.length, 2);
        assert.deepEqual((yield* backend.provenance.attach({ obsId: "missing" })).sourcePaths, []);
      }),
    ),
  );
});

test("recall rejects malformed hit identities and consolidated memory details", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "aster-memory-recall-protocol-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  for (const value of [{ obsId: 42 }, { obsId: " " }, { obsId: "obs", sessionId: 42 }]) {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const backend = yield* memory(root, async () => Response.json({ results: [value] }));
          assert.equal(
            (yield* backend.client.search("query").pipe(Effect.flip))._tag,
            "MemoryRecallError",
          );
        }),
      ),
    );
  }
  for (const detail of [
    undefined,
    "invalid",
    {},
    { content: "Evidence", sourceObservationIds: [42] },
  ]) {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const backend = yield* memory(root, async (url) =>
            Response.json(
              String(url).includes("/memories/") ? { memory: detail } : { results: [] },
            ),
          );
          assert.equal(
            (yield* backend.client.expand(["mem_one"]).pipe(Effect.flip))._tag,
            "MemoryRecallError",
          );
        }),
      ),
    );
  }
});
