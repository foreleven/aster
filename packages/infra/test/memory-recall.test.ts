import assert from "node:assert/strict";
import { test } from "node:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Deferred, Effect, Fiber } from "effect";
import { memory } from "./memory-fixtures.js";
test("recall expands selected identities only, preserves order/cap and joins durable provenance", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "aster-recall-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const bodies: unknown[] = [];
  for (const readOnly of [false, true])
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const backend = yield* memory(
            root,
            async (url, init) => {
              const body = init?.body ? JSON.parse(String(init.body)) : {};
              bodies.push(body);
              if (String(url).includes("/memories/"))
                return Response.json({
                  memory: { content: "Compressed", sourceObservationIds: ["obs"] },
                });
              return Response.json(
                body.query
                  ? { results: [{ obsId: "obs", sessionId: "one" }] }
                  : { results: [{ obsId: "obs", observation: { narrative: "Evidence" } }] },
              );
            },
            readOnly,
          );
          if (!readOnly) yield* backend.provenance.insert("one", "obs", ["/source"]);
          assert.deepEqual((yield* backend.client.search("Query")).results[0]?.sourcePaths, [
            "/source",
          ]);
          const expanded = yield* backend.client.expand(["mem_one", "obs", "missing"]);
          assert.deepEqual(
            expanded.results.map((row) => row.obsId),
            ["mem_one", "obs"],
          );
          assert.deepEqual(expanded.results[0]?.sourcePaths, ["/source"]);
          assert.equal(
            (yield* backend.client.expand(Array.from({ length: 21 }, (_, i) => `id${i}`)))
              .truncated,
            true,
          );
        }),
      ),
    );
  assert.ok(bodies.some((body) => JSON.stringify(body).includes('"expandIds"')));
});
test("recall cancellation reaches HTTP and starts no fallback or retry", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "aster-recall-abort-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        let aborted = false;
        let calls = 0;
        const backend = yield* memory(root, (_url, init) => {
          calls++;
          Deferred.doneUnsafe(entered, Effect.void);
          return new Promise<Response>((_, reject) =>
            init?.signal?.addEventListener("abort", () => {
              aborted = true;
              reject(init.signal?.reason);
            }),
          );
        });
        const fiber = yield* backend.client.expand(["mem_one"]).pipe(Effect.forkScoped);
        yield* Deferred.await(entered);
        yield* Fiber.interrupt(fiber);
        assert.equal(aborted, true);
        assert.equal(calls, 1);
      }),
    ),
  );
});
