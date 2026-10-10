import assert from "node:assert/strict";
import { test } from "node:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Effect, Path } from "effect";
import { openProvenance } from "../src/memory/provenance.js";
test("source mappings survive read-only reopen and consolidated observation references", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "aster-provenance-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  for (const readOnly of [false, true])
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const store = yield* openProvenance(root, readOnly);
          if (!readOnly) yield* store.insert("session", "obs", ["/source"]);
          assert.deepEqual(
            (yield* store.attach({
              obsId: "mem_one",
              observation: { sourceObservationIds: ["obs"] },
            })).sourcePaths,
            ["/source"],
          );
          assert.deepEqual((yield* store.attach({ sessionId: "session" })).sourcePaths, [
            "/source",
          ]);
          if (readOnly)
            assert.equal(
              (yield* store.insert("session", "other", []).pipe(Effect.result))._tag,
              "Failure",
            );
        }),
      ).pipe(Effect.provide(Path.layer)),
    );
});
