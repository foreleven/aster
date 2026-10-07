import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { Cause, Deferred, Effect, Exit, Fiber, Option } from "effect";
import { makeMemoryBackend, makeMemoryClient } from "../src/agentmemory/index.js";

// Real provenance storage and client, with the only network boundary replaced.
test("interrupted capture waits leave admitted transport work for backend drain", async (t) => {
  const dir = await mkdtemp(join(tmpdir(), "aster-memory-drain-"));
  t.after(() => rm(dir, { recursive: true, force: true }));
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const observed = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const draining = yield* Deferred.make<void>();
        const drained = yield* Deferred.make<void>();
        let ended = false;
        const client = makeMemoryClient(
          { url: "http://unused.invalid", secret: "fake", dataDir: dir, project: "test", cwd: dir },
          async (url) => {
            const path = new URL(String(url)).pathname;
            if (path.endsWith("/observe")) {
              await Effect.runPromise(Deferred.succeed(observed, undefined));
              await Effect.runPromise(Deferred.await(release));
              return Response.json({ observationId: "one" });
            }
            if (path.endsWith("/observations"))
              return Response.json({ observations: [{ id: "one", title: "processed" }] });
            if (path.endsWith("/session/end")) ended = true;
            return Response.json({});
          },
        );
        // Always unblock detached transport work before closing its SQLite connection,
        // including when an assertion fails.
        yield* Effect.addFinalizer(() =>
          Deferred.succeed(release, undefined).pipe(
            Effect.andThen(Effect.promise(client.drain)),
            Effect.ensuring(Effect.sync(client.close)),
          ),
        );
        const backend = makeMemoryBackend(client, { description: "Memory", retrieval: "bm25" });
        const capture = yield* backend
          .capture({
            sessionId: "capture",
            records: [
              {
                path: "/source",
                revision: 1,
                description: "Source",
                state: { value: 1 },
                messages: [],
              },
            ],
          })
          .pipe(Effect.forkScoped);
        yield* Deferred.await(observed);
        yield* Fiber.interrupt(capture);
        const exit = yield* Fiber.await(capture);
        assert.ok(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause));
        const drain = yield* Deferred.succeed(draining, undefined).pipe(
          Effect.andThen(backend.drain),
          Effect.andThen(Deferred.succeed(drained, undefined)),
          Effect.forkScoped,
        );
        yield* Deferred.await(draining);
        yield* Effect.yieldNow;
        assert.ok(Option.isNone(yield* Deferred.poll(drained)));
        assert.equal(ended, false);
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(drain);
        assert.equal(ended, true);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});
