import assert from "node:assert/strict";
import { test } from "node:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Cause, Deferred, Effect, Exit, Fiber, Redacted } from "effect";
import { memory } from "./memory-fixtures.js";
import { makeCapture } from "../src/memory/capture.js";
const records = [{ path: "/source", revision: 1, description: "Source", text: "Evidence" }];
test("interrupted observers leave admitted capture work for drain, including shared session waiters", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "aster-capture-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const observed = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const run = Effect.runPromiseWith(yield* Effect.context<never>());
        const calls: string[] = [];
        const backend = yield* memory(root, async (url) => {
          const path = new URL(String(url)).pathname;
          calls.push(path);
          if (path.endsWith("/observe")) {
            await run(Deferred.succeed(observed, undefined));
            await run(Deferred.await(release));
            return Response.json({ observationId: "obs" });
          }
          if (path.endsWith("/observations"))
            return Response.json({ observations: [{ id: "obs", title: "compressed" }] });
          return Response.json({});
        });
        yield* Effect.addFinalizer(() => Deferred.succeed(release, undefined));
        const first = yield* backend
          .capture!.capture({ sessionId: "one", records })
          .pipe(Effect.forkScoped);
        yield* Deferred.await(observed);
        const second = yield* backend
          .capture!.capture({ sessionId: "one", records })
          .pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        yield* Fiber.interrupt(first);
        yield* Fiber.interrupt(second);
        assert.equal(calls.filter((path) => path.endsWith("/observe")).length, 1);
        const drain = yield* backend.capture!.drain.pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        assert.equal(drain.pollUnsafe(), undefined);
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(drain);
        assert.equal(calls.at(-1), "/agentmemory/session/end");
      }),
    ),
  );
});

test("completed admitted defects remain observable after the capture observer leaves", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "aster-capture-defect-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const exit = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const finished = yield* Deferred.make<void>();
        const backend = yield* memory(root, async () => Response.json({}));
        const capture = yield* makeCapture(
          {
            ...backend.client,
            request: () =>
              Deferred.succeed(entered, undefined).pipe(
                Effect.andThen(Deferred.await(release)),
                Effect.andThen(Effect.die("admitted capture defect")),
                Effect.ensuring(Deferred.succeed(finished, undefined)),
              ),
          },
          backend.provenance,
          {
            url: "https://unused.test",
            secret: Redacted.make("secret"),
            dataDir: root,
            project: "test",
            cwd: root,
          },
        );
        const observer = yield* capture
          .capture({ sessionId: "one", records })
          .pipe(Effect.forkScoped);
        yield* Deferred.await(entered);
        yield* Fiber.interrupt(observer);
        yield* Deferred.succeed(release, undefined);
        yield* Deferred.await(finished);
        yield* Effect.yieldNow;
        const drained = yield* Effect.exit(capture.drain);
        assert.equal(drained._tag, "Failure");
        if (Exit.isFailure(drained)) assert.equal(Cause.hasDies(drained.cause), true);
      }),
    ).pipe(Effect.exit),
  );
  assert.equal(exit._tag, "Failure");
  if (Exit.isFailure(exit)) assert.equal(Cause.hasDies(exit.cause), true);
});
test("failed processing reports a typed failure and does not close the upstream session", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "aster-capture-fail-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const calls: string[] = [];
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const backend = yield* memory(
          root,
          async (url) => {
            const path = new URL(String(url)).pathname;
            calls.push(path);
            return Response.json(
              path.endsWith("/observe")
                ? { observationId: "obs" }
                : path.endsWith("/observations")
                  ? { observations: [] }
                  : {},
            );
          },
          false,
          1,
        );
        assert.equal(
          (yield* backend.capture!.capture({ sessionId: "one", records }).pipe(Effect.flip))._tag,
          "MemoryCaptureError",
        );
        assert.equal(
          calls.some((path) => path.endsWith("/session/end")),
          false,
        );
      }),
    ),
  );
});
