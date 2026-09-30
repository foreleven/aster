import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { Deferred, Effect, Fiber } from "effect";
import { MemoryRecallError } from "@aster/core";
import { makeMemoryClient, makeMemoryRecall } from "../src/index.js";

const connection = (dataDir: string) => ({
  url: "http://memory.invalid",
  secret: "test",
  dataDir,
  project: "test",
  cwd: dataDir,
});

test("interrupting memory recall aborts the actual search and expansion transport", async () => {
  const dir = await mkdtemp(join(tmpdir(), "aster-recall-"));
  try {
    for (const kind of ["search", "expand"]) {
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const entered = yield* Deferred.make<void>();
            let signal: AbortSignal | undefined;
            let requests = 0;
            const client = yield* Effect.acquireRelease(
              Effect.sync(() =>
                makeMemoryClient(connection(dir), (_url, init) => {
                  requests++;
                  signal = init?.signal ?? undefined;
                  assert.ok(signal);
                  Deferred.doneUnsafe(entered, Effect.void);
                  return new Promise<Response>((_resolve, reject) =>
                    signal!.addEventListener("abort", () => reject(signal!.reason), { once: true }),
                  );
                }),
              ),
              (client) => Effect.sync(() => client.close()),
            );
            const recall = makeMemoryRecall(client);
            const operation =
              kind === "search" ? recall.search("project") : recall.expand([{ obsId: "mem_one" }]);
            assert.equal(requests, 0, "constructing recall must not start a request");
            const fiber = yield* operation.pipe(Effect.forkScoped);
            yield* Deferred.await(entered).pipe(Effect.timeout("2 seconds"));
            yield* Fiber.interrupt(fiber);
            assert.equal(signal?.aborted, true);
            assert.equal(requests, 1);
          }),
        ),
      );
    }
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("an aborted expansion cannot start consolidated-memory fallback even if fetch resolves", async () => {
  const dir = await mkdtemp(join(tmpdir(), "aster-recall-"));
  const controller = new AbortController();
  const cancelled = new Error("cancelled expansion");
  let requests = 0;
  const client = makeMemoryClient(connection(dir), async () => {
    requests++;
    controller.abort(cancelled);
    return Response.json({ results: [] });
  });
  try {
    await assert.rejects(
      client.expand(["mem_one", "mem_two"], controller.signal),
      (cause) => cause === cancelled,
    );
    assert.equal(requests, 1);
  } finally {
    client.close();
    await rm(dir, { recursive: true, force: true });
  }
});

test("the memory adapter exposes tagged failures with the original backend cause", async () => {
  const cause = new Error("backend unavailable");
  const recall = makeMemoryRecall({
    search: async () => {
      throw cause;
    },
    expand: async () => {
      throw cause;
    },
  });
  for (const operation of [recall.search("project"), recall.expand([{ obsId: "one" }])]) {
    const error = await Effect.runPromise(Effect.flip(operation));
    assert.ok(error instanceof MemoryRecallError);
    assert.equal(error.cause, cause);
    assert.equal(error.message, "backend unavailable");
  }
});
