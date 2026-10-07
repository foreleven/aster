import { CurrentActors } from "../src/tools/actors.js";
import type { CoreTool } from "../src/tools/define.js";
import assert from "node:assert/strict";
import { test } from "node:test";
import { Deferred, Effect, Fiber } from "effect";
import { contextQueryTools, memoryTools, contextTools } from "../src/tools/catalogues.js";
import { MemoryRecallError } from "../src/memory/contracts.js";
import { toolSystem } from "./tool-fixtures.js";

const call = (actors: CurrentActors["Service"], tool: CoreTool, args: object) =>
  tool.execute("call", args).pipe(Effect.provideService(CurrentActors, actors));

test("Memory asks keep mailboxes available and caller interruption releases recall", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const released = yield* Deferred.make<void>();
        const env = yield* toolSystem({
          memory: {
            search: () =>
              Deferred.succeed(entered, undefined).pipe(
                Effect.andThen(Effect.never),
                Effect.ensuring(Deferred.succeed(released, undefined)),
              ),
            expand: () => Effect.succeed({ evidence: "expanded" }),
          },
        });
        const [search, expand] = memoryTools();
        const worker = yield* call(env.system, search!, { query: "blocked" }).pipe(
          Effect.forkScoped,
        );
        yield* Deferred.await(entered);
        yield* env.memory.awaitStarted;
        assert.equal(
          (yield* call(env.system, expand!, { items: [{ obsId: "one" }] })).isError,
          undefined,
        );
        yield* Fiber.interrupt(worker);
        yield* Deferred.await(released);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("Context query cancellation reaches the backend and parallel callers retain separate results", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const released = yield* Deferred.make<void>();
        const unblock = yield* Deferred.make<void>();
        const env = yield* toolSystem({
          queries: {
            register: () => Effect.void,
            query: (input) =>
              input.args.query === "blocked"
                ? Deferred.succeed(entered, undefined).pipe(
                    Effect.andThen(Deferred.await(unblock)),
                    Effect.as({ ...input, queriedAt: "now", data: "blocked result" }),
                    Effect.ensuring(Deferred.succeed(released, undefined)),
                  )
                : Effect.succeed({ ...input, queriedAt: "now", data: input.args.query }),
          },
        });
        const first = contextQueryTools("/goals/one", (id) => id);
        const second = contextQueryTools("/goals/two", (id) => id);
        const input = { path: "/apps/test", command: "search", args: { query: "blocked" } };
        const worker = yield* call(env.system, first[0]!, input).pipe(Effect.forkScoped);
        yield* Deferred.await(entered);
        // One slow integration query cannot hold the catalogue mailbox or another caller's query.
        yield* call(env.system, contextTools()[0]!, { query: "" });
        const reply = yield* call(env.system, second[0]!, { ...input, args: { query: "second" } });
        assert.match(JSON.stringify(reply), /second/);
        yield* Fiber.interrupt(worker);
        yield* Deferred.await(released);
        assert.equal((yield* env.messages.read("/goals/one")).length, 0);
        assert.equal(
          (yield* env.messages.read("/goals/two")).filter(
            (entry) => entry.kind === "tool.query-result",
          ).length,
          1,
        );
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("Memory backend errors remain failed query results and capacity is bounded", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const started = yield* Deferred.make<void>();
        let count = 0;
        const env = yield* toolSystem({
          memory: {
            search: () =>
              Effect.gen(function* () {
                count++;
                if (count === 4) yield* Deferred.succeed(started, undefined);
                return yield* Effect.never;
              }),
            expand: () => Effect.fail(new MemoryRecallError({ message: "Backend unavailable" })),
          },
        });
        const [search, expand] = memoryTools();
        const failed = yield* call(env.system, expand!, { items: [] });
        assert.equal(failed.isError, true);
        const workers = yield* Effect.forEach([1, 2, 3, 4], () =>
          call(env.system, search!, { query: "blocked" }).pipe(Effect.forkScoped),
        );
        yield* Deferred.await(started);
        const rejected = yield* call(env.system, search!, { query: "overflow" });
        assert.equal(rejected.isError, true);
        assert.equal(count, 4);
        yield* Effect.forEach(workers, Fiber.interrupt);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});
