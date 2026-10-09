import assert from "node:assert/strict";
import { test } from "node:test";
import { Clock, Deferred, Effect, Exit, Fiber, Queue, Scope } from "effect";
import { TestClock } from "effect/testing";
import {
  makeImAgentQueue,
  makeImSummaryGate,
  pollChats,
  dayStart,
  type ChatMessageQuery,
} from "@aster/integrations";
const path = "/lark/im/chats/test";
const chat = { id: "test", name: "Test", mode: "group", description: "" };
const message = {
  id: "one",
  at: "2026-10-09T04:00:00Z",
  content: "Decision",
  sender: {},
  url: "",
  deleted: false,
};

// Actor timing and recovery are tested against ContextSession in the integration package.
test("hourly catch-up bounds every successful window", async () => {
  const now = Date.parse("2026-09-29T02:20:00Z");
  const windows: [string, string][] = [];
  const client = {
    searchMessages: ({ start, end }: ChatMessageQuery) =>
      Effect.sync(() => {
        windows.push([start, end]);
        return [];
      }),
  };
  let cursor: string | undefined;
  let startup = true;
  for (let i = 0; i < 20; i++) {
    const result = await Effect.runPromise(
      pollChats(client, cursor, now, startup, dayStart("2026-09-29")),
    );
    assert.ok(Date.parse(result.through) - Date.parse(result.start) <= 3600000);
    if (cursor) assert.ok(Date.parse(result.through) > Date.parse(cursor));
    cursor = result.through;
    startup = false;
    if (result.caughtUp) break;
  }
  assert.equal(windows[0]?.[0], "2026-09-28T16:00:00.000Z");
  assert.equal(cursor, "2026-09-29T02:20:00.000Z");
  assert.ok(windows.length > 10);
});
test("shared FIFO preserves spacing, cancellation, restart checkpoints and Scope ownership", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const clock = yield* TestClock.make();
        yield* clock.adjust(100000);
        yield* Effect.gen(function* () {
          let saved: number | undefined;
          const checkpoint = {
            load: () => Effect.sync(() => saved),
            save: (at: number) =>
              Effect.sync(() => {
                saved = at;
              }),
          };
          const owner = yield* Scope.make();
          const queue = yield* makeImAgentQueue(
            { startIntervalMs: 10000, concurrency: 2 },
            checkpoint,
          ).pipe(Effect.provideService(Scope.Scope, owner));
          const release = yield* Deferred.make<void>();
          const started = yield* Queue.unbounded<string>();
          const starts: string[] = [];
          const work = (id: string) =>
            Effect.sync(() => {
              starts.push(id);
            }).pipe(
              Effect.andThen(Queue.offer(started, id)),
              Effect.andThen(Deferred.await(release)),
            );
          const a = yield* queue.run("a", work("a")).pipe(Effect.forkScoped);
          assert.equal(yield* Queue.take(started), "a");
          const b = yield* queue.run("b", work("b")).pipe(Effect.forkScoped);
          const cancelled = yield* queue
            .run("cancelled", work("cancelled"))
            .pipe(Effect.forkScoped);
          const c = yield* queue.run("c", work("c")).pipe(Effect.forkScoped);
          yield* clock.adjust(10000);
          assert.equal(yield* Queue.take(started), "b");
          assert.deepEqual(starts, ["a", "b"]);
          yield* Fiber.interrupt(cancelled);
          yield* Fiber.interrupt(a);
          yield* clock.adjust(10000);
          assert.equal(yield* Queue.take(started), "c");
          assert.deepEqual(starts, ["a", "b", "c"]);
          yield* Scope.close(owner, Exit.void);
          assert.equal((yield* Fiber.await(b))._tag, "Failure");
          assert.equal((yield* Fiber.await(c))._tag, "Failure");
          const restarted = yield* makeImAgentQueue(
            { startIntervalMs: 10000, concurrency: 1 },
            checkpoint,
          );
          const next = yield* restarted
            .run(
              "next",
              Effect.sync(() => {
                starts.push("next");
              }),
            )
            .pipe(Effect.forkScoped);
          yield* clock.adjust(9999);
          assert.equal(starts.includes("next"), false);
          yield* clock.adjust(1);
          yield* Fiber.join(next);
          assert.equal(starts.at(-1), "next");
        }).pipe(Effect.provideService(Clock.Clock, clock));
      }),
    ),
  );
});
test("System One handles sparse important messages and rejects invalid decisions", async () => {
  let captured = "";
  const gate = makeImSummaryGate({
    systemOne: (request) =>
      Effect.sync(() => {
        captured = JSON.stringify(request);
        return { answers: { summarize: { type: "choice", choice: "yes" } } };
      }),
  });
  assert.equal(await Effect.runPromise(gate.needed({ path, chat, messages: [message] })), true);
  assert.match(captured, /single important message/);
  const invalid = makeImSummaryGate({ systemOne: () => Effect.sync(() => ({ answers: {} })) });
  await assert.rejects(
    Effect.runPromise(invalid.needed({ path, chat, messages: [message] })),
    /valid summary decision/,
  );
});
test("IM admission rejects duplicate keys and releases a cancelled claim", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const clock = yield* TestClock.make();
        yield* Effect.gen(function* () {
          const queue = yield* makeImAgentQueue(
            { startIntervalMs: 10, concurrency: 2 },
            { load: () => Effect.succeed(undefined), save: () => Effect.void },
          );
          const entered = yield* Deferred.make<void>();
          const first = yield* queue
            .run("chat", Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)))
            .pipe(Effect.forkScoped);
          yield* Deferred.await(entered);
          const duplicate = yield* Effect.result(
            queue.run("chat", Effect.die("Duplicate work started")),
          );
          assert.equal(duplicate._tag, "Failure");
          if (duplicate._tag === "Failure")
            assert.equal(duplicate.failure._tag, "ChatSummaryError");
          yield* Fiber.interrupt(first);
          const next = yield* queue
            .run("chat", Effect.succeed("reclaimed"))
            .pipe(Effect.forkScoped);
          yield* clock.adjust(10);
          assert.equal(yield* Fiber.join(next), "reclaimed");
        }).pipe(Effect.provideService(Clock.Clock, clock));
      }),
    ),
  );
});
