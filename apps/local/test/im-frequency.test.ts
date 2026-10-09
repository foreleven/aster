import { ContextQueries } from "@aster/core";
import { LarkConfig, parseLarkConfig } from "@aster/integrations";
import { gateStub, summaryStub } from "./summary-fixtures.js";
// Tests inject internal completion and timer messages through untyped selections.
import { ActorSystem } from "@aster/actor";
import { ContextRegistry, contextSpawnOptions } from "@aster/core";
import { makeContextRegistry } from "@aster/core/testing";
import {
  ChatSummarizer,
  ImAgentQueue,
  imDate,
  imDayStart,
  ImSearch,
  ImStorage,
  ImSummaryGate,
  LarkChatActor,
  LarkImActor,
  makeImAgentQueue,
  makeImStorage,
  makeImSummaryGate,
  parseImPolicy,
  pollIm,
  type ChatSummary,
  type ChatSummaryInput,
} from "@aster/integrations";
import { Clock, Deferred, Effect, Exit, Fiber, Layer, Scope } from "effect";
import { TestClock } from "effect/testing";
import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";

const until = (check: () => boolean) =>
  Effect.gen(function* () {
    while (!check()) yield* Effect.sleep(1);
  }).pipe(Effect.timeout("5 seconds"));
const chat = { id: "oc_frequency", name: "Test", mode: "group", description: "" };
const path = `/lark/im/chats/${chat.id}`;
const today = imDate(Date.now());
const message = {
  id: "one",
  at: new Date(imDayStart(today) + 1000).toISOString(),
  content: "Key decision",
  sender: {},
  url: "",
  deleted: false,
};
const summary: ChatSummary = { text: "Result", references: [] };
const storeFor = (t: { after: (cleanup: () => void) => void }) => {
  const dir = mkdtempSync(join(tmpdir(), "aster-frequency-"));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  return makeImStorage(dir);
};
const immediate = Layer.succeed(ImAgentQueue, {
  run: (_chat, execute) => execute,
});

test("IM frequency defaults and hourly catch-up bound every successful window", async () => {
  assert.deepEqual(parseImPolicy(undefined), {
    pollIntervalMs: 900_000,
    catchUpWindowMs: 3_600_000,
    agentStartIntervalMs: 10_000,
    agentConcurrency: 2,
  });
  assert.throws(() => parseImPolicy({ config: { summary: { agentConcurrency: 0 } } }), /positive/);
  const now = Date.parse("2026-09-29T02:20:00Z");
  const windows: [string, string][] = [];
  const client = {
    recent: async (from: string, to: string) => {
      windows.push([from, to]);
      return [];
    },
  };
  let cursor: string | undefined;
  let startup = true;
  for (let i = 0; i < 20; i++) {
    const result = await pollIm(client, cursor, now, undefined, startup, imDayStart("2026-09-29"));
    assert.ok(Date.parse(result.through) - Date.parse(result.start) <= 3_600_000);
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
        yield* clock.adjust(100_000);
        yield* Effect.gen(function* () {
          let saved: number | undefined;
          const checkpoint = {
            load: () => saved,
            save: (at: number) => {
              saved = at;
            },
          };
          const owner = yield* Scope.make();
          const queue = yield* makeImAgentQueue(
            { startIntervalMs: 10_000, concurrency: 2 },
            checkpoint,
          ).pipe(Effect.provideService(Scope.Scope, owner));
          const release = yield* Deferred.make<void>();
          const starts: string[] = [];
          const work = (id: string) =>
            Effect.sync(() => {
              starts.push(id);
            }).pipe(Effect.andThen(Deferred.await(release)));
          const a = yield* queue.run("a", work("a")).pipe(Effect.forkScoped);
          yield* Effect.yieldNow;
          const b = yield* queue.run("b", work("b")).pipe(Effect.forkScoped);
          const cancelled = yield* queue
            .run("cancelled", work("cancelled"))
            .pipe(Effect.forkScoped);
          const c = yield* queue.run("c", work("c")).pipe(Effect.forkScoped);
          yield* clock.adjust(10_000);
          assert.deepEqual(starts, ["a", "b"]);
          yield* Fiber.interrupt(cancelled);
          yield* Fiber.interrupt(a);
          yield* clock.adjust(10_000);
          assert.deepEqual(starts, ["a", "b", "c"]);
          yield* Scope.close(owner, Exit.void);
          assert.equal((yield* Fiber.await(b))._tag, "Failure");
          assert.equal((yield* Fiber.await(c))._tag, "Failure");
          const restarted = yield* makeImAgentQueue(
            { startIntervalMs: 10_000, concurrency: 1 },
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
          yield* clock.adjust(9_999);
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

test("deferral survives restart and repeated updates; new evidence is assessed together", async (t) => {
  const storage = storeFor(t);
  storage.ingest({ chat, messages: [message] });
  let judgments = 0;
  const assessed: ChatSummaryInput[] = [];
  const gate = Layer.succeed(ImSummaryGate, {
    needed: gateStub(async (input: ChatSummaryInput) => {
      judgments++;
      assessed.push(input);
      return false;
    }),
  });
  const run = (restart: boolean) =>
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            ContextQueries.layer,
            Layer.succeed(ContextRegistry, registry),
            Layer.succeed(ImStorage, storage),
            gate,
            immediate,
            Layer.succeed(ChatSummarizer, {
              summarize: summaryStub(async () => {
                throw new Error("deferred batches must not call Agent");
              }),
            }),
          ),
        );
        const actor = yield* system.spawn("chat", LarkChatActor, contextSpawnOptions(path));
        if (!restart) {
          yield* until(() => !!storage.get(today, chat.id)?.assessment);
          return;
        }
        yield* actor.tell({ _tag: "Update", chat, messages: [message] });
        yield* (yield* system.select(actor.path).resolve()).tell({ _tag: "Summarize" });
        yield* Effect.sleep(30);
        assert.equal(judgments, 1);
        yield* actor.tell({ _tag: "Update", chat, messages: [{ ...message, id: "two" }] });
        yield* (yield* system.select(actor.path).resolve()).tell({ _tag: "Summarize" });
        yield* until(() => judgments === 2);
        assert.equal(assessed[1]?.messages.length, 2);
        assert.equal(storage.get(today, chat.id)?.pending.length, 2);
        assert.equal(storage.get(today, chat.id)?.summary, undefined);
      }),
    );
  await Effect.runPromise(run(false));
  await Effect.runPromise(run(true));
});

test("queued arrivals merge once; restart reuses the completed daily stage", async (t) => {
  const storage = storeFor(t);
  storage.ingest({ chat, messages: [message] });
  let release: (() => void) | undefined;
  const inputs: ChatSummaryInput[] = [];
  let permits = 0;
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            ContextQueries.layer,
            Layer.succeed(ContextRegistry, registry),
            Layer.succeed(ImStorage, storage),
            Layer.succeed(ImSummaryGate, { needed: gateStub(async () => true) }),
            Layer.succeed(ImAgentQueue, {
              run: (_id, execute) =>
                Effect.gen(function* () {
                  permits++;
                  if (permits === 1)
                    yield* Effect.promise(
                      () =>
                        new Promise<void>((resolve) => {
                          release = resolve;
                        }),
                    );
                  return yield* execute;
                }),
            }),
            Layer.succeed(ChatSummarizer, {
              summarize: summaryStub(async (input) => {
                inputs.push(input);
                if (!input.date) throw new Error("rolling offline");
                return summary;
              }),
            }),
          ),
        );
        const actor = yield* system.spawn("chat", LarkChatActor, contextSpawnOptions(path));
        yield* until(() => !!release);
        yield* actor.tell({ _tag: "Update", chat, messages: [{ ...message, id: "two" }] });
        yield* until(() => storage.get(today, chat.id)?.pending.length === 2);
        assert.equal(permits, 1);
        release!();
        yield* until(() => !!storage.get(today, chat.id)?.lastError);
        assert.equal(inputs[0]?.messages.length, 2);
        assert.equal(storage.get(today, chat.id)?.stage?.daily?.text, summary.text);
      }),
    ),
  );
  // Restart after the persisted retry deadline, using a clock rather than waiting in real time.
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const clock = yield* TestClock.make();
        yield* clock.adjust(Date.now() + 31_000);
        const registry = yield* makeContextRegistry();
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            ContextQueries.layer,
            Layer.succeed(Clock.Clock, clock),
            Layer.succeed(ContextRegistry, registry),
            Layer.succeed(ImStorage, makeImStorage(storage.root)),
            immediate,
            Layer.succeed(ImSummaryGate, {
              needed: gateStub(async () => {
                throw new Error("must not rejudge a fixed stage");
              }),
            }),
            Layer.succeed(ChatSummarizer, {
              summarize: summaryStub(async (input) => {
                inputs.push(input);
                return summary;
              }),
            }),
          ),
        );
        yield* system.spawn("chat", LarkChatActor, contextSpawnOptions(path));
        yield* until(() => storage.get(today, chat.id)?.pending.length === 0);
        assert.equal(inputs.length, 3);
        assert.equal(inputs[2]?.date, undefined);
        assert.deepEqual(inputs[2]?.messages, inputs[0]?.messages);
      }),
    ),
  );
});

test("System One failure retries after thirty seconds instead of deferring", async (t) => {
  const storage = storeFor(t);
  storage.ingest({ chat, messages: [message] });
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const clock = yield* TestClock.make();
        yield* clock.adjust(Date.now());
        const registry = yield* makeContextRegistry();
        let decisions = 0,
          runs = 0;
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            ContextQueries.layer,
            Layer.succeed(Clock.Clock, clock),
            Layer.succeed(ContextRegistry, registry),
            Layer.succeed(ImStorage, storage),
            immediate,
            Layer.succeed(ImSummaryGate, {
              needed: gateStub(async () => {
                if (++decisions === 1) throw new Error("System One offline");
                return true;
              }),
            }),
            Layer.succeed(ChatSummarizer, {
              summarize: summaryStub(async () => {
                runs++;
                return summary;
              }),
            }),
          ),
        );
        yield* system.spawn("chat", LarkChatActor, contextSpawnOptions(path));
        yield* until(() => !!storage.get(today, chat.id)?.retryAt);
        assert.equal(runs, 0);
        yield* Effect.sleep(5);
        yield* clock.adjust("29 seconds");
        assert.equal(decisions, 1);
        yield* clock.adjust("2 seconds");
        yield* until(() => storage.get(today, chat.id)?.pending.length === 0);
        assert.equal(decisions, 2);
        assert.equal(runs, 2);
      }),
    ),
  );
});

test("cross-midnight retrieval failure delays day-end flush; successful tail retrieval flushes deferred work", async (t) => {
  const storage = storeFor(t);
  const day = "2026-09-29";
  const lastPoll = Date.parse("2026-09-29T15:50:00Z");
  const oldMessage = { ...message, at: "2026-09-29T15:49:00Z" };
  storage.ingest({ chat, messages: [oldMessage] });
  storage.markRetrieved(new Date(imDayStart(day)).toISOString(), new Date(lastPoll).toISOString());
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const clock = yield* TestClock.make();
        yield* clock.adjust(lastPoll);
        const registry = yield* makeContextRegistry();
        let polls = 0,
          decisions = 0,
          runs = 0;
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            ContextQueries.layer,
            Layer.succeed(Clock.Clock, clock),
            Layer.succeed(ContextRegistry, registry),
            Layer.succeed(ImStorage, storage),
            immediate,
            Layer.succeed(LarkConfig, parseLarkConfig({})),
            Layer.succeed(ImSearch, {
              recent: async () => {
                if (++polls === 2) throw new Error("tail unavailable");
                return [];
              },
            }),
            Layer.succeed(ImSummaryGate, {
              needed: gateStub(async () => {
                decisions++;
                return false;
              }),
            }),
            Layer.succeed(ChatSummarizer, {
              summarize: summaryStub(async () => {
                runs++;
                return summary;
              }),
            }),
          ),
        );
        yield* system.spawn("im", LarkImActor, contextSpawnOptions("/lark/im"));
        yield* until(() => polls === 1 && !!storage.get(day, chat.id)?.assessment);
        yield* Effect.sleep(5);
        yield* clock.adjust("15 minutes");
        yield* until(() => polls === 2);
        yield* Effect.sleep(10);
        assert.equal(runs, 0);
        assert.equal(storage.get(day, chat.id)?.flush, undefined);
        yield* clock.adjust("15 minutes");
        yield* until(() => runs === 2 && storage.get(day, chat.id)?.pending.length === 0);
        assert.equal(decisions, 1, "day-end bypasses a repeated judgment");
        assert.equal(storage.progress(day)?.through, "2026-09-29T16:00:00.000Z");
      }),
    ),
  );
});

test("channel commits each catch-up window, resumes a failed window, then waits fifteen minutes", async (t) => {
  const storage = storeFor(t);
  const now = Date.parse("2026-09-29T19:20:00+08:00");
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const clock = yield* TestClock.make();
        yield* clock.adjust(now);
        const registry = yield* makeContextRegistry();
        const windows: [string, string][] = [];
        let fail = true;
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            ContextQueries.layer,
            Layer.succeed(Clock.Clock, clock),
            Layer.succeed(ContextRegistry, registry),
            Layer.succeed(ImStorage, storage),
            immediate,
            Layer.succeed(LarkConfig, parseLarkConfig({})),
            Layer.succeed(ImSummaryGate, { needed: gateStub(async () => false) }),
            Layer.succeed(ChatSummarizer, { summarize: summaryStub(async () => summary) }),
            Layer.succeed(ImSearch, {
              recent: async (from, through) => {
                windows.push([from, through]);
                if (windows.length === 2 && fail) throw new Error("window failed");
                return [];
              },
            }),
          ),
        );
        yield* system.spawn("im", LarkImActor, contextSpawnOptions("/lark/im"));
        yield* until(() => windows.length === 2);
        yield* Effect.sleep(10);
        assert.equal(storage.progress("2026-09-29")?.through, windows[0]![1]);
        assert.equal((registry.get("/lark/im")?.state as { ready: boolean }).ready, false);
        fail = false;
        yield* clock.adjust("15 minutes");
        yield* until(
          () => (registry.get("/lark/im")?.state as { ready?: boolean })?.ready === true,
        );
        assert.equal(
          windows[2]![0],
          windows[1]![0],
          "retry starts at the last committed coverage with overlap",
        );
        const completed = windows.length;
        assert.ok(completed > 19, "catch-up windows ran without a fifteen-minute delay per window");
        yield* Effect.sleep(10);
        yield* clock.adjust("14 minutes");
        assert.equal(windows.length, completed);
        yield* clock.adjust("1 minute");
        yield* until(() => windows.length === completed + 1);
      }),
    ),
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
            { load: () => undefined, save: () => {} },
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
          if (duplicate._tag === "Failure") assert.equal(duplicate.failure._tag, "ImSummaryError");
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
