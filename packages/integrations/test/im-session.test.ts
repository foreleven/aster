import { DurableContext } from "@aster/core";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem } from "@aster/actor";
import { ContextQueries, ContextRegistry, contextSpawnOptions } from "@aster/core";
import { makeContextRegistry } from "@aster/core/testing";
import { Clock, Deferred, Effect, Fiber, Layer, Queue, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";
import { LarkChatActor } from "../src/lark/im/chat-actor.js";
import { ChatSnapshot } from "../src/lark/im/chat/snapshot.js";
import { LarkConfig, parseLarkConfig } from "../src/lark/config.js";
import { ChatSummarizer, type ChatSummaryInput } from "../src/lark/im/summary/summarizer.js";
import { ChatSummaryGate } from "../src/lark/im/summary/gate.js";
import {
  ImAgentQueue,
  makeImAgentQueue,
  type AgentAdmission,
} from "../src/lark/im/summary/agent-queue.js";
import { ChatSummaryError } from "../src/lark/shared/errors.js";

const chat = { id: "test", name: "Test", mode: "group", description: "" };
const path = "/lark/im/chats/test";
const now = Date.parse("2026-10-09T04:00:00Z");
const message = {
  id: "m",
  at: new Date(now).toISOString(),
  content: "original",
  sender: {},
  url: "",
  deleted: false,
};
const summary = { text: "Summary", references: [] };
const immediate: AgentAdmission = { run: (_id, execute) => execute };
const setup = (
  model: ChatSummarizer["Service"],
  gate: ChatSummaryGate["Service"] = { needed: () => Effect.succeed(true) },
  admission = immediate,
  maxMessages = 200,
) =>
  Effect.gen(function* () {
    const registry = yield* makeContextRegistry();
    const changes = yield* registry.subscribe;
    const system = yield* ActorSystem.make().pipe(
      ActorSystem.provide(
        Layer.merge(
          Layer.succeed(ContextRegistry, registry),
          Layer.succeed(DurableContext, registry.backend),
        ),
        ContextQueries.layer,
        Layer.succeed(ChatSummarizer, model),
        Layer.succeed(ChatSummaryGate, gate),
        Layer.succeed(ImAgentQueue, admission),
        Layer.succeed(
          LarkConfig,
          parseLarkConfig({ children: { "/im": { config: { summary: { maxMessages } } } } }),
        ),
      ),
    );
    const completions = yield* system.events.pipe(
      Stream.filter(
        (event) =>
          event._tag === "CommandProcessed" &&
          event.commandTag === "Summarized" &&
          event.path === "/user/chat-test",
      ),
      Stream.toQueue({ capacity: "unbounded" }),
    );
    const spawnChat = (id: string) =>
      system
        .spawn(`chat-${id}`, LarkChatActor, contextSpawnOptions(`/lark/im/chats/${id}`))
        .pipe(Effect.flatMap((ref) => system.select(ref.path).resolve()));
    const spawn = spawnChat("test");
    const ref = yield* spawn;
    yield* ref.awaitStarted;
    yield* Queue.take(completions);
    const waitFor = (
      predicate: (state: typeof ChatSnapshot.Type, messages: readonly unknown[]) => boolean,
      contextPath = path,
    ) =>
      changes.pipe(
        Stream.filter(
          ({ record }) =>
            record.path === contextPath &&
            predicate(Schema.decodeUnknownSync(ChatSnapshot)(record.state), record.messages),
        ),
        Stream.runHead,
      );
    return { registry, system, ref, spawn, spawnChat, waitFor, settled: Queue.take(completions) };
  });
const run = <A, E>(program: Effect.Effect<A, E, import("effect").Scope.Scope>) =>
  withClock(() => program);
const withClock = <A, E>(
  program: (clock: TestClock.TestClock) => Effect.Effect<A, E, import("effect").Scope.Scope>,
) =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const clock = yield* TestClock.make();
        yield* clock.adjust(now);
        return yield* program(clock).pipe(Effect.provideService(Clock.Clock, clock));
      }),
    ),
  );

test("one rolling summary atomically retires unchanged evidence and retains an in-flight edit", async () => {
  await run(
    Effect.gen(function* () {
      const entered = yield* Deferred.make<ChatSummaryInput>();
      const release = yield* Deferred.make<void>();
      let assessments = 0;
      let calls = 0;
      const { ref, registry, waitFor } = yield* setup(
        {
          summarize: (input) =>
            Effect.gen(function* () {
              calls++;
              yield* Deferred.succeed(entered, input);
              yield* Deferred.await(release);
              return summary;
            }),
        },
        { needed: () => Effect.sync(() => ++assessments === 1) },
      );
      yield* ref.ask<void>((replyTo) => ({ _tag: "Update", chat, messages: [message], replyTo }));
      const input = yield* Deferred.await(entered);
      assert.deepEqual(input.messages, [message]);
      const edited = { ...message, content: "edited" };
      yield* ref.ask<void>((replyTo) => ({ _tag: "Update", chat, messages: [edited], replyTo }));
      yield* Deferred.succeed(release, undefined);
      yield* waitFor((state) => state.summary?.text === summary.text);
      assert.equal(calls, 1);
      assert.deepEqual(registry.get(path)?.messages, [edited]);
      assert.deepEqual(registry.backend.journal().at(-1)?.record.messages, [edited]);
      assert.equal(registry.backend.journal().length, 1);
    }),
  );
});

for (const pending of [
  { ...message, id: "second" },
  { ...message, content: "edited" },
]) {
  test(`flush received during execution survives commit for ${pending.id === message.id ? "an edit" : "a new message"}`, async () => {
    await run(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const second = yield* Deferred.make<ChatSummaryInput>();
        let calls = 0;
        let assessments = 0;
        const { ref, registry, waitFor } = yield* setup(
          {
            summarize: (input) =>
              Effect.gen(function* () {
                if (++calls === 1) {
                  yield* Deferred.succeed(entered, undefined);
                  yield* Deferred.await(release);
                } else {
                  yield* Deferred.succeed(second, input);
                }
                return summary;
              }),
          },
          { needed: () => Effect.sync(() => ++assessments === 1) },
        );
        yield* ref.ask<void>((replyTo) => ({ _tag: "Update", chat, messages: [message], replyTo }));
        yield* Deferred.await(entered);
        yield* ref.ask<void>((replyTo) => ({ _tag: "Update", chat, messages: [pending], replyTo }));
        yield* ref.tell({ _tag: "Flush", date: "2026-10-09" });
        yield* waitFor((state) => state.flushThrough === "2026-10-09");
        assert.equal(calls, 1);
        yield* Deferred.succeed(release, undefined);
        yield* waitFor((state) => state.summary !== undefined);
        const committed = Schema.decodeUnknownSync(ChatSnapshot)(registry.get(path)?.state);
        // An older result cannot clear the flush while its uncovered evidence remains.
        assert.ok(committed.flushThrough || registry.get(path)?.messages.length === 0);
        const input = yield* Deferred.await(second);
        assert.deepEqual(input.messages, [pending]);
        assert.deepEqual(input.previous, summary);
        yield* waitFor((state, messages) => state.summary !== undefined && messages.length === 0);
        assert.equal(
          Schema.decodeUnknownSync(ChatSnapshot)(registry.get(path)?.state).flushThrough,
          undefined,
        );
        assert.equal(assessments, 1);
        assert.equal(calls, 2);
      }),
    );
  });
}

test("deduplication survives restart while deferred evidence gets a fresh assessment", async () => {
  await run(
    Effect.gen(function* () {
      let assessments = 0;
      const { system, ref, spawn, settled, registry } = yield* setup(
        { summarize: () => Effect.die("Deferred batch must not execute") },
        {
          needed: () =>
            Effect.sync(() => {
              assessments++;
              return false;
            }),
        },
      );
      yield* ref.ask<void>((replyTo) => ({ _tag: "Update", chat, messages: [message], replyTo }));
      yield* settled;
      yield* system.stop(ref);
      const restored = yield* spawn;
      yield* restored.awaitStarted;
      yield* settled;
      yield* restored.ask<void>((replyTo) => ({
        _tag: "Update",
        chat,
        messages: [message],
        replyTo,
      }));
      yield* restored.ask<void>((replyTo) => ({ _tag: "Update", chat, messages: [], replyTo }));
      assert.equal(assessments, 2);
      yield* restored.ask<void>((replyTo) => ({
        _tag: "Update",
        chat,
        messages: [{ ...message, id: "new" }],
        replyTo,
      }));
      yield* settled;
      assert.equal(assessments, 3);
      assert.equal(registry.get(path)?.messages.length, 2);
    }),
  );
});

test("one shared admission request includes accumulated arrivals and deduplicates replay", async () => {
  await run(
    Effect.gen(function* () {
      const queued = yield* Deferred.make<void>();
      const permit = yield* Deferred.make<void>();
      const called = yield* Deferred.make<ChatSummaryInput>();
      let admissions = 0;
      let assessments = 0;
      const admission: AgentAdmission = {
        run: (_id, work) =>
          Effect.sync(() => {
            admissions++;
          }).pipe(
            Effect.andThen(Deferred.succeed(queued, undefined)),
            Effect.andThen(Deferred.await(permit)),
            Effect.andThen(work),
          ),
      };
      const { ref, registry, waitFor } = yield* setup(
        { summarize: (input) => Deferred.succeed(called, input).pipe(Effect.as(summary)) },
        {
          needed: () =>
            Effect.sync(() => {
              assessments++;
              return true;
            }),
        },
        admission,
      );
      yield* ref.ask<void>((replyTo) => ({ _tag: "Update", chat, messages: [message], replyTo }));
      yield* Deferred.await(queued);
      const second = { ...message, id: "second" };
      yield* ref.ask<void>((replyTo) => ({ _tag: "Update", chat, messages: [second], replyTo }));
      yield* ref.tell({ _tag: "Flush", date: "2026-10-09" });
      yield* ref.ask<void>((replyTo) => ({ _tag: "Update", chat, messages: [], replyTo }));
      assert.equal(admissions, 1);
      assert.equal(assessments, 1);
      yield* Deferred.succeed(permit, undefined);
      assert.equal((yield* Deferred.await(called)).messages.length, 2);
      yield* waitFor((state, messages) => state.summary !== undefined && messages.length === 0);
      yield* ref.ask<void>((replyTo) => ({
        _tag: "Update",
        chat,
        messages: [message, second],
        replyTo,
      }));
      assert.equal(registry.get(path)?.messages.length, 0);
      assert.equal(admissions, 1);
      assert.equal(assessments, 1);
    }),
  );
});

test("in-flight arrivals are reassessed after commit without duplicate summary runs", async () => {
  await run(
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      const second = yield* Deferred.make<ChatSummaryInput>();
      let calls = 0;
      const { ref, waitFor } = yield* setup({
        summarize: (input) =>
          Effect.gen(function* () {
            if (++calls === 1) {
              yield* Deferred.succeed(entered, undefined);
              yield* Deferred.await(release);
            } else {
              yield* Deferred.succeed(second, input);
            }
            return summary;
          }),
      });
      yield* ref.ask<void>((replyTo) => ({ _tag: "Update", chat, messages: [message], replyTo }));
      yield* Deferred.await(entered);
      const pending = { ...message, id: "second" };
      yield* ref.ask<void>((replyTo) => ({ _tag: "Update", chat, messages: [pending], replyTo }));
      yield* ref.ask<void>((replyTo) => ({ _tag: "Update", chat, messages: [], replyTo }));
      assert.equal(calls, 1);
      yield* Deferred.succeed(release, undefined);
      const input = yield* Deferred.await(second);
      assert.deepEqual(input.previous, summary);
      assert.deepEqual(input.messages, [pending]);
      yield* waitFor((state, messages) => state.summary !== undefined && messages.length === 0);
      assert.equal(calls, 2);
    }),
  );
});

test("Chat Actors share FIFO admission, include queued arrivals and release stopped work", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const clock = yield* TestClock.make();
        yield* clock.adjust(now);
        yield* Effect.gen(function* () {
          const shared = yield* makeImAgentQueue(
            { startIntervalMs: 10_000, concurrency: 1 },
            { load: () => Effect.succeed(undefined), save: () => Effect.void },
          );
          const submitted = yield* Queue.unbounded<string>();
          const started = yield* Queue.unbounded<ChatSummaryInput>();
          const release = yield* Deferred.make<void>();
          const stopped = yield* Deferred.make<void>();
          const starts: Array<readonly [string, number]> = [];
          const {
            ref: a,
            spawnChat,
            system,
            waitFor,
          } = yield* setup(
            {
              summarize: (input) =>
                Effect.gen(function* () {
                  starts.push([input.chat.id, yield* Clock.currentTimeMillis]);
                  yield* Queue.offer(started, input);
                  if (input.chat.id === "test") yield* Deferred.await(release);
                  if (input.chat.id === "b")
                    return yield* Effect.never.pipe(
                      Effect.onInterrupt(() => Deferred.succeed(stopped, undefined)),
                    );
                  return summary;
                }),
            },
            undefined,
            {
              run: (id, work) =>
                Queue.offer(submitted, id).pipe(Effect.andThen(shared.run(id, work))),
            },
          );
          const b = yield* spawnChat("b");
          const c = yield* spawnChat("c");
          yield* b.awaitStarted;
          yield* c.awaitStarted;
          for (const [id, ref] of [
            ["test", a],
            ["b", b],
            ["c", c],
          ] as const) {
            yield* ref.ask<void>((replyTo) => ({
              _tag: "Update",
              chat: { ...chat, id },
              messages: [message],
              replyTo,
            }));
            assert.equal(yield* Queue.take(submitted), id);
            if (id === "test") assert.equal((yield* Queue.take(started)).chat.id, "test");
          }
          const added = { ...message, id: "queued-arrival" };
          yield* b.ask<void>((replyTo) => ({
            _tag: "Update",
            chat: { ...chat, id: "b" },
            messages: [added],
            replyTo,
          }));
          yield* clock.adjust(10_000);
          assert.deepEqual(starts, [["test", now]]);
          yield* Deferred.succeed(release, undefined);
          const input = yield* Queue.take(started);
          assert.equal(input.chat.id, "b");
          assert.deepEqual(input.messages, [message, added]);
          assert.deepEqual(starts, [
            ["test", now],
            ["b", now + 10_000],
          ]);
          yield* system.stop(b);
          yield* Deferred.await(stopped);
          yield* clock.adjust(9_999);
          assert.equal(starts.length, 2);
          yield* clock.adjust(1);
          assert.equal((yield* Queue.take(started)).chat.id, "c");
          assert.deepEqual(starts, [
            ["test", now],
            ["b", now + 10_000],
            ["c", now + 20_000],
          ]);
          yield* waitFor(
            (state, messages) => state.summary !== undefined && messages.length === 0,
            "/lark/im/chats/c",
          );
        }).pipe(Effect.provideService(Clock.Clock, clock));
      }),
    ),
  );
});

for (const queued of [false, true]) {
  test(`summary failure retries after thirty seconds ${queued ? "despite repeated triggers" : "without new ingress"}`, async () => {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const clock = yield* TestClock.make();
          yield* clock.adjust(now);
          yield* Effect.gen(function* () {
            let calls = 0;
            const failed = yield* Deferred.make<void>();
            const { ref, waitFor, registry } = yield* setup({
              summarize: () =>
                ++calls === 1
                  ? Deferred.succeed(failed, undefined).pipe(
                      Effect.andThen(
                        Effect.fail(new ChatSummaryError({ message: "retry", kind: "transient" })),
                      ),
                    )
                  : Effect.succeed(summary),
            });
            yield* ref.ask<void>((replyTo) => ({
              _tag: "Update",
              chat,
              messages: [message],
              replyTo,
            }));
            yield* Deferred.await(failed);
            assert.equal(registry.get(path)?.messages.length, 1);
            if (queued) {
              yield* ref.tell({ _tag: "Flush", date: "2026-10-09" });
              yield* waitFor((state) => state.flushThrough === "2026-10-09");
            }
            yield* clock.adjust(29_999);
            assert.equal(calls, 1);
            yield* clock.adjust(1);
            yield* waitFor(
              (state, messages) => state.summary !== undefined && messages.length === 0,
            );
            assert.equal(calls, 2);
          }).pipe(Effect.provideService(Clock.Clock, clock));
        }),
      ),
    );
  });
}

test("restart finishes already accepted prior-day evidence without refetching history", async () => {
  await run(
    Effect.gen(function* () {
      const called = yield* Deferred.make<ChatSummaryInput>();
      const { system, ref, spawn, waitFor, settled } = yield* setup(
        { summarize: (input) => Deferred.succeed(called, input).pipe(Effect.as(summary)) },
        { needed: () => Effect.succeed(false) },
      );
      const yesterday = { ...message, at: "2026-10-08T04:00:00Z" };
      yield* ref.ask<void>((replyTo) => ({ _tag: "Update", chat, messages: [yesterday], replyTo }));
      yield* settled;
      yield* system.stop(ref);
      const restored = yield* spawn;
      yield* restored.awaitStarted;
      assert.deepEqual((yield* Deferred.await(called)).messages, [yesterday]);
      yield* waitFor((state, messages) => state.summary !== undefined && messages.length === 0);
    }),
  );
});

test("stopping a Chat cancels model work; stale reads and commits cannot mutate state", async () => {
  await run(
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const interrupted = yield* Deferred.make<void>();
      const { system, ref, registry } = yield* setup({
        summarize: () =>
          Deferred.succeed(entered, undefined).pipe(
            Effect.andThen(Effect.never),
            Effect.onInterrupt(() => Deferred.succeed(interrupted, undefined)),
          ),
      });
      yield* ref.ask<void>((replyTo) => ({ _tag: "Update", chat, messages: [message], replyTo }));
      yield* Deferred.await(entered);
      const before = registry.get(path);
      const stale = yield* ref.ask((replyTo) => ({
        _tag: "GetSummaryMessages",
        generation: "retired",
        replyTo,
      }));
      assert.equal(stale, undefined);
      yield* ref.ask((replyTo) => ({
        _tag: "ApplySummary",
        generation: "retired",
        value: { rolling: summary, batch: [message] },
        replyTo,
      }));
      yield* ref.tell({
        _tag: "Summarized",
        generation: "retired",
        result: { _tag: "Success", value: true },
      });
      yield* ref.ask<void>((replyTo) => ({ _tag: "Update", chat, messages: [], replyTo }));
      assert.deepEqual(registry.get(path), before);
      assert.equal(
        (yield* system.inspect()).find((actor) => actor.path === ref.path)?.pendingEffects,
        1,
      );
      yield* system.stop(ref);
      yield* Deferred.await(interrupted);
      assert.equal(registry.get(path)?.messages.length, 1);
    }),
  );
});

test("gate failures remain retryable and unchanged summaries do not emit another source event", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const clock = yield* TestClock.make();
        yield* clock.adjust(now);
        yield* Effect.gen(function* () {
          let decisions = 0;
          const failed = yield* Deferred.make<void>();
          const { ref, registry, waitFor } = yield* setup(
            { summarize: () => Effect.succeed(summary) },
            {
              needed: () =>
                Effect.suspend(() =>
                  ++decisions === 1
                    ? Deferred.succeed(failed, undefined).pipe(
                        Effect.andThen(
                          Effect.fail(
                            new ChatSummaryError({ message: "gate offline", kind: "transient" }),
                          ),
                        ),
                      )
                    : Effect.succeed(true),
                ),
            },
          );
          yield* ref.ask<void>((replyTo) => ({
            _tag: "Update",
            chat,
            messages: [message],
            replyTo,
          }));
          yield* Deferred.await(failed);
          yield* clock.adjust(30_000);
          yield* waitFor((state, messages) => state.summary !== undefined && messages.length === 0);
          assert.equal(registry.backend.journal().length, 1);
          yield* ref.ask<void>((replyTo) => ({
            _tag: "Update",
            chat,
            messages: [{ ...message, id: "second" }],
            replyTo,
          }));
          yield* waitFor(
            (state, messages) =>
              state.summary !== undefined &&
              Object.keys(state.seen).length === 2 &&
              messages.length === 0,
          );
          assert.equal(registry.backend.journal().length, 1);
        }).pipe(Effect.provideService(Clock.Clock, clock));
      }),
    ),
  );
});

test("model defects reach supervision and do not become ordinary retry checkpoints", async () => {
  await run(
    Effect.gen(function* () {
      const { ref, system, registry } = yield* setup({
        summarize: () => Effect.die(new Error("model defect")),
      });
      const restarting = yield* system.events.pipe(
        Stream.filter((event) => event._tag === "ActorRestarting"),
        Stream.runHead,
        Effect.forkScoped,
      );
      yield* Effect.yieldNow;
      yield* ref.ask<void>((replyTo) => ({ _tag: "Update", chat, messages: [message], replyTo }));
      yield* Fiber.join(restarting);
      assert.equal(Object.hasOwn(registry.get(path)?.state ?? {}, "retryAt"), false);
      assert.equal(registry.get(path)?.messages.length, 1);
    }),
  );
});

test("transient failures stop after four attempts and only new evidence or flush starts another cycle", async () => {
  await withClock((clock) =>
    Effect.gen(function* () {
      const attempts = yield* Queue.unbounded<number>();
      let calls = 0;
      let admissions = 0;
      const { ref, settled, registry, system, spawn } = yield* setup(
        {
          summarize: () =>
            Queue.offer(attempts, ++calls).pipe(
              Effect.andThen(
                Effect.fail(new ChatSummaryError({ message: "offline", kind: "transient" })),
              ),
            ),
        },
        undefined,
        {
          run: (_id, work) =>
            Effect.sync(() => {
              admissions++;
            }).pipe(Effect.andThen(work)),
        },
      );
      yield* ref.ask<void>((replyTo) => ({ _tag: "Update", chat, messages: [message], replyTo }));
      assert.equal(yield* Queue.take(attempts), 1);
      for (let attempt = 2; attempt <= 4; attempt++) {
        yield* clock.adjust(30_000);
        assert.equal(yield* Queue.take(attempts), attempt);
      }
      yield* settled;
      yield* clock.adjust("10 minutes");
      yield* ref.ask<void>((replyTo) => ({ _tag: "Update", chat, messages: [message], replyTo }));
      assert.equal(calls, 4);
      assert.equal(admissions, 4);
      assert.equal(registry.get(path)?.messages.length, 1);
      yield* ref.tell({ _tag: "Flush", date: "2026-10-09" });
      assert.equal(yield* Queue.take(attempts), 5);
      yield* system.stop(ref);
      const restored = yield* spawn;
      yield* restored.awaitStarted;
      assert.equal(yield* Queue.take(attempts), 6);
      assert.equal(Object.hasOwn(registry.get(path)?.state ?? {}, "retryAt"), false);
    }),
  );
});

test("a retry releases shared capacity and retains its selected batch despite new arrivals", async () => {
  await withClock((clock) =>
    Effect.gen(function* () {
      const shared = yield* makeImAgentQueue(
        { startIntervalMs: 10_000, concurrency: 1 },
        { load: () => Effect.succeed(undefined), save: () => Effect.void },
      );
      const started = yield* Queue.unbounded<ChatSummaryInput>();
      let calls = 0;
      const { ref, spawnChat, waitFor } = yield* setup(
        {
          summarize: (input) =>
            Effect.gen(function* () {
              yield* Queue.offer(started, input);
              if (++calls === 1)
                return yield* new ChatSummaryError({ message: "offline", kind: "transient" });
              return summary;
            }),
        },
        undefined,
        shared,
      );
      yield* ref.ask<void>((replyTo) => ({ _tag: "Update", chat, messages: [message], replyTo }));
      assert.equal((yield* Queue.take(started)).chat.id, "test");
      const other = yield* spawnChat("other");
      yield* other.awaitStarted;
      yield* other.ask<void>((replyTo) => ({
        _tag: "Update",
        chat: { ...chat, id: "other" },
        messages: [message],
        replyTo,
      }));
      const newer = { ...message, id: "newer" };
      yield* ref.ask<void>((replyTo) => ({ _tag: "Update", chat, messages: [newer], replyTo }));
      yield* clock.adjust(10_000);
      assert.equal((yield* Queue.take(started)).chat.id, "other");
      yield* clock.adjust(20_000);
      const retry = yield* Queue.take(started);
      assert.equal(retry.chat.id, "test");
      assert.deepEqual(retry.messages, [message]);
      yield* clock.adjust(10_000);
      assert.deepEqual((yield* Queue.take(started)).messages, [newer]);
      yield* waitFor((_state, messages) => messages.length === 0);
    }),
  );
});

test("message-count overflow skips the gate and shrinking commits every successful chronological batch", async () => {
  await run(
    Effect.gen(function* () {
      const inputs: ChatSummaryInput[] = [];
      let admissions = 0;
      const all = Array.from({ length: 5 }, (_, index) => ({
        ...message,
        id: String(index),
        at: new Date(now + index).toISOString(),
      }));
      const { ref, settled, registry } = yield* setup(
        {
          summarize: (input) =>
            Effect.gen(function* () {
              inputs.push(input);
              if (input.messages.length > 2)
                return yield* new ChatSummaryError({
                  message: "context_length_exceeded",
                  kind: "capacity",
                });
              return {
                text: [
                  ...(input.previous?.text.split(",") ?? []),
                  ...input.messages.map((value) => value.id),
                ].join(","),
                references: [],
              };
            }),
        },
        { needed: () => Effect.die("Overflow must bypass the gate") },
        {
          run: (_id, work) =>
            Effect.sync(() => {
              admissions++;
            }).pipe(Effect.andThen(work)),
        },
        4,
      );
      yield* ref.ask<void>((replyTo) => ({
        _tag: "Update",
        chat,
        messages: [...all].reverse(),
        replyTo,
      }));
      yield* settled;
      assert.deepEqual(
        inputs.map((input) => input.messages.length),
        [4, 2, 2, 1],
      );
      assert.equal(admissions, 4);
      assert.equal(
        Schema.decodeUnknownSync(ChatSnapshot)(registry.get(path)?.state).summary?.text,
        "0,1,2,3,4",
      );
      assert.equal(registry.get(path)?.messages.length, 0);
      assert.deepEqual(
        registry.backend.journal().map((event) => event.record.messages.length),
        [3, 1, 0],
      );
    }),
  );
});

for (const kind of ["permanent", "capacity"] as const) {
  test(`${kind} failure at one message retains evidence without another automatic attempt`, async () => {
    await withClock((clock) =>
      Effect.gen(function* () {
        let calls = 0;
        const { ref, settled, registry } = yield* setup({
          summarize: () =>
            Effect.sync(() => {
              calls++;
            }).pipe(
              Effect.andThen(
                Effect.fail(new ChatSummaryError({ message: "cannot summarize", kind })),
              ),
            ),
        });
        yield* ref.ask<void>((replyTo) => ({ _tag: "Update", chat, messages: [message], replyTo }));
        yield* settled;
        yield* clock.adjust("10 minutes");
        assert.equal(calls, 1);
        assert.deepEqual(registry.get(path)?.messages, [message]);
      }),
    );
  });
}

test("a date-bounded flush does not force today's arrivals after yesterday is covered", async () => {
  await run(
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let calls = 0;
      let assessments = 0;
      const { ref, settled, registry } = yield* setup(
        {
          summarize: () =>
            Effect.gen(function* () {
              calls++;
              yield* Deferred.succeed(entered, undefined);
              yield* Deferred.await(release);
              return summary;
            }),
        },
        {
          needed: () =>
            Effect.sync(() => {
              assessments++;
              return false;
            }),
        },
      );
      const yesterday = { ...message, at: "2026-10-08T04:00:00Z" };
      yield* ref.ask<void>((replyTo) => ({ _tag: "Update", chat, messages: [yesterday], replyTo }));
      yield* settled;
      yield* ref.tell({ _tag: "Flush", date: "2026-10-08" });
      yield* Deferred.await(entered);
      const today = { ...message, id: "today" };
      yield* ref.ask<void>((replyTo) => ({ _tag: "Update", chat, messages: [today], replyTo }));
      yield* Deferred.succeed(release, undefined);
      yield* settled;
      yield* settled;
      assert.equal(calls, 1);
      assert.equal(assessments, 2);
      assert.deepEqual(registry.get(path)?.messages, [today]);
      assert.equal(
        Schema.decodeUnknownSync(ChatSnapshot)(registry.get(path)?.state).flushThrough,
        undefined,
      );
    }),
  );
});

test("receipt pruning preserves pending evidence and replay overlap, then cleans retired receipts", async () => {
  await run(
    Effect.gen(function* () {
      const { ref, settled, registry, waitFor } = yield* setup(
        { summarize: () => Effect.succeed(summary) },
        { needed: () => Effect.succeed(false) },
      );
      const old = { ...message, id: "old", at: new Date(now - 60_001).toISOString() };
      const overlap = { ...message, id: "overlap", at: new Date(now - 60_000).toISOString() };
      yield* ref.ask<void>((replyTo) => ({
        _tag: "Update",
        chat,
        messages: [old, overlap],
        replyTo,
      }));
      yield* settled;
      yield* ref.tell({ _tag: "RetainReceipts", from: overlap.at });
      yield* ref.ask<void>((replyTo) => ({ _tag: "Update", chat, messages: [], replyTo }));
      assert.deepEqual(
        Object.keys(Schema.decodeUnknownSync(ChatSnapshot)(registry.get(path)?.state).seen),
        ["old", "overlap"],
      );
      yield* ref.tell({ _tag: "Flush", date: "2026-10-09" });
      yield* settled;
      assert.deepEqual(
        Object.keys(Schema.decodeUnknownSync(ChatSnapshot)(registry.get(path)?.state).seen),
        ["overlap"],
      );
      yield* ref.ask<void>((replyTo) => ({ _tag: "Update", chat, messages: [overlap], replyTo }));
      assert.equal(registry.get(path)?.messages.length, 0);
      yield* ref.tell({ _tag: "RetainReceipts", from: new Date(now).toISOString() });
      yield* waitFor((state) => Object.keys(state.seen).length === 0);
    }),
  );
});

test("capacity reduction of a flush restores ordinary assessment once its date is covered", async () => {
  await run(
    Effect.gen(function* () {
      const sizes: number[] = [];
      const { ref, settled, registry } = yield* setup(
        {
          summarize: (input) =>
            Effect.gen(function* () {
              sizes.push(input.messages.length);
              if (input.messages.length > 1)
                return yield* new ChatSummaryError({ message: "too long", kind: "capacity" });
              return summary;
            }),
        },
        { needed: () => Effect.succeed(false) },
      );
      const yesterday = { ...message, id: "yesterday", at: "2026-10-08T04:00:00Z" };
      yield* ref.ask<void>((replyTo) => ({
        _tag: "Update",
        chat,
        messages: [yesterday, message],
        replyTo,
      }));
      yield* settled;
      yield* ref.tell({ _tag: "Flush", date: "2026-10-08" });
      yield* settled;
      yield* settled;
      assert.deepEqual(sizes, [2, 1]);
      assert.deepEqual(registry.get(path)?.messages, [message]);
      assert.equal(
        Schema.decodeUnknownSync(ChatSnapshot)(registry.get(path)?.state).flushThrough,
        undefined,
      );
    }),
  );
});

test("successful batches survive a later failure and restart resumes only uncovered evidence", async () => {
  await run(
    Effect.gen(function* () {
      let calls = 0;
      const inputs: ChatSummaryInput[] = [];
      const { ref, settled, registry, system, spawn } = yield* setup(
        {
          summarize: (input) =>
            Effect.gen(function* () {
              inputs.push(input);
              if (++calls === 2)
                return yield* new ChatSummaryError({
                  message: "invalid output",
                  kind: "permanent",
                });
              return summary;
            }),
        },
        undefined,
        immediate,
        2,
      );
      const all = Array.from({ length: 5 }, (_, index) => ({
        ...message,
        id: String(index),
        at: new Date(now + index).toISOString(),
      }));
      yield* ref.ask<void>((replyTo) => ({ _tag: "Update", chat, messages: all, replyTo }));
      yield* settled;
      assert.deepEqual(registry.get(path)?.messages, all.slice(2));
      assert.deepEqual(
        Schema.decodeUnknownSync(ChatSnapshot)(registry.get(path)?.state).summary,
        summary,
      );
      yield* system.stop(ref);
      const restored = yield* spawn;
      yield* restored.awaitStarted;
      yield* settled;
      assert.deepEqual(inputs[2]?.messages, all.slice(2, 4));
      assert.deepEqual(inputs[2]?.previous, summary);
      assert.equal(registry.get(path)?.messages.length, 0);
      assert.equal(calls, 4);
    }),
  );
});

test("transport-only sender changes do not resurrect summarized evidence", async () => {
  await run(
    Effect.gen(function* () {
      let assessments = 0;
      const { ref, settled, registry } = yield* setup(
        { summarize: () => Effect.succeed(summary) },
        {
          needed: () =>
            Effect.sync(() => {
              assessments++;
              return true;
            }),
        },
      );
      const received = { ...message, sender: { name: "Alex", transportVersion: 1 } };
      yield* ref.ask<void>((replyTo) => ({ _tag: "Update", chat, messages: [received], replyTo }));
      yield* settled;
      yield* ref.ask<void>((replyTo) => ({
        _tag: "Update",
        chat,
        messages: [{ ...received, sender: { ...received.sender, transportVersion: 2 } }],
        replyTo,
      }));
      assert.equal(assessments, 1);
      assert.equal(registry.get(path)?.messages.length, 0);
    }),
  );
});
