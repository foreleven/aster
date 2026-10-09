import { ChatMessage } from "../src/lark/im/service/model.js";
import type { TestContextRegistry } from "@aster/core/testing";
import { DurableContext } from "@aster/core";
import { makeTestContextRegistryWithBackend } from "@aster/core/testing";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem } from "@aster/actor";
import {
  ContextQueries,
  ContextRegistry,
  ContextSession,
  contextSpawnOptions,
  makeDurableContext,
  type StoredContext,
} from "@aster/core";
import { Clock, Deferred, Effect, Layer, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";
import { makeContextRegistry } from "@aster/core/testing";
import { LarkConfig, parseLarkConfig } from "../src/lark/config.js";
import { LarkImActor } from "../src/lark/im/channel-actor.js";
import { ImSnapshot } from "../src/lark/im/channel/snapshot.js";
import { ChatSnapshot, messageFingerprint } from "../src/lark/im/chat/snapshot.js";
import { LarkChatService, LarkChatQueryError } from "../src/lark/im/service/chat-service.js";
import { ChatSummarizer } from "../src/lark/im/summary/summarizer.js";
import { ChatSummaryGate } from "../src/lark/im/summary/gate.js";
import { ImAgentQueue, type AgentAdmission } from "../src/lark/im/summary/agent-queue.js";
const chat = { id: "channel", name: "Test", mode: "group", description: "" };
const path = `/lark/im/chats/${chat.id}`;
const message = {
  id: "m",
  at: "2026-10-09T15:49:00Z",
  content: "Decision",
  sender: {},
  url: "",
  deleted: false,
};
const summary = { text: "Summary", references: [] };
const admission: AgentAdmission = { run: (_id, work) => work };
const start = (
  registry: TestContextRegistry,
  client: Pick<LarkChatService["Service"], "searchMessages">,
  model: ChatSummarizer["Service"] = { summarize: () => Effect.succeed(summary) },
) =>
  Effect.gen(function* () {
    const system = yield* ActorSystem.make().pipe(
      ActorSystem.provide(
        ContextQueries.layer,
        Layer.merge(
          Layer.succeed(ContextRegistry, registry),
          Layer.succeed(DurableContext, registry.backend),
        ),
        Layer.succeed(LarkConfig, parseLarkConfig({})),
        Layer.succeed(LarkChatService, {
          ...client,
          listMessages: () => Effect.die("Unexpected history query"),
          getChatSettings: () => Effect.die("Unexpected settings query"),
        }),
        Layer.succeed(ChatSummaryGate, { needed: () => Effect.succeed(false) }),
        Layer.succeed(ImAgentQueue, admission),
        Layer.succeed(ChatSummarizer, model),
      ),
    );
    yield* (yield* system.spawn("im", LarkImActor, contextSpawnOptions("/lark/im"))).awaitStarted;
    return system;
  });
const channelState = (registry: TestContextRegistry) =>
  Schema.decodeUnknownSync(ImSnapshot)(registry.get("/lark/im")?.state);

test("Channel never advances coverage before every Chat acknowledges durable ingress", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const clock = yield* TestClock.make();
        yield* clock.adjust(Date.parse("2026-10-09T16:20:00Z"));
        yield* Effect.gen(function* () {
          const saving = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          const records = new Map<string, StoredContext>();
          const backend = yield* makeDurableContext({
            load: Effect.sync(() => [...records.values()]),
            save: (record) =>
              Effect.gen(function* () {
                if (record.snapshot.path === path && record.snapshot.messages.length) {
                  yield* Deferred.succeed(saving, undefined);
                  yield* Deferred.await(release);
                }
                records.set(record.snapshot.path, structuredClone(record));
              }),
          });
          const registry = makeTestContextRegistryWithBackend(backend);
          const changes = yield* registry.subscribe;
          yield* start(registry, {
            searchMessages: () =>
              Effect.succeed([{ chat, messages: [{ ...message, at: "2026-10-09T16:10:00Z" }] }]),
          });
          yield* Deferred.await(saving);
          assert.equal(channelState(registry).through, undefined);
          assert.equal(channelState(registry).ready, false);
          yield* Deferred.succeed(release, undefined);
          yield* changes.pipe(
            Stream.filter(
              ({ record }) =>
                record.path === "/lark/im" &&
                Schema.decodeUnknownSync(ImSnapshot)(record.state).ready,
            ),
            Stream.runHead,
          );
          assert.equal(records.get(path)?.snapshot.messages.length, 1);
          assert.equal(channelState(registry).through, "2026-10-09T16:20:00.000Z");
          assert.ok(registry.get("/lark/im/retrieval/days/2026-10-10"));
        }).pipe(Effect.provideService(Clock.Clock, clock));
      }),
    ),
  );
});

test("failed cross-midnight retrieval does not flush; successful coverage flushes once", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const clock = yield* TestClock.make();
        yield* clock.adjust(Date.parse("2026-10-09T15:50:00Z"));
        yield* Effect.gen(function* () {
          const registry = yield* makeContextRegistry();
          yield* Effect.scoped(
            Effect.gen(function* () {
              yield* ContextSession.make({
                path: "/lark/im",
                state: ImSnapshot,
                message: Schema.Never,
                initial: {
                  state: { ready: false, chats: 1, through: "2026-10-09T15:50:00Z" },
                  description: "IM",
                  messages: [],
                },
              });
              yield* ContextSession.make({
                path,
                state: ChatSnapshot,
                message: ChatMessage,
                messageKey: (message) => message.id,
                initial: {
                  state: {
                    chat,
                    seen: {
                      [message.id]: { fingerprint: messageFingerprint(message), at: message.at },
                    },
                  },
                  description: "Chat",
                  messages: [message],
                },
              });
            }).pipe(
              Effect.provideService(ContextRegistry, registry),
              Effect.provideService(DurableContext, registry.backend),
            ),
          );
          let polls = 0;
          let runs = 0;
          const changes = yield* registry.subscribe;
          const waitForChannel = (predicate: (state: typeof ImSnapshot.Type) => boolean) =>
            changes.pipe(
              Stream.filter(
                ({ record }) =>
                  record.path === "/lark/im" &&
                  predicate(Schema.decodeUnknownSync(ImSnapshot)(record.state)),
              ),
              Stream.runHead,
            );
          yield* start(
            registry,
            {
              searchMessages: () =>
                Effect.gen(function* () {
                  if (++polls === 2)
                    return yield* new LarkChatQueryError({ message: "tail unavailable" });
                  return [];
                }),
            },
            {
              summarize: () =>
                Effect.sync(() => {
                  runs++;
                  return summary;
                }),
            },
          );
          yield* waitForChannel((state) => state.ready);
          yield* Effect.yieldNow;
          yield* clock.adjust("15 minutes");
          yield* waitForChannel((state) => state.lastError !== undefined);
          assert.equal(runs, 0);
          assert.equal(
            Schema.decodeUnknownSync(ChatSnapshot)(registry.get(path)?.state).flushThrough,
            undefined,
          );
          yield* Effect.yieldNow;
          yield* clock.adjust("15 minutes");
          yield* changes.pipe(
            Stream.filter(
              ({ record }) =>
                record.path === path &&
                Schema.decodeUnknownSync(ChatSnapshot)(record.state).summary !== undefined,
            ),
            Stream.runHead,
          );
          assert.equal(runs, 1);
          assert.equal(registry.get(path)?.messages.length, 0);
          assert.ok(registry.get("/lark/im/retrieval/days/2026-10-09"));
          assert.ok(registry.get("/lark/im/retrieval/days/2026-10-10"));
        }).pipe(Effect.provideService(Clock.Clock, clock));
      }),
    ),
  );
});

test("catch-up retries the failed window and waits fifteen minutes only after completion", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const clock = yield* TestClock.make();
        yield* clock.adjust(Date.parse("2026-10-09T18:20:00Z"));
        yield* Effect.gen(function* () {
          const registry = yield* makeContextRegistry();
          const changes = yield* registry.subscribe;
          const windows: string[][] = [];
          yield* start(registry, {
            searchMessages: ({ start, end }) =>
              Effect.gen(function* () {
                windows.push([start, end]);
                if (windows.length === 2)
                  return yield* new LarkChatQueryError({ message: "window failed" });
                return [];
              }),
          });
          yield* changes.pipe(
            Stream.filter(
              ({ record }) =>
                record.path === "/lark/im" &&
                Schema.decodeUnknownSync(ImSnapshot)(record.state).lastError !== undefined,
            ),
            Stream.runHead,
          );
          assert.equal(channelState(registry).through, windows[0]![1]);
          yield* Effect.yieldNow;
          yield* clock.adjust("15 minutes");
          yield* changes.pipe(
            Stream.filter(
              ({ record }) =>
                record.path === "/lark/im" &&
                Schema.decodeUnknownSync(ImSnapshot)(record.state).ready,
            ),
            Stream.runHead,
          );
          assert.equal(windows[2]![0], windows[1]![0]);
          const completed = windows.length;
          yield* Effect.yieldNow;
          yield* clock.adjust("14 minutes");
          assert.equal(windows.length, completed);
          yield* clock.adjust("1 minute");
          yield* changes.pipe(
            Stream.filter(
              ({ record }) =>
                record.path === "/lark/im" &&
                Schema.decodeUnknownSync(ImSnapshot)(record.state).through !==
                  windows[completed - 1]![1],
            ),
            Stream.runHead,
          );
          assert.equal(windows.length, completed + 1);
        }).pipe(Effect.provideService(Clock.Clock, clock));
      }),
    ),
  );
});
