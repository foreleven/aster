import { gateStub, summaryStub } from "./summary-fixtures.js";
import { LarkConfig, parseLarkConfig } from "@aster/integrations";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem } from "@aster/actor";
import { ContextRegistry, contextSpawnOptions } from "@aster/core";
import { makeContextRegistry } from "@aster/core/testing";
import { mkdtempSync, readFileSync, existsSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  ImAgentQueue,
  ImSummaryGate,
  ChatSummarizer,
  ImSearch,
  LarkImActor,
  ImStorage,
  makeImStorage,
  imDate,
  imDayStart,
  LarkChatActor,
  makeImClient,
  parseImPage,
  pollIm,
  type ChatSummary,
  type ChatSummaryInput,
} from "@aster/integrations";
import { TestClock } from "effect/testing";
import { Effect, Layer, Clock } from "effect";

test("IM searches recent messages before filtering muted chats and paginates the fixed window", async () => {
  const calls: string[][] = [];
  const cli = makeImClient(async (args) => {
    calls.push(args);
    if (args.includes("batch_query"))
      return JSON.stringify({
        data: {
          items: [
            { chat_id: "oc_group", is_muted: true },
            { chat_id: "oc_person", is_muted: false },
          ],
        },
      });
    const second = args.includes("--page-token");
    return JSON.stringify({
      data: {
        messages: second
          ? [
              {
                message_id: "old",
                chat_id: "oc_person",
                chat_type: "p2p",
                create_time: "1790640060",
                content: { text: "first" },
              },
            ]
          : [
              {
                message_id: "muted",
                chat_id: "oc_group",
                create_time: "2026-09-29T00:02:00Z",
                content: "ignored",
              },
              {
                message_id: "new",
                chat_id: "oc_person",
                chat_type: "p2p",
                create_time: "2026-09-29T00:03:00Z",
                content: "new",
              },
              {
                message_id: "outside",
                chat_id: "oc_old",
                create_time: "2026-09-28T00:00:00Z",
                content: "outside window",
              },
            ],
        has_more: !second,
        page_token: "next",
      },
    });
  });
  const batches = await cli.recent("2026-09-29T00:00:00Z", "2026-09-29T00:05:00Z");
  assert.equal(calls[0]![1], "+messages-search");
  assert.equal(calls[1]![1], "+messages-search");
  assert.equal(calls[2]![1], "chat.user_setting");
  assert.deepEqual(
    batches.map((b) => b.chat.id),
    ["oc_person"],
  );
  assert.deepEqual(
    batches[0]!.messages.map((m) => m.content),
    ["first", "new"],
  );
  assert.ok(calls.every((args) => args.includes("user")));
  assert.ok(
    !calls.some((args) => args.includes("+chat-list") || args.includes("+chat-messages-list")),
  );
  assert.throws(() => parseImPage('{"data":{"messages":[],"has_more":true}}'), /token/);
});

test("IM fails closed on incomplete mute settings and rejects repeated page tokens", async () => {
  const cli = makeImClient(async (args) =>
    JSON.stringify({
      data: args.includes("batch_query")
        ? { items: [] }
        : {
            messages: [{ message_id: "m", chat_id: "c", create_time: "2026-09-29T00:01:00Z" }],
            has_more: false,
          },
    }),
  );
  await assert.rejects(cli.recent("2026-09-29T00:00:00Z", "2026-09-29T00:05:00Z"), /Incomplete/);
  const repeated = makeImClient(async () =>
    JSON.stringify({ data: { messages: [], has_more: true, page_token: "same" } }),
  );
  await assert.rejects(repeated.recent("2026-09-29T00:00:00Z", "2026-09-29T00:05:00Z"), /repeated/);
});

test("IM batches mute lookups by ten and skips settings for empty windows", async () => {
  const sizes: number[] = [];
  const cli = makeImClient(async (args) => {
    if (args.includes("batch_query")) {
      const ids = JSON.parse(args[args.indexOf("--data") + 1]!).chat_ids as string[];
      sizes.push(ids.length);
      return JSON.stringify({
        data: { items: ids.map((chat_id) => ({ chat_id, is_muted: chat_id === "c10" })) },
      });
    }
    return JSON.stringify({
      data: {
        messages: Array.from({ length: 23 }, (_, i) => ({
          message_id: `m${i}`,
          chat_id: `c${i}`,
          create_time: "2026-09-29T00:01:00Z",
        })),
        has_more: false,
      },
    });
  });
  const batches = await cli.recent("2026-09-29T00:00:00Z", "2026-09-29T00:05:00Z");
  assert.deepEqual(sizes, [10, 10, 3]);
  assert.equal(batches.length, 22);
  let calls = 0;
  await makeImClient(async () => {
    calls++;
    return '{"data":{"messages":[],"has_more":false}}';
  }).recent("2026-09-29T00:00:00Z", "2026-09-29T00:05:00Z");
  assert.equal(calls, 1);
});

const until = (check: () => boolean) =>
  Effect.gen(function* () {
    while (!check()) yield* Effect.sleep(1);
  }).pipe(Effect.timeout("5 seconds"));
const path = "/lark/im/chats/oc_test";
const chat = { id: "oc_test", name: "Project", mode: "group", description: "" };
const today = imDate(Date.now());
const start = imDayStart(today);
const message = {
  id: "one",
  at: new Date(start + 1_000).toISOString(),
  content: "hello",
  sender: {},
  url: "",
  deleted: false,
};
const summary = (text: string): ChatSummary => ({ text, references: [] });
const tempStorage = (t: { after: (fn: () => void) => void }) => {
  const root = mkdtempSync(join(tmpdir(), "aster-im-test-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  return makeImStorage(root);
};

test("startup clamps old cursors to Beijing midnight while continuous polling crosses midnight", async () => {
  const windows: string[][] = [];
  const cli = {
    recent: async (from: string, through: string) => {
      windows.push([from, through]);
      return [];
    },
  };
  const now = Date.parse("2026-09-29T02:00:00Z");
  await pollIm(cli, undefined, now);
  await pollIm(cli, "2026-09-28T10:00:00Z", now);
  await pollIm(cli, "2026-09-29T01:00:00Z", now);
  await pollIm(cli, "2026-09-30T01:00:00Z", now);
  assert.deepEqual(
    windows.map((w) => w[0]),
    [
      "2026-09-28T16:00:00.000Z",
      "2026-09-28T16:00:00.000Z",
      "2026-09-29T00:59:00.000Z",
      "2026-09-28T16:00:00.000Z",
    ],
  );
  const midnight = Date.parse("2026-09-29T16:00:40Z");
  await pollIm(cli, "2026-09-29T15:59:40Z", midnight, undefined, false);
  assert.equal(windows.at(-1)![0], "2026-09-29T15:58:40.000Z");
  await pollIm(cli, "2026-09-29T15:59:40Z", midnight);
  assert.equal(windows.at(-1)![0], "2026-09-29T16:00:00.000Z");
  await assert.rejects(
    pollIm(
      {
        recent: async () => {
          throw new Error("offline");
        },
      },
      undefined,
      now,
    ),
    /offline/,
  );
});

test("daily retrieval intervals survive restart, split midnight and preserve gaps", (t) => {
  const storage = tempStorage(t);
  storage.ingest({ chat, messages: [message] });
  assert.equal(
    storage.progress(today),
    undefined,
    "durable inbox does not advance retrieval on its own",
  );
  storage.markRetrieved("2026-09-29T15:59:40Z", "2026-09-29T16:00:40Z");
  storage.markRetrieved("2026-09-29T16:02:00Z", "2026-09-29T16:03:00Z");
  const reopened = makeImStorage(storage.root);
  assert.equal(reopened.progress("2026-09-29")?.through, "2026-09-29T16:00:00.000Z");
  assert.deepEqual(reopened.progress("2026-09-30")?.intervals, [
    { from: "2026-09-29T16:00:00.000Z", through: "2026-09-29T16:00:40.000Z" },
    { from: "2026-09-29T16:02:00.000Z", through: "2026-09-29T16:03:00.000Z" },
  ]);
  assert.equal(reopened.get(today, chat.id)?.pending.length, 1);
});

test("chat updates daily and rolling summaries, retains concurrent arrivals and deduplicates after restart", async (t) => {
  const storage = tempStorage(t);
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        const calls: ChatSummaryInput[] = [];
        let finishDaily: ((value: ChatSummary) => void) | undefined;
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(ImAgentQueue, { run: (_id, execute) => execute }),
            Layer.succeed(ImSummaryGate, { needed: gateStub(async () => true) }),
            Layer.succeed(ContextRegistry, registry),
            Layer.succeed(ImStorage, storage),
            Layer.succeed(ChatSummarizer, {
              summarize: summaryStub(async (input) => {
                calls.push(input);
                if (calls.length === 1)
                  return new Promise((resolve) => {
                    finishDaily = resolve;
                  });
                return summary(input.date ? "Daily update" : "Rolling update");
              }),
            }),
          ),
        );
        const actor = yield* system.spawn("chat", LarkChatActor, contextSpawnOptions(path));
        yield* actor.tell({ _tag: "Update", chat, messages: [message] });
        yield* actor.tell({ _tag: "Summarize" });
        yield* until(() => !!finishDaily);
        const second = { ...message, id: "two" };
        yield* actor.tell({ _tag: "Update", chat, messages: [second] });
        yield* until(() => registry.get(path)?.messages.length === 2);
        finishDaily!(summary("Today"));
        yield* until(() => registry.get(path)?.messages.length === 0);
        assert.equal(calls.length, 4);
        assert.deepEqual(calls[2]?.messages, [second]);
        assert.equal(calls[2]?.previous?.text, "Today");
        assert.equal(calls[3]?.previous?.text, "Rolling update");
        assert.equal("through" in registry.get(path)!.state, false);
        const markdown = readFileSync(join(storage.root, today, `${chat.id}.md`), "utf8");
        assert.match(markdown, /timezone: "Asia\/Shanghai"/);
        assert.match(markdown, /message_count: 2/);
        assert.match(markdown, /Daily update/);
        // Simulate process restart at the persistence boundary, without sharing fingerprints in memory.
        const reopened = makeImStorage(storage.root);
        reopened.ingest({ chat, messages: [message, second] });
        assert.equal(reopened.get(today, chat.id)?.pending.length, 0);
      }),
    ),
  );
});

test("a rolling-summary failure retains raw messages and retry saves both summaries", async (t) => {
  const storage = tempStorage(t);
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        let rollingCalls = 0;
        let dailyCalls = 0;
        const clock = yield* TestClock.make();
        yield* clock.adjust(Date.now());
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(Clock.Clock, clock),
            Layer.succeed(ImAgentQueue, { run: (_id, execute) => execute }),
            Layer.succeed(ImSummaryGate, { needed: gateStub(async () => true) }),
            Layer.succeed(ContextRegistry, registry),
            Layer.succeed(ImStorage, storage),
            Layer.succeed(ChatSummarizer, {
              summarize: summaryStub(async (input) => {
                if (input.date) dailyCalls++;
                if (!input.date && ++rollingCalls === 1) throw new Error("rolling failed");
                return summary("Success");
              }),
            }),
          ),
        );
        const actor = yield* system.spawn("chat", LarkChatActor, contextSpawnOptions(path));
        yield* actor.tell({ _tag: "Update", chat, messages: [message] });
        yield* actor.tell({ _tag: "Summarize" });
        yield* until(() => !!storage.get(today, chat.id)?.lastError);
        assert.equal(registry.get(path)?.messages.length, 1);
        assert.equal(storage.get(today, chat.id)?.pending.length, 1);
        assert.ok(storage.get(today, chat.id)?.retryAt);
        yield* Effect.sleep(5);
        yield* clock.adjust("31 seconds");
        yield* actor.tell({ _tag: "Summarize" });
        yield* until(() => storage.get(today, chat.id)?.pending.length === 0);
        assert.equal(rollingCalls, 2);
        assert.equal(dailyCalls, 1);
        assert.equal(storage.get(today, chat.id)?.lastError, undefined);
        assert.ok(existsSync(join(storage.root, today, `${chat.id}.md`)));
      }),
    ),
  );
});

test("startup resumes today's quiet chat backlog but leaves prior-day messages untouched", async (t) => {
  const storage = tempStorage(t);
  const old = { ...message, id: "old", at: new Date(start - 1).toISOString() };
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry({
          loadAll: () => [
            {
              path,
              description: "Project",
              state: { chat, through: "legacy" },
              messages: [old, message],
            },
          ],
          save: () => {},
        });
        const calls: ChatSummaryInput[] = [];
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(ImAgentQueue, { run: (_id, execute) => execute }),
            Layer.succeed(ImSummaryGate, { needed: gateStub(async () => true) }),
            Layer.succeed(ContextRegistry, registry),
            Layer.succeed(ImStorage, storage),
            Layer.succeed(ChatSummarizer, {
              summarize: summaryStub(async (input) => {
                calls.push(input);
                return summary("Daily recovery");
              }),
            }),
          ),
        );
        yield* system.spawn("chat", LarkChatActor, contextSpawnOptions(path));
        yield* until(() => storage.get(today, chat.id)?.pending.length === 0);
        yield* until(() => registry.backend.journal().length === 1);
        assert.deepEqual(
          calls.map((call) => call.messages),
          [[message], [message]],
        );
        assert.deepEqual(registry.get(path)?.messages, [old]);
        assert.equal(storage.get(imDate(old.at), chat.id), undefined);
      }),
    ),
  );
});

test("a mixed midnight batch creates separate daily files for group and direct conversations", async (t) => {
  const storage = tempStorage(t);
  const before = { ...message, id: "before", at: new Date(start - 1).toISOString() };
  const midnight = { ...message, id: "midnight", at: new Date(start).toISOString() };
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        const calls: ChatSummaryInput[] = [];
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(ImAgentQueue, { run: (_id, execute) => execute }),
            Layer.succeed(ImSummaryGate, { needed: gateStub(async () => true) }),
            Layer.succeed(ContextRegistry, registry),
            Layer.succeed(ImStorage, storage),
            Layer.succeed(ChatSummarizer, {
              summarize: summaryStub(async (input) => {
                calls.push(input);
                return summary(input.date || "rolling");
              }),
            }),
          ),
        );
        const actor = yield* system.spawn("chat", LarkChatActor, contextSpawnOptions(path));
        yield* actor.tell({
          _tag: "Update",
          chat: { ...chat, mode: "p2p" },
          messages: [before, midnight],
        });
        yield* actor.tell({ _tag: "Summarize" });
        yield* until(() => calls.length === 4 && registry.get(path)?.messages.length === 0);
        const daily = calls.filter((call) => call.date);
        assert.deepEqual(
          daily.map((call) => call.messages.map((m) => m.id)),
          [["before"], ["midnight"]],
        );
        assert.equal(daily[1]?.previous, undefined);
        assert.ok(existsSync(join(storage.root, imDate(before.at), `${chat.id}.md`)));
        assert.match(
          readFileSync(join(storage.root, today, `${chat.id}.md`), "utf8"),
          /chat_mode: "p2p"/,
        );
      }),
    ),
  );
});

test("a journaled summary commit replays after restart without invoking the model", async (t) => {
  const storage = tempStorage(t);
  storage.ingest({ chat, messages: [message] });
  const day = storage.get(today, chat.id)!;
  day.commit = {
    batch: [message],
    daily: summary("daily"),
    rolling: summary("rolling"),
    evaluate: true,
    updatedAt: new Date().toISOString(),
  };
  storage.save(day);
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(ImAgentQueue, { run: (_id, execute) => execute }),
            Layer.succeed(ImSummaryGate, { needed: gateStub(async () => true) }),
            Layer.succeed(ContextRegistry, registry),
            Layer.succeed(ImStorage, makeImStorage(storage.root)),
            Layer.succeed(ChatSummarizer, {
              summarize: summaryStub(async () => {
                throw new Error("must replay");
              }),
            }),
          ),
        );
        yield* system.spawn("chat", LarkChatActor, contextSpawnOptions(path));
        yield* until(() => storage.get(today, chat.id)?.pending.length === 0);
        assert.deepEqual(registry.get(path)?.state, { chat, summary: summary("rolling") });
        assert.match(readFileSync(join(storage.root, today, `${chat.id}.md`), "utf8"), /daily/);
      }),
    ),
  );
});

test("IM startup wakes today's persisted inbox without search hits and journals retrieval before advancing", async (t) => {
  const storage = tempStorage(t);
  storage.ingest({ chat, messages: [message] });
  const oldChat = { ...chat, id: "oc_old" };
  storage.ingest({
    chat: oldChat,
    messages: [{ ...message, at: new Date(start - 1).toISOString() }],
  });
  const freshChat = { ...chat, id: "oc_fresh" };
  const freshMessage = { ...message, id: "fresh" };
  const markRetrieved = storage.markRetrieved;
  let checkedHandoff = false;
  storage.markRetrieved = (from, through) => {
    if (!checkedHandoff)
      assert.deepEqual(storage.get(today, freshChat.id)?.pending, [freshMessage]);
    checkedHandoff = true;
    markRetrieved(from, through);
  };
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        markRetrieved(new Date(start).toISOString(), new Date(Date.now() - 60_000).toISOString());
        const registry = yield* makeContextRegistry();
        const calls: string[] = [];
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(ImAgentQueue, { run: (_id, execute) => execute }),
            Layer.succeed(ImSummaryGate, { needed: gateStub(async () => true) }),
            Layer.succeed(ContextRegistry, registry),
            Layer.succeed(ImStorage, storage),
            Layer.succeed(LarkConfig, parseLarkConfig({})),
            Layer.succeed(ImSearch, {
              recent: async () => [{ chat: freshChat, messages: [freshMessage] }],
            }),
            Layer.succeed(ChatSummarizer, {
              summarize: summaryStub(async (input) => {
                calls.push(input.chat.id);
                return summary("Recovery");
              }),
            }),
          ),
        );
        yield* system.spawn("im", LarkImActor, contextSpawnOptions("/lark/im"));
        yield* until(() => checkedHandoff && storage.get(today, chat.id)?.pending.length === 0);
        assert.equal("through" in registry.get("/lark/im")!.state, false);
        assert.ok(storage.progress(today)?.through);
        assert.ok(
          calls.includes(chat.id),
          "quiet chat must be restored even though search returned only another chat",
        );
        assert.ok(!calls.includes(oldChat.id));
        assert.equal(storage.get(imDate(start - 1), oldChat.id)?.pending.length, 1);
      }),
    ),
  );
});

test("automatic retry runs without another incoming message and unchanged summaries do not screen again", async (t) => {
  const storage = tempStorage(t);
  storage.ingest({ chat, messages: [message] });
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry({
          loadAll: () => [
            {
              path,
              description: "Project",
              state: { chat, summary: summary("same") },
              messages: [message],
            },
          ],
          save: () => {},
        });
        const clock = yield* TestClock.make();
        yield* clock.adjust(Date.now());
        let calls = 0;
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(ImAgentQueue, { run: (_id, execute) => execute }),
            Layer.succeed(ImSummaryGate, { needed: gateStub(async () => true) }),
            Layer.succeed(ContextRegistry, registry),
            Layer.succeed(ImStorage, storage),
            Layer.succeed(Clock.Clock, clock),
            Layer.succeed(ChatSummarizer, {
              summarize: summaryStub(async () => {
                if (++calls === 1) throw new Error("daily unavailable");
                return summary("same");
              }),
            }),
          ),
        );
        yield* system.spawn("chat", LarkChatActor, contextSpawnOptions(path));
        yield* until(() => !!storage.get(today, chat.id)?.lastError);
        yield* Effect.sleep(5);
        yield* clock.adjust("31 seconds");
        yield* until(() => storage.get(today, chat.id)?.pending.length === 0);
        assert.equal(calls, 3);
        assert.equal(registry.backend.journal().length, 0);
      }),
    ),
  );
});

test("legacy recovery cannot overwrite a newer journaled edit", (t) => {
  const storage = tempStorage(t);
  const edited = { ...message, content: "edited" };
  storage.ingest({ chat, messages: [edited] });
  storage.ingest({ chat, messages: [message] }, true);
  assert.deepEqual(storage.get(today, chat.id)?.pending, [edited]);
});
