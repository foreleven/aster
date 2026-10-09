import assert from "node:assert/strict";
import { test } from "node:test";
import { Deferred, Effect, Fiber } from "effect";
import { LarkCliError } from "../src/lark/shared/errors.js";
import {
  LarkChatService,
  LarkChatQueryError,
  type ChatMessageQuery,
} from "../src/lark/im/service/chat-service.js";
import { pollChats } from "../src/lark/im/service/poll.js";
const query = { start: "2026-09-29T00:00:00Z", end: "2026-09-29T00:05:00Z", excludeMuted: true };
const makeClient = (run: (args: readonly string[]) => string) =>
  LarkChatService.make((args) => Effect.sync(() => run(args)));

// Actor/session durability scenarios live in packages/integrations/test/im-session.test.ts.
test("IM searches recent messages before filtering muted chats and paginates the fixed window", async () => {
  const calls: (readonly string[])[] = [];
  const cli = makeClient((args) => {
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
  const batches = await Effect.runPromise(cli.searchMessages(query));
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
  await assert.rejects(
    Effect.runPromise(
      makeClient(() => '{"data":{"messages":[],"has_more":true}}').searchMessages(query),
    ),
    /token/,
  );
});
test("IM fails closed on incomplete mute settings and rejects repeated page tokens", async () => {
  const cli = makeClient((args) =>
    JSON.stringify({
      data: args.includes("batch_query")
        ? { items: [] }
        : {
            messages: [{ message_id: "m", chat_id: "c", create_time: "2026-09-29T00:01:00Z" }],
            has_more: false,
          },
    }),
  );
  await assert.rejects(Effect.runPromise(cli.searchMessages(query)), /Incomplete/);
  const repeated = makeClient(() =>
    JSON.stringify({ data: { messages: [], has_more: true, page_token: "same" } }),
  );
  await assert.rejects(Effect.runPromise(repeated.searchMessages(query)), /repeated/);
});
test("IM batches mute lookups by ten and skips settings for empty windows", async () => {
  const sizes: number[] = [];
  const cli = makeClient((args) => {
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
  const batches = await Effect.runPromise(cli.searchMessages(query));
  assert.deepEqual(sizes, [10, 10, 3]);
  assert.equal(batches.length, 22);
  let calls = 0;
  await Effect.runPromise(
    makeClient(() => {
      calls++;
      return '{"data":{"messages":[],"has_more":false}}';
    }).searchMessages(query),
  );
  assert.equal(calls, 1);
});
test("startup clamps old cursors to Beijing midnight while continuous polling crosses midnight", async () => {
  const windows: string[][] = [];
  const cli = {
    searchMessages: ({ start, end }: ChatMessageQuery) =>
      Effect.sync(() => {
        windows.push([start, end]);
        return [];
      }),
  };
  const now = Date.parse("2026-09-29T02:00:00Z");
  await Effect.runPromise(pollChats(cli, undefined, now));
  await Effect.runPromise(pollChats(cli, "2026-09-28T10:00:00Z", now));
  await Effect.runPromise(pollChats(cli, "2026-09-29T01:00:00Z", now));
  await Effect.runPromise(pollChats(cli, "2026-09-30T01:00:00Z", now));
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
  await Effect.runPromise(pollChats(cli, "2026-09-29T15:59:40Z", midnight, false));
  assert.equal(windows.at(-1)![0], "2026-09-29T15:58:40.000Z");
  await Effect.runPromise(pollChats(cli, "2026-09-29T15:59:40Z", midnight));
  assert.equal(windows.at(-1)![0], "2026-09-29T16:00:00.000Z");
  await assert.rejects(
    Effect.runPromise(
      pollChats(
        { searchMessages: () => Effect.fail(new LarkChatQueryError({ message: "offline" })) },
        undefined,
        now,
      ),
    ),
    /offline/,
  );
});

test("direct search forwards keywords, keeps exact bounds and deduplicates messages without mute queries", async () => {
  const calls: (readonly string[])[] = [];
  const service = makeClient((args) => {
    calls.push(args);
    const second = args.includes("--page-token");
    const message = { message_id: "m", chat_id: "c", create_time: "2026-09-29T00:00:00.500Z" };
    return JSON.stringify({
      data: {
        messages: second
          ? [{ ...message, content: "edited" }]
          : [
              { ...message, content: "original" },
              { ...message, message_id: "before", create_time: "2026-09-29T00:00:00Z" },
              { ...message, message_id: "end", create_time: "2026-09-29T00:00:01.500Z" },
            ],
        has_more: !second,
        page_token: "next",
      },
    });
  });
  const search = service.searchMessages({
    start: "2026-09-29T00:00:00.500Z",
    end: "2026-09-29T00:00:01.500Z",
    query: "release",
  });
  assert.equal(calls.length, 0);
  for (let i = 0; i < 2; i++) {
    const result = await Effect.runPromise(search);
    assert.deepEqual(
      result[0]?.messages.map(({ id, content }) => ({ id, content })),
      [{ id: "m", content: "edited" }],
    );
  }
  assert.equal(calls.length, 4);
  for (const args of calls) {
    assert.equal(args[1], "+messages-search");
    assert.equal(args[args.indexOf("--query") + 1], "release");
    assert.equal(args[args.indexOf("--start") + 1], "2026-09-29T00:00:00Z");
    assert.equal(args[args.indexOf("--end") + 1], "2026-09-29T00:00:02Z");
  }
});

test("settings queries deduplicate requested IDs and reject malformed or unrelated results", async () => {
  const service = makeClient((args) => {
    assert.deepEqual(JSON.parse(args[args.indexOf("--data") + 1]!), { chat_ids: ["a", "b"] });
    return JSON.stringify({
      data: {
        items: [
          { chat_id: "b", is_muted: false },
          { chat_id: "a", is_muted: true },
        ],
      },
    });
  });
  assert.deepEqual(await Effect.runPromise(service.getChatSettings(["a", "b", "a"])), [
    { chatId: "b", isMuted: false },
    { chatId: "a", isMuted: true },
  ]);
  assert.deepEqual(await Effect.runPromise(service.getChatSettings([])), []);
  for (const items of [
    [{ chat_id: "a", is_muted: "false" }],
    [{ chat_id: "other", is_muted: false }],
    [
      { chat_id: "a", is_muted: false },
      { chat_id: "a", is_muted: false },
    ],
  ]) {
    const bad = makeClient(() => JSON.stringify({ data: { items } }));
    const error = await Effect.runPromise(bad.getChatSettings(["a"]).pipe(Effect.flip));
    assert.equal(error._tag, "LarkChatQueryError");
  }
});

test("invalid windows and responses fail in the typed channel", async () => {
  const neverRun = LarkChatService.make(() => Effect.die("Invalid input reached CLI"));
  for (const bounds of [
    { start: "invalid", end: query.end },
    { start: query.end, end: query.start },
  ]) {
    const error = await Effect.runPromise(neverRun.searchMessages(bounds).pipe(Effect.flip));
    assert.equal(error._tag, "LarkChatQueryError");
  }
  for (const stdout of [
    "not JSON",
    '{"ok":false,"error":{"message":"denied"}}',
    '{"data":{"messages":{},"has_more":false}}',
    '{"data":{"messages":[{"message_id":"bad","chat_id":"chat","create_time":"2026-09-29T00:02:00Z","sender":{"name":123}}],"has_more":false}}',
  ]) {
    const error = await Effect.runPromise(
      makeClient(() => stdout)
        .searchMessages(query)
        .pipe(Effect.flip),
    );
    assert.equal(error._tag, "LarkChatQueryError");
  }
});

test("transport failures stay typed and cancellation interrupts the active CLI effect", async () => {
  const failure = new LarkCliError({ message: "offline", cause: "offline" });
  const failed = LarkChatService.make(() => Effect.fail(failure));
  assert.equal(await Effect.runPromise(failed.searchMessages(query).pipe(Effect.flip)), failure);
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const cancelled = yield* Deferred.make<void>();
        let calls = 0;
        const service = LarkChatService.make(() =>
          Effect.gen(function* () {
            calls++;
            yield* Deferred.succeed(entered, undefined);
            return yield* Effect.never;
          }).pipe(Effect.onInterrupt(() => Deferred.succeed(cancelled, undefined))),
        );
        const fiber = yield* service.searchMessages(query).pipe(Effect.forkScoped);
        yield* Deferred.await(entered);
        yield* Fiber.interrupt(fiber);
        yield* Deferred.await(cancelled);
        assert.equal(calls, 1);
      }),
    ),
  );
});

test("IM history forwards CLI time/order/cursor arguments and decodes one provider page", async () => {
  const calls: (readonly string[])[] = [];
  const service = makeClient((args) => {
    calls.push(args);
    return JSON.stringify({
      ok: true,
      data: {
        messages: [
          {
            message_id: "history",
            create_time: "2026-01-01T00:00:00Z",
            content: "Old message",
            sender: { name: "Alice", secret: "PRIVATE" },
            deleted: false,
          },
        ],
        has_more: true,
        page_token: "next",
      },
    });
  });
  const result = await Effect.runPromise(
    service.listMessages({
      chatId: "oc_old",
      start: "2026-01-01",
      end: "2026-01-02",
      order: "asc",
      pageSize: 10,
      pageToken: "first",
    }),
  );
  assert.deepEqual(calls, [
    [
      "im",
      "+chat-messages-list",
      "--as",
      "user",
      "--chat-id",
      "oc_old",
      "--order",
      "asc",
      "--page-size",
      "10",
      "--format",
      "json",
      "--no-reactions",
      "--start",
      "2026-01-01",
      "--end",
      "2026-01-02",
      "--page-token",
      "first",
    ],
  ]);
  assert.equal(result.hasMore, true);
  assert.equal(result.nextPageToken, "next");
  assert.equal(result.items[0]!.content, "Old message");
  assert.doesNotMatch(JSON.stringify(result), /PRIVATE/);
});

test("IM history rejects invalid selectors and pagination while preserving defects and cancellation", async () => {
  const service = LarkChatService.make(() => Effect.die("Invalid arguments reached CLI"));
  for (const args of [
    {},
    { chatId: "chat", userId: "user" },
    { chatId: "chat", pageSize: 51 },
    { chatId: "chat", start: "bad" },
    { chatId: "chat", start: "2026-02-02", end: "2026-02-01" },
  ]) {
    const error = await Effect.runPromise(service.listMessages(args).pipe(Effect.flip));
    assert.equal(error._tag, "LarkChatQueryError");
    assert.equal(error.kind, "invalid-input");
  }
  for (const page of [{ has_more: true }, { has_more: true, page_token: "first" }]) {
    const broken = makeClient(() => JSON.stringify({ data: { messages: [], ...page } }));
    await assert.rejects(
      Effect.runPromise(broken.listMessages({ chatId: "chat", pageToken: "first" })),
      /pagination/,
    );
  }
  await assert.rejects(Effect.runPromise(service.listMessages({ chatId: "chat" })), /reached CLI/);
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const started = yield* Deferred.make<void>(),
          released = yield* Deferred.make<void>();
        const slow = LarkChatService.make(() =>
          Deferred.succeed(started, undefined).pipe(
            Effect.andThen(Effect.never),
            Effect.ensuring(Deferred.succeed(released, undefined)),
          ),
        );
        const fiber = yield* slow.listMessages({ chatId: "chat" }).pipe(Effect.forkScoped);
        yield* Deferred.await(started);
        yield* Fiber.interrupt(fiber);
        yield* Deferred.await(released);
      }),
    ),
  );
});
