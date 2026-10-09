import { Effect } from "effect";
import assert from "node:assert/strict";
import { test } from "node:test";
import { makeMailClient } from "../src/lark/mail/client.js";

test("Mail query uses CLI full-text search and exact filters without fetching bodies", async () => {
  const calls: (readonly string[])[] = [];
  const client = makeMailClient((args) =>
    Effect.sync(() => {
      calls.push(args);
      return JSON.stringify({
        ok: true,
        data: {
          messages: [
            {
              message_id: "email",
              date: "Fri, 9 Oct 2026 12:00:00 +0800",
              from: "Alice",
              subject: "Budget",
              labels: "UNREAD",
              private: "PRIVATE",
            },
          ],
          has_more: true,
          page_token: "search:next",
        },
      });
    }),
  );
  const result = await Effect.runPromise(
    client.listMessages("me", {
      query: "budget",
      folder: "sent",
      from: "alice@example.com, bob@example.com",
      to: "team@example.com",
      isUnread: false,
      hasAttachment: true,
      start: "2026-10-01T00:00:00+08:00",
      end: "2026-10-09T00:00:00+08:00",
      limit: 25,
      pageToken: "search:first",
    }),
  );
  assert.equal(calls.length, 1);
  const args = calls[0]!;
  assert.deepEqual(JSON.parse(args[args.indexOf("--filter") + 1]!), {
    folder: "sent",
    from: ["alice@example.com", "bob@example.com"],
    to: ["team@example.com"],
    is_unread: false,
    has_attachment: true,
    time_range: { start_time: "2026-10-01T00:00:00+08:00", end_time: "2026-10-09T00:00:00+08:00" },
  });
  assert.equal(args[args.indexOf("--query") + 1], "budget");
  assert.equal(args[args.indexOf("--max") + 1], "25");
  assert.equal(args[args.indexOf("--page-token") + 1], "search:first");
  assert.equal(result.items[0]!.messageId, "email");
  assert.equal(result.nextPageToken, "search:next");
  assert.doesNotMatch(JSON.stringify(result), /PRIVATE|body/);
});

test("Mail list validation and incomplete cursor responses fail before reporting complete history", async () => {
  const noRun = makeMailClient(() => Effect.die("Invalid input reached CLI"));
  for (const args of [
    { limit: 401 },
    { start: "bad" },
    { start: "2026-10-02T00:00:00Z", end: "2026-10-01T00:00:00Z" },
    { query: "x".repeat(51) },
  ]) {
    const error = await Effect.runPromise(noRun.listMessages("me", args).pipe(Effect.flip));
    assert.equal(error._tag, "LarkResponseError");
    if (error._tag === "LarkResponseError") assert.equal(error.kind, "invalid-input");
  }
  for (const page of [{ has_more: true }, { has_more: true, page_token: "first" }]) {
    const client = makeMailClient(() =>
      Effect.succeed(JSON.stringify({ data: { messages: [], ...page } })),
    );
    await assert.rejects(
      Effect.runPromise(client.listMessages("me", { pageToken: "first" })),
      /pagination/,
    );
  }
  await assert.rejects(Effect.runPromise(noRun.listMessages("me", {})), /reached CLI/);
});
