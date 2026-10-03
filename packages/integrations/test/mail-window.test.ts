import assert from "node:assert/strict";
import { test } from "node:test";
import { Effect, Exit } from "effect";
import { makeMailClient, mailDayStart, mailWindow } from "../src/index.js";

const midnight = Date.parse("2026-10-03T00:00:00+08:00");

test("mail search serializes second-precision timestamps with an explicit timezone", async () => {
  const cli = makeMailClient((args) =>
    Effect.sync(() => {
      const filter = JSON.parse(args[args.indexOf("--filter") + 1]!);
      // The search API specifies ISO 8601 timestamps accurate to the second.
      const secondsWithOffset = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[+-]\d{2}:\d{2}$/;
      assert.match(filter.time_range.start_time, secondsWithOffset);
      assert.match(filter.time_range.end_time, secondsWithOffset);
      assert.equal(Date.parse(filter.time_range.start_time), midnight);
      assert.equal(Date.parse(filter.time_range.end_time), midnight + 3_600_000 + 1000);
      return JSON.stringify({ messages: [], has_more: false });
    }),
  );
  assert.deepEqual(
    await Effect.runPromise(cli.listIds("me", midnight + 123, midnight + 3_600_000 + 789)),
    [],
  );
});

test("mail windows catch up today in hours, overlap, and finish yesterday across midnight", () => {
  const now = midnight + 3 * 3_600_000;
  assert.equal(mailDayStart(now), midnight);
  assert.deepEqual(mailWindow(undefined, now, midnight), {
    start: midnight,
    through: midnight + 3_600_000,
    caughtUp: false,
  });
  assert.deepEqual(mailWindow(midnight + 3_600_000, now, midnight), {
    start: midnight + 3_600_000 - 60_000,
    through: midnight + 2 * 3_600_000 - 60_000,
    caughtUp: false,
  });
  const tomorrow = midnight + 86_400_000;
  assert.deepEqual(mailWindow(tomorrow - 30_000, tomorrow + 30_000, midnight), {
    start: tomorrow - 90_000,
    through: tomorrow + 30_000,
    caughtUp: true,
  });
  assert.equal(
    mailWindow(undefined, tomorrow + 30_000, mailDayStart(tomorrow + 30_000)).start,
    tomorrow,
  );
  assert.equal(mailWindow(now + 1, now, midnight).start, midnight);
  assert.deepEqual(mailWindow(undefined, midnight, midnight), {
    start: midnight,
    through: midnight,
    caughtUp: true,
  });
});

test("mail time filter stays fixed across all pages and does not truncate at 100", async () => {
  const calls: ReadonlyArray<string>[] = [];
  const cli = makeMailClient((args) =>
    Effect.sync(() => {
      calls.push(args);
      const page = calls.length;
      return JSON.stringify({
        ok: true,
        data: {
          messages: Array.from({ length: page === 1 ? 100 : 2 }, (_, i) => ({
            message_id: String(page === 1 ? i : 99 + i),
          })),
          has_more: page === 1,
          page_token: page === 1 ? "search:next" : "",
        },
      });
    }),
  );
  const result = await Effect.runPromise(cli.listIds("me", midnight, midnight + 3_600_000));
  assert.equal(result.length, 101);
  assert.equal(result.at(-1), "100");
  assert.equal(calls.length, 2);
  for (const args of calls) {
    assert.deepEqual(JSON.parse(args[args.indexOf("--filter") + 1]!), {
      folder: "inbox",
      time_range: {
        start_time: "2026-10-02T16:00:00+00:00",
        end_time: "2026-10-02T17:00:00+00:00",
      },
    });
    assert.equal(args[args.indexOf("--max") + 1], "100");
  }
  assert.equal(calls[0]!.includes("--page-token"), false);
  assert.equal(calls[1]!.at(-1), "search:next");
});

for (const page of [
  { messages: [], has_more: true },
  { messages: [{ message_id: "" }], has_more: false },
  { messages: [], has_more: true, page_token: "search:repeated" },
])
  test(`mail rejects unsafe pagination: ${JSON.stringify(page)}`, async () => {
    let calls = 0;
    const cli = makeMailClient(() =>
      Effect.sync(() => {
        calls++;
        return JSON.stringify(page);
      }),
    );
    const result = await Effect.runPromise(
      Effect.result(cli.listIds("me", midnight, midnight + 1000)),
    );
    assert.equal(result._tag, "Failure");
    if (result._tag === "Failure") assert.equal(result.failure._tag, "LarkResponseError");
    assert.ok(calls <= 2);
  });

test("mail preserves transport defects instead of converting them to a successful empty window", async () => {
  const cli = makeMailClient(() => Effect.die("transport defect"));
  const result = await Effect.runPromiseExit(cli.listIds("me", midnight, midnight + 1000));
  assert.ok(Exit.isFailure(result));
});
