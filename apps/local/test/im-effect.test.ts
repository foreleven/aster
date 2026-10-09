import assert from "node:assert/strict";
import { test } from "node:test";
import { Clock, Effect } from "effect";
import { TestClock } from "effect/testing";
import { makeImSummaryGate } from "@aster/integrations";
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

// Cancellation and stale-work protection are exercised in the integration Session tests.
test("summary screening reads the caller Clock without creating a second runtime", async () => {
  const gate = makeImSummaryGate({
    systemOne: () =>
      Clock.currentTimeMillis.pipe(
        Effect.map((now) => ({
          answers: { summarize: { type: "choice" as const, choice: now === 123 ? "yes" : "no" } },
        })),
      ),
  });
  const result = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const clock = yield* TestClock.make();
        yield* clock.adjust(123);
        return yield* gate
          .needed({ path, chat, messages: [message] })
          .pipe(Effect.provideService(Clock.Clock, clock));
      }),
    ),
  );
  assert.equal(result, true);
});
