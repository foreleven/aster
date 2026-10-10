import assert from "node:assert/strict";
import { test } from "node:test";
import { Clock, Effect } from "effect";
import { TestClock } from "effect/testing";
import { makeImSummaryGate } from "../../src/lark/testing.js";
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

test("System One handles sparse important messages and rejects invalid decisions", async () => {
  let captured = "";
  const gate = makeImSummaryGate({
    systemOne: (request) =>
      Effect.sync(() => {
        captured = JSON.stringify(request);
        return { answers: { summarize: { type: "choice", choice: "yes" } } };
      }),
  });
  assert.equal(
    await Effect.runPromise(Effect.scoped(gate.needed({ path, chat, messages: [message] }))),
    true,
  );
  assert.match(captured, /single important message/);
  const invalid = makeImSummaryGate({ systemOne: () => Effect.sync(() => ({ answers: {} })) });
  await assert.rejects(
    Effect.runPromise(Effect.scoped(invalid.needed({ path, chat, messages: [message] }))),
    /valid summary decision/,
  );
});

import { Layer } from "effect";
import { SystemOneClient } from "@aster/core";
import { ChatSummaryGate } from "../../src/lark/im/summary/gate.js";
test("Lark IM builds its summary gate from the injected global System One client", async () => {
  const input = {
    path: "/lark/im/chats/test",
    chat: { id: "test", name: "Test", mode: "group", description: "" },
    messages: [],
  };
  let answer = "no";
  let calls = 0;
  const client = Layer.succeed(SystemOneClient, {
    systemOne: (request) =>
      Effect.sync(() => {
        calls++;
        assert.equal(request.state, JSON.stringify(input));
        assert.ok(request.questions.summarize);
        return { answers: { summarize: { type: "choice", choice: answer } } };
      }),
  });
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const gate = yield* ChatSummaryGate;
        assert.equal(yield* gate.needed(input), false);
        answer = "yes";
        assert.equal(yield* gate.needed(input), true);
        answer = "invalid";
        const result = yield* Effect.result(gate.needed(input));
        assert.equal(result._tag, "Failure");
        if (result._tag === "Failure")
          assert.match(result.failure.message, /no valid summary decision/);
      }).pipe(Effect.provide(ChatSummaryGate.layer.pipe(Layer.provide(client)))),
    ),
  );
  assert.equal(calls, 3);
});

test("summary admission sends readable sender identity without opaque provider fields", async () => {
  const secret = "PRIVATE_LARK_SENTINEL";
  const chat = { id: "chat", name: "Project", mode: "group", description: "Project group" };
  const message = {
    id: "message",
    at: "2026-10-02T00:00:00Z",
    content: "Release ready",
    url: "https://example.com/message",
    deleted: false,
    sender: { id: "user", name: "Alice", token: secret },
  };
  let input = "";
  const gate = makeImSummaryGate({
    systemOne: (request) =>
      Effect.sync(() => {
        input = typeof request.state === "string" ? request.state : JSON.stringify(request.state);
        return { answers: { summarize: { type: "choice", choice: "yes" } } };
      }),
  });
  assert.equal(
    await Effect.runPromise(
      Effect.scoped(gate.needed({ path: "/lark/im/chats/chat", chat, messages: [message] })),
    ),
    true,
  );
  assert.equal(input.includes(secret), false);
  assert.ok(input.includes("Alice") && input.includes("Release ready"));
});
