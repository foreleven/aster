import assert from "node:assert/strict";
import { test } from "node:test";
import { Effect, Layer } from "effect";
import { SystemOneClient } from "@aster/core";
import { ChatSummaryGate } from "@aster/integrations";
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
  );
  assert.equal(calls, 3);
});
