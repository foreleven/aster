import { Effect } from "effect";
import assert from "node:assert/strict";
import { test } from "node:test";
import { makeExecutionGate, makeSignalExtractor, type SignalDefinition } from "../src/index.js";

const definition: SignalDefinition = {
  slug: "review",
  when: "A review is needed",
  task: "Review evidence",
  agent: "custom-agent",
  mode: "auto",
};
const context = {
  path: "/test",
  description: "Test evidence",
  state: { ready: true },
  messages: [],
};

test("execution decisions use injected capabilities for any executor and fail closed on non-choice answers", async () => {
  let allow = true;
  const gate = makeExecutionGate(
    {
      systemOne: (request) =>
        Effect.sync(() => {
          const input = JSON.parse(String(request.state));
          assert.equal(input.signal.agent, "custom-agent");
          assert.deepEqual(input.context, context);
          assert.equal(input.execution.capabilities, "Custom review capability");
          return {
            answers: {
              executable: allow
                ? { type: "choice", choice: "yes" }
                : { type: "text", choice: "yes" },
            },
          };
        }),
    },
    (agent) => ({
      supportedAgent: agent === "custom-agent",
      workspace: "Provided",
      capabilities: "Custom review capability",
    }),
  );
  assert.equal(
    await Effect.runPromise(gate(definition, context, { instructions: "Review", input: [] })),
    true,
  );
  allow = false;
  assert.equal(
    await Effect.runPromise(gate(definition, context, { instructions: "Review", input: [] })),
    false,
  );
});

test("Signal extraction validates model output independently of Codex and skips empty candidates", async () => {
  let calls = 0;
  let result: unknown = { triggeredSignalIds: ["review", "unknown", "review", 3] };
  const snapshot = { [context.path]: context };
  const extract = makeSignalExtractor({
    accessInstructions: ["Read through the test adapter"],
    run: (prompt, _schema, supplied) =>
      Effect.sync(() => {
        calls++;
        assert.ok(prompt.includes("Read through the test adapter"));
        assert.equal(supplied, snapshot);
        return result;
      }),
  });
  assert.deepEqual(await Effect.runPromise(extract(context.path, [], snapshot)), []);
  assert.equal(calls, 0);
  assert.deepEqual(await Effect.runPromise(extract(context.path, [definition], snapshot)), [
    "review",
  ]);
  result = {};
  await assert.rejects(
    Effect.runPromise(extract(context.path, [definition], snapshot)),
    /triggeredSignalIds/,
  );
});
