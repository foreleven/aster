import assert from "node:assert/strict";
import { test } from "node:test";
import { Effect, Redacted } from "effect";
import { normalizeContext } from "@earendil-works/pi-ai";
import { Models } from "../src/index.js";

const start = (index: number, id: string, input: unknown) => ({
  type: "content_block_start",
  index,
  content_block: { type: "tool_use", id, name: "lookup", input },
});
const delta = (index: number, partial_json: string) => ({
  type: "content_block_delta",
  index,
  delta: { type: "input_json_delta", partial_json },
});
const stop = (index: number) => ({ type: "content_block_stop", index });
const response = (
  blocks: readonly { readonly type: string; readonly [key: string]: unknown }[],
) => {
  const events = [
    {
      type: "message_start",
      message: {
        id: "test-message",
        type: "message",
        role: "assistant",
        content: [],
        model: "test-model",
        usage: { input_tokens: 1, output_tokens: 0 },
      },
    },
    ...blocks,
    { type: "message_delta", delta: { stop_reason: "tool_use" }, usage: { output_tokens: 1 } },
    { type: "message_stop" },
  ];
  return new Response(
    events.map((event) => `event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`).join(""),
    {
      headers: { "content-type": "text/event-stream" },
    },
  );
};
const parse = (
  blocks: readonly { readonly type: string; readonly [key: string]: unknown }[],
  provider: "anthropic" | "minimax" = "anthropic",
) =>
  Effect.runPromise(
    Models.use((models) =>
      Effect.gen(function* () {
        const resolved = yield* models.resolve("test");
        return yield* Effect.promise(async () => {
          const stream = await resolved.stream(
            resolved.model,
            normalizeContext({ messages: [{ role: "user", content: "Test", timestamp: 0 }] }),
            { apiKey: resolved.getApiKey(), fetch: async () => response(blocks), maxRetries: 0 },
          );
          return await stream.result();
        });
      }),
    ).pipe(
      Effect.provide(
        Models.layer([
          {
            name: "test",
            provider,
            model: "test-model",
            url: "https://unused.invalid",
            apiKey: Redacted.make("synthetic-key"),
          },
        ]),
      ),
    ),
  );

for (const provider of ["anthropic", "minimax"] as const) {
  test(`${provider}: complete initial tool input survives without JSON deltas`, async () => {
    const message = await parse(
      [start(0, "one", { instructions: "Find evidence" }), stop(0)],
      provider,
    );
    assert.equal(message.stopReason, "toolUse");
    assert.deepEqual(message.content, [
      {
        type: "toolCall",
        id: "one",
        name: "lookup",
        arguments: { instructions: "Find evidence" },
      },
    ]);
  });
}

test("Anthropic: split JSON deltas replace the initial input", async () => {
  const message = await parse([
    start(0, "one", { old: true }),
    delta(0, '{"instructions":'),
    delta(0, '"Read", "hints":["/apps"]}'),
    stop(0),
  ]);
  assert.equal(message.stopReason, "toolUse");
  const block = message.content[0];
  assert.ok(block?.type === "toolCall");
  assert.deepEqual(block.arguments, { instructions: "Read", hints: ["/apps"] });
});

test("Anthropic: interleaved tool blocks and mixed text retain separate inputs", async () => {
  const message = await parse([
    { type: "content_block_start", index: 0, content_block: { type: "text", text: "Investigate" } },
    start(1, "first", { source: "one" }),
    start(2, "second", {}),
    delta(2, '{"source":'),
    stop(1),
    delta(2, '"two"}'),
    stop(2),
    stop(0),
  ]);
  assert.equal(message.stopReason, "toolUse");
  assert.deepEqual(message.content, [
    { type: "text", text: "Investigate" },
    { type: "toolCall", id: "first", name: "lookup", arguments: { source: "one" } },
    { type: "toolCall", id: "second", name: "lookup", arguments: { source: "two" } },
  ]);
});

test("Anthropic: legitimate empty-argument calls remain valid", async () => {
  const message = await parse([start(0, "empty", {}), stop(0)]);
  assert.equal(message.stopReason, "toolUse");
  const block = message.content[0];
  assert.ok(block?.type === "toolCall");
  assert.deepEqual(block.arguments, {});
});

test("Anthropic: malformed or non-object arguments terminate safely", async () => {
  for (const blocks of [
    [start(0, "bad", {}), delta(0, '{"private":"sensitive'), stop(0)],
    [start(0, "bad", []), stop(0)],
    [start(0, "bad", null), stop(0)],
    [start(0, "bad", {}), delta(0, ""), stop(0)],
  ]) {
    const message = await parse(blocks);
    assert.equal(message.stopReason, "error");
    assert.equal(message.errorMessage, "Provider returned malformed tool arguments");
    assert.equal(message.content.length, 0);
  }
});
