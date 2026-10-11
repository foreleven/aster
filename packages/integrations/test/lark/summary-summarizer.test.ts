import { LanguageSettings } from "@aster/core";
import assert from "node:assert/strict";
import { test } from "node:test";
import { AgentRuntime } from "@aster/core/agent/ordinary";
import type { AgentTool } from "@aster/core/agent";
import { Effect } from "effect";
import { makeChatSummarizer } from "../../src/lark/im/summary/summarizer.js";
import { chatSummaryError } from "../../src/lark/im/summary/errors.js";
import { ChatSummaryError } from "../../src/lark/im/summary/errors.js";

const input = {
  path: "/lark/im/chats/team",
  chat: { id: "team", name: "Team", mode: "group", description: "Project A" },
  previous: {
    text: "Previous progress",
    references: [{ id: "old", url: "https://example.com/old" }],
  },
  messages: [
    {
      id: "new",
      at: "2026-10-09T00:00:00Z",
      content: "Release ready",
      sender: { name: "Alex", secret: "PRIVATE_SENDER" },
      url: "https://example.com/new",
      deleted: false,
    },
  ],
};

test("summary Agent uses Effect tools, validates evidence and retires callbacks", async () => {
  let tool: AgentTool | undefined;
  const summary = {
    text: "Release ready",
    references: [
      { id: "old", url: "https://example.com/old" },
      { id: "new", url: "https://example.com/new" },
    ],
  };
  const runner = AgentRuntime.make((request) =>
    Effect.promise(async () => {
      assert.equal(request.name, "summary-model");
      assert.equal(request.resultTool, "save_summary");
      assert.match(JSON.stringify(request.messages), /rolling work-chat summary/);
      assert.match(JSON.stringify(request.messages), /Project A/);
      assert.equal(JSON.stringify(request.messages).includes("PRIVATE_SENDER"), false);
      tool = request.tools![0]!;
      await assert.rejects(tool.execute("empty", { text: " ", references: [] }));
      await assert.rejects(
        tool.execute("invented", {
          text: "Ready",
          references: [{ id: "new", url: "https://invalid.example" }],
        }),
      );
      const accepted = await tool.execute("save", summary);
      assert.equal(accepted.terminate, true);
      return {
        messages: [
          {
            role: "toolResult" as const,
            toolCallId: "save",
            toolName: "save_summary",
            content: accepted.content,
            details: accepted.details,
            isError: false,
            timestamp: 0,
          },
        ],
      };
    }),
  );
  const result = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const summarizer = yield* makeChatSummarizer("summary-model").pipe(
          Effect.provide(LanguageSettings.layer),
        );
        return yield* summarizer.summarize(input);
      }).pipe(Effect.provideService(AgentRuntime, runner)),
    ),
  );
  assert.deepEqual(result, summary);
  await assert.rejects(tool!.execute("late", summary));
});

test("missing summary results remain typed failures", async () => {
  const result = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const summarizer = yield* makeChatSummarizer("summary-model").pipe(
          Effect.provide(LanguageSettings.layer),
        );
        return yield* summarizer.summarize(input).pipe(Effect.result);
      }).pipe(
        Effect.provideService(
          AgentRuntime,
          AgentRuntime.make(() => Effect.succeed({ messages: [] })),
        ),
      ),
    ),
  );
  assert.equal(result._tag, "Failure");
  if (result._tag === "Failure") {
    assert.equal(result.failure._tag, "ChatSummaryError");
    assert.match(result.failure.message, /no valid summary/);
  }
});

test("summary failures classify capacity and transient causes without retrying unknown or invalid output", () => {
  for (const message of [
    "context_length_exceeded",
    "maximum context length is 10000",
    "prompt is too long",
    "Request Entity Too Large",
  ])
    assert.equal(chatSummaryError(new Error(message)).kind, "capacity");
  for (const cause of [
    new Error("fetch failed"),
    new Error("rate limit exceeded"),
    { message: "service failure", statusCode: 503 },
    new Error("transport failed", { cause: { code: "ECONNRESET" } }),
  ])
    assert.equal(chatSummaryError(cause).kind, "transient");
  for (const cause of [
    new Error("Invalid API key"),
    { status: 401 },
    new Error("unrecognized error"),
  ])
    assert.equal(chatSummaryError(cause).kind, "permanent");
  const invalid = new ChatSummaryError({
    message: "Invalid summary result",
    cause: new Error("rate limit exceeded"),
  });
  assert.equal(chatSummaryError(invalid), invalid);
  assert.notEqual(chatSummaryError(invalid).kind, "transient");
});

import { ConfigProvider } from "effect";
for (const language of [undefined, "en-US"]) {
  test(`summary text uses output language configuration instead of prompt or evidence language: ${language ?? "default"}`, async () => {
    const expected = language ?? "zh-CN";
    const runner = AgentRuntime.make((request) =>
      Effect.sync(() => {
        const system = request.messages.find((message) => message.role === "system");
        assert.ok(
          system && JSON.stringify(system).includes(`Write natural-language output in ${expected}`),
        );
        assert.ok(!JSON.stringify(system).includes("summary in English"));
        return {
          messages: [
            {
              role: "toolResult" as const,
              toolCallId: "save",
              toolName: "save_summary",
              content: [],
              details: { text: "Summary", references: [] },
              isError: false,
              timestamp: 0,
            },
          ],
        };
      }),
    );
    await Effect.runPromise(
      Effect.gen(function* () {
        const summarizer = yield* makeChatSummarizer("summary-model").pipe(
          Effect.provide(LanguageSettings.layer),
        );
        yield* summarizer.summarize(input);
      }).pipe(
        Effect.provideService(AgentRuntime, runner),
        Effect.provideService(
          ConfigProvider.ConfigProvider,
          ConfigProvider.fromUnknown({ config: language ? { language } : {} }),
        ),
      ),
    );
  });
}
