import assert from "node:assert/strict";
import { test } from "node:test";
import { Cause, Effect, Fiber, Layer } from "effect";
import { createAssistantMessageEventStream, type AssistantMessage } from "@earendil-works/pi-ai";
import {
  Agent,
  AgentError,
  Models,
  Type,
  type AgentMessage,
  type AgentTool,
  type StreamFn,
} from "../src/index.js";

const model = {
  id: "test",
  name: "test",
  provider: "test",
  api: "openai-completions" as const,
  baseUrl: "http://unused",
  reasoning: false,
  input: ["text" as const],
  contextWindow: 1000,
  maxTokens: 100,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
};
const assistant = (overrides: Partial<AssistantMessage> = {}): AssistantMessage => ({
  role: "assistant",
  content: [{ type: "text", text: "done" }],
  api: "openai-completions",
  provider: "test",
  model: "test",
  stopReason: "stop",
  timestamp: 1,
  usage: {
    input: 0,
    output: 0,
    cacheRead: 0,
    cacheWrite: 0,
    totalTokens: 0,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
  },
  ...overrides,
});
const respond = (message: AssistantMessage) => {
  const stream = createAssistantMessageEventStream();
  if (message.stopReason === "error" || message.stopReason === "aborted")
    stream.push({ type: "error", reason: message.stopReason, error: message });
  else if (message.stopReason !== "pending")
    stream.push({ type: "done", reason: message.stopReason, message });
  return stream;
};
const models = (stream: StreamFn) =>
  Layer.succeed(Models, {
    resolve: () => Effect.succeed({ model, stream, getApiKey: () => "test" }),
  });
const input = (text: string): AgentMessage[] => [
  { role: "system", content: "Test", timestamp: 0 },
  { role: "user", content: [{ type: "text", text }], timestamp: 0 },
];

test("reusing an Agent isolates concurrent runs and returns only generated messages", async () => {
  const seen: unknown[] = [];
  const result = await Effect.runPromise(
    Effect.gen(function* () {
      const agent = yield* Agent.make({ name: "test" });
      return yield* Effect.all(
        [agent.run({ messages: input("one") }), agent.run({ messages: input("two") })],
        { concurrency: "unbounded" },
      );
    }).pipe(
      Effect.provide(
        models((_model, context) => {
          seen.push(context.messages);
          return respond(assistant());
        }),
      ),
    ),
  );
  assert.equal(seen.length, 2);
  assert.ok(seen.every((messages) => Array.isArray(messages) && messages.length === 2));
  assert.ok(
    result.every((run) => run.messages.length === 1 && run.messages[0]!.role === "assistant"),
  );
});

test("terminal pi error messages become typed failures with the generated transcript", async () => {
  const failure = await Effect.runPromise(
    Effect.gen(function* () {
      const agent = yield* Agent.make({ name: "test" });
      return yield* agent.run({ messages: input("fail") }).pipe(Effect.flip);
    }).pipe(
      Effect.provide(
        models(() => respond(assistant({ stopReason: "error", errorMessage: "provider failed" }))),
      ),
    ),
  );
  assert.ok(failure instanceof AgentError);
  assert.equal(failure.message, "provider failed");
  assert.equal(failure.messages.length, 1);
});

test("tools are declared with an existing system message and tool errors remain recoverable", async () => {
  let requests = 0;
  const result = await Effect.runPromise(
    Effect.gen(function* () {
      const parameters = Type.Object({ value: Type.Number() });
      const save: AgentTool<typeof parameters> = {
        name: "save",
        label: "Save",
        description: "Save",
        parameters,
        execute: async (_id, args) => {
          if (args.value === 1) throw new Error("try again");
          return {
            content: [{ type: "text", text: "saved" }],
            details: { value: args.value },
            terminate: true,
          };
        },
      };
      const agent = yield* Agent.make({ name: "test", tools: [save] });
      return yield* agent.run({ messages: input("save") });
    }).pipe(
      Effect.provide(
        models((_model, context) => {
          const system = context.messages[0];
          assert.ok(
            system?.role === "system" && system.toolsAdded?.some((tool) => tool.name === "save"),
          );
          if (++requests === 2)
            assert.ok(
              context.messages.some((message) => message.role === "toolResult" && message.isError),
            );
          return respond(
            assistant({
              stopReason: "toolUse",
              content: [
                {
                  type: "toolCall",
                  id: String(requests),
                  name: "save",
                  arguments: { value: requests },
                },
              ],
            }),
          );
        }),
      ),
    ),
  );
  const results = result.messages.filter((message) => message.role === "toolResult");
  assert.equal(requests, 2);
  assert.equal(results[0]!.isError, true);
  assert.equal(results[1]!.isError, false);
  assert.deepEqual(results[1]!.details, { value: 2 });
});

test("Effect interruption aborts pi and waits for it to become idle", async () => {
  let entered = false;
  let aborted = false;
  const exit = await Effect.runPromise(
    Effect.gen(function* () {
      const agent = yield* Agent.make({ name: "test" });
      const fiber = yield* agent.run({ messages: input("wait") }).pipe(Effect.forkChild);
      while (!entered) yield* Effect.yieldNow;
      yield* Fiber.interrupt(fiber);
      return yield* Fiber.await(fiber);
    }).pipe(
      Effect.provide(
        models((_model, _context, options) => {
          const stream = createAssistantMessageEventStream();
          options?.signal?.addEventListener(
            "abort",
            () => {
              aborted = true;
              stream.push({
                type: "error",
                reason: "aborted",
                error: assistant({ stopReason: "aborted", errorMessage: "cancelled" }),
              });
            },
            { once: true },
          );
          entered = true;
          return stream;
        }),
      ),
      Effect.timeout("2 seconds"),
    ),
  );
  assert.equal(aborted, true);
  assert.equal(exit._tag, "Failure");
  if (exit._tag === "Failure") assert.equal(Cause.hasInterruptsOnly(exit.cause), true);
});

test("unknown models fail at Agent.make without a provider request", async () => {
  const failure = await Effect.runPromise(
    Agent.make({ name: "missing" }).pipe(Effect.flip, Effect.provide(Models.layer([]))),
  );
  assert.ok(failure instanceof AgentError);
  assert.match(failure.message, /Unknown model/);
});

test("required result gets one correction in the same conversation", async () => {
  let calls = 0;
  const tool: AgentTool = {
    name: "submit_result",
    label: "Result",
    description: "Result",
    parameters: Type.Object({ ok: Type.Boolean() }),
    execute: async () => ({
      content: [{ type: "text", text: "ok" }],
      details: { ok: true },
      terminate: true,
    }),
  };
  const result = await Effect.runPromise(
    Effect.gen(function* () {
      const agent = yield* Agent.make({ name: "test", tools: [tool], resultTool: "submit_result" });
      return yield* agent.run({ messages: input("return a result") });
    }).pipe(
      Effect.provide(
        models((_model, context) => {
          calls++;
          if (calls === 1) return respond(assistant());
          assert.ok(context.messages.some((m) => m.role === "assistant"));
          return respond(
            assistant({
              stopReason: "toolUse",
              content: [
                { type: "toolCall", id: "result", name: "submit_result", arguments: { ok: true } },
              ],
            }),
          );
        }),
      ),
    ),
  );
  assert.equal(calls, 2);
  assert.ok(result.messages.some((m) => m.role === "toolResult" && !m.isError));
});

test("missing results are bounded and token truncation is diagnosed without retry", async () => {
  const tool: AgentTool = {
    name: "submit_result",
    label: "Result",
    description: "Result",
    parameters: Type.Object({}),
    execute: async () => ({ content: [], details: {}, terminate: true }),
  };
  for (const stopReason of ["stop", "length"] as const) {
    let calls = 0;
    const failure = await Effect.runPromise(
      Effect.gen(function* () {
        const agent = yield* Agent.make({
          name: "test",
          tools: [tool],
          resultTool: "submit_result",
        });
        return yield* agent.run({ messages: input("return a result") }).pipe(Effect.flip);
      }).pipe(
        Effect.provide(
          models(() => {
            calls++;
            return respond(assistant({ stopReason }));
          }),
        ),
      ),
    );
    assert.equal(calls, stopReason === "length" ? 1 : 2);
    assert.match(
      failure.message,
      stopReason === "length" ? /length; inputTokens=/ : /after one correction/,
    );
  }
});

test("message persistence is awaited before tools and context guards apply to subsequent provider calls", async () => {
  const persisted: AgentMessage[] = [];
  let requests = 0,
    guardCalls = 0;
  const result = await Effect.runPromise(
    Effect.gen(function* () {
      const agent = yield* Agent.make({
        name: "test",
        onMessage: async (message) => {
          await Promise.resolve();
          persisted.push(message);
        },
        transformContext: async (messages) => {
          guardCalls++;
          if (messages.some((m) => m.role === "toolResult"))
            throw new Error("context budget reached");
          return messages;
        },
        tools: [
          {
            name: "read",
            label: "read",
            description: "read",
            parameters: Type.Object({}),
            execute: async () => {
              assert.equal(persisted.at(-1)?.role, "assistant");
              return { content: [{ type: "text", text: "evidence" }], details: undefined };
            },
          },
        ],
      });
      return yield* agent.run({ messages: input("read") }).pipe(Effect.flip);
    }).pipe(
      Effect.provide(
        models(() => {
          requests++;
          return respond(
            assistant({
              stopReason: "toolUse",
              content: [{ type: "toolCall", id: "read-1", name: "read", arguments: {} }],
            }),
          );
        }),
      ),
    ),
  );
  assert.equal(requests, 1);
  assert.ok(guardCalls >= 2);
  assert.ok(persisted.some((m) => m.role === "toolResult"));
  assert.match(result.message, /context budget/);
});
