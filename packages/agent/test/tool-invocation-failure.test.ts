import assert from "node:assert/strict";
import { test } from "node:test";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createAssistantMessageEventStream } from "@earendil-works/pi-ai";
import { Effect, Layer, Option, Schema } from "effect";
import {
  Models,
  schemaTool,
  ToolInvocationFailure,
  type AssistantMessage,
  type ResolvedModel,
} from "../src/index.js";
import { AgentRunner } from "../src/agent/index.js";
import { AgentConversations, DurableHarness } from "../src/harness/index.js";

const response = (id: string): AssistantMessage => ({
  role: "assistant",
  content: [{ type: "toolCall", id, name: "submit", arguments: {} }],
  api: "openai-completions",
  provider: "test",
  model: "test",
  timestamp: 0,
  stopReason: "toolUse",
  usage: {
    input: 0,
    output: 0,
    cacheRead: 0,
    cacheWrite: 0,
    totalTokens: 0,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
  },
});
const model: ResolvedModel["model"] = {
  id: "test",
  name: "test",
  provider: "test",
  api: "openai-completions",
  baseUrl: "http://unused",
  reasoning: false,
  input: ["text"],
  contextWindow: 1000,
  maxTokens: 100,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
};

for (const boundary of ["agent", "harness"] as const) {
  test(`${boundary}: uncertainty stops real Pi reasoning before a fresh call identity`, async () => {
    let requests = 0;
    const executed: string[] = [];
    const tool = schemaTool({
      name: "submit",
      label: "Submit",
      description: "Submit",
      replay: "never",
      parameters: Schema.Struct({}),
      execute: (callId) =>
        Effect.gen(function* () {
          executed.push(callId);
          return yield* new ToolInvocationFailure({
            message: "Submission receipt is missing",
            outcome: "unknown",
            operationId: "original-operation",
          });
        }),
    });
    const models = Layer.succeed(Models, {
      resolve: () =>
        Effect.succeed({
          model,
          getApiKey: () => "synthetic-key",
          stream: () => {
            requests++;
            const stream = createAssistantMessageEventStream();
            if (requests > 2) {
              stream.push({
                type: "done",
                reason: "stop",
                message: {
                  ...response("unused"),
                  content: [{ type: "text", text: "Stopped fixture retry" }],
                  stopReason: "stop",
                },
              });
              return stream;
            }
            stream.push({ type: "done", reason: "toolUse", message: response(`call-${requests}`) });
            return stream;
          },
        }),
    });
    const work =
      boundary === "agent"
        ? AgentRunner.use((runner) =>
            runner.run({
              name: "test",
              messages: [{ role: "user", content: "Submit", timestamp: 0 }],
              tools: [tool],
            }),
          ).pipe(Effect.asVoid, Effect.provide(AgentRunner.layer.pipe(Layer.provide(models))))
        : DurableHarness.use((harness) =>
            harness.withConversation(
              {
                name: "test",
                owner: "/test/uncertain",
                instructions: "Submit",
                tools: [tool],
              },
              (conversation) =>
                conversation
                  .submit({ requestId: "input-one", content: "Submit" })
                  .pipe(Effect.flatMap((submission) => submission.wait)),
            ),
          ).pipe(
            Effect.asVoid,
            Effect.provide(
              DurableHarness.layer.pipe(Layer.provide([models, AgentConversations.memory])),
            ),
          );
    const failure = await Effect.runPromise(work.pipe(Effect.flip, Effect.timeout("5 seconds")));
    assert.equal(failure.outcome, "unknown");
    assert.equal(failure.operationId, "original-operation");
    assert.equal(failure.message, "Submission receipt is missing");
    assert.deepEqual(executed, ["call-1"]);
    assert.equal(requests, 1);
  });
}

test("reopened native submission retains terminal uncertainty without another model or tool call", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "aster-native-uncertain-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  let requests = 0;
  let calls = 0;
  const tool = schemaTool({
    name: "submit",
    label: "Submit",
    description: "Submit",
    replay: "never",
    parameters: Schema.Struct({}),
    execute: () =>
      Effect.gen(function* () {
        calls++;
        return yield* new ToolInvocationFailure({
          message: "Receipt missing",
          outcome: "unknown",
          operationId: "retained-operation",
        });
      }),
  });
  const models = Layer.succeed(Models, {
    resolve: () =>
      Effect.succeed({
        model,
        getApiKey: () => "synthetic-key",
        stream: () => {
          requests++;
          const stream = createAssistantMessageEventStream();
          stream.push({ type: "done", reason: "toolUse", message: response("original-call") });
          return stream;
        },
      }),
  });
  const layer = DurableHarness.layer.pipe(
    Layer.provide([models, AgentConversations.layer(directory)]),
  );
  const options = {
    name: "test",
    owner: "/test/reopened-uncertain",
    instructions: "Submit",
    tools: [tool],
  };
  const initial = await Effect.runPromise(
    DurableHarness.use((harness) =>
      harness.withConversation(options, (conversation) =>
        conversation
          .submit({ requestId: "original-input", content: "Submit" })
          .pipe(Effect.flatMap((submission) => submission.wait)),
      ),
    ).pipe(Effect.provide(layer), Effect.flip, Effect.timeout("5 seconds")),
  );
  assert.equal(initial.outcome, "unknown");
  const recovered = await Effect.runPromise(
    DurableHarness.use((harness) =>
      harness.withConversation(options, (conversation) =>
        Effect.gen(function* () {
          const saved = yield* conversation.submission("original-input");
          assert.ok(Option.isSome(saved));
          const first = yield* saved.value.wait.pipe(Effect.flip);
          const deduplicated = yield* conversation.submit({
            requestId: "original-input",
            content: "Submit",
          });
          const second = yield* deduplicated.wait.pipe(Effect.flip);
          return [first, second];
        }),
      ),
    ).pipe(Effect.provide(layer), Effect.timeout("5 seconds")),
  );
  for (const failure of recovered) {
    assert.equal(failure.outcome, "unknown");
    assert.equal(failure.operationId, "retained-operation");
    assert.equal(failure.message, "Receipt missing");
  }
  assert.equal(requests, 1);
  assert.equal(calls, 1);
  const rejected = await Effect.runPromise(
    AgentConversations.use((conversations) =>
      conversations.resolveFailure(options.owner, "original-input", "wrong-operation"),
    ).pipe(Effect.provide(AgentConversations.layer(directory)), Effect.flip),
  );
  assert.equal(rejected.kind, "conflict");
  const stillUnknown = await Effect.runPromise(
    DurableHarness.use((harness) =>
      harness.withConversation(options, (conversation) =>
        conversation
          .submit({
            requestId: "original-input",
            content: "Submit",
          })
          .pipe(Effect.flatMap((submission) => submission.wait)),
      ),
    ).pipe(Effect.provide(layer), Effect.flip),
  );
  assert.equal(stillUnknown.outcome, "unknown");
  // Trusted receipt resolution needs storage only, without acquiring any Models service.
  await Effect.runPromise(
    AgentConversations.use((conversations) =>
      Effect.gen(function* () {
        yield* conversations.resolveFailure(options.owner, "original-input", "retained-operation");
        yield* conversations.resolveFailure(options.owner, "original-input", "retained-operation");
      }),
    ).pipe(Effect.provide(AgentConversations.layer(directory))),
  );
  const resolved = await Effect.runPromise(
    DurableHarness.use((harness) =>
      harness.withConversation(options, (conversation) =>
        Effect.gen(function* () {
          const saved = yield* conversation.submission("original-input");
          assert.ok(Option.isSome(saved));
          const failure = yield* saved.value.wait.pipe(Effect.flip);
          yield* conversation.resolveFailure("original-input", "retained-operation");
          return failure;
        }),
      ),
    ).pipe(Effect.provide(layer)),
  );
  assert.equal(resolved.outcome, "failed");
  assert.equal(resolved.operationId, "retained-operation");
  assert.match(resolved.message, /reconciled/);
  assert.equal(requests, 1);
  assert.equal(calls, 1);
});
