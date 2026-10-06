import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import {
  createAssistantMessageEventStream,
  Type,
  type AssistantMessage,
} from "@earendil-works/pi-ai";
import { Cause, Deferred, Effect, Fiber, Layer, Schema } from "effect";
import { Harness } from "@earendil-works/pi-durable";
import { Agent, AgentError, Models, PiStorageLease, rejectedToolResult } from "../src/index.js";

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

const assistant = (): AssistantMessage => ({
  role: "assistant",
  content: [{ type: "text", text: "durable answer" }],
  api: "openai-completions",
  provider: "test",
  model: "test",
  stopReason: "stop",
  timestamp: Date.now(),
  usage: {
    input: 0,
    output: 0,
    cacheRead: 0,
    cacheWrite: 0,
    totalTokens: 0,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
  },
});

test("durable response observers run before tools and do not replay settled responses", async () => {
  const directory = await mkdtemp(join(tmpdir(), "aster-pi-response-"));
  const events: string[] = [];
  let calls = 0;
  const layer = Layer.succeed(Models, {
    resolve: () =>
      Effect.succeed({
        model,
        getApiKey: () => "test",
        stream: () => {
          const message = assistant();
          if (++calls === 1) {
            message.stopReason = "toolUse";
            message.content = [
              { type: "thinking", thinking: "Inspect the current state" },
              { type: "toolCall", id: "read-1", name: "read", arguments: {} },
            ];
          }
          const stream = createAssistantMessageEventStream();
          stream.push({ type: "done", reason: calls === 1 ? "toolUse" : "stop", message });
          return stream;
        },
      }),
  });
  const run = Effect.gen(function* () {
    const agent = yield* Agent.make({
      name: "test",
      durable: { sessionId: "response", requestId: "one", storageDirectory: directory },
      onResponse: (message, signal) => {
        assert.equal(signal?.aborted, false);
        events.push(`response:${message.stopReason}`);
        if (message.stopReason === "toolUse")
          assert.deepEqual(message.content[0], {
            type: "thinking",
            thinking: "Inspect the current state",
          });
      },
      onMessage: () => {
        events.push("settled");
      },
      tools: [
        {
          name: "read",
          label: "Read",
          description: "Read",
          replay: "safe",
          parameters: Type.Object({}),
          execute: async () => {
            assert.deepEqual(events, ["response:toolUse"]);
            events.push("tool");
            return { content: [{ type: "text", text: "state" }], details: {} };
          },
        },
      ],
    });
    return yield* agent.run({ messages: [{ role: "user", content: "Read", timestamp: 0 }] });
  }).pipe(Effect.provide(layer));
  try {
    const first = await Effect.runPromise(run);
    assert.deepEqual(events, [
      "response:toolUse",
      "tool",
      "response:stop",
      "settled",
      "settled",
      "settled",
    ]);
    events.length = 0;
    assert.deepEqual(await Effect.runPromise(run), first);
    assert.deepEqual(events, ["settled", "settled", "settled"]);
    assert.equal(calls, 2);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("durable collection paginates a long run and applies the context guard every round", async () => {
  let calls = 0;
  let guards = 0;
  const layer = Layer.succeed(Models, {
    resolve: () =>
      Effect.succeed({
        model,
        getApiKey: () => "test",
        stream: (_model, context) => {
          calls++;
          assert.ok(
            context.messages.some(
              (message) => message.role === "user" && message.content === "guarded",
            ),
          );
          const stream = createAssistantMessageEventStream();
          const message: AssistantMessage =
            calls <= 55
              ? {
                  ...assistant(),
                  stopReason: "toolUse",
                  content: [{ type: "toolCall", id: `call-${calls}`, name: "read", arguments: {} }],
                }
              : assistant();
          stream.push({ type: "done", reason: calls <= 55 ? "toolUse" : "stop", message });
          return stream;
        },
      }),
  });
  const directory = await mkdtemp(join(tmpdir(), "aster-pi-pages-"));
  try {
    const run = Effect.gen(function* () {
      const agent = yield* Agent.make({
        name: "test",
        durable: { sessionId: "pages", requestId: "long-run", storageDirectory: directory },
        transformContext: async (messages) => {
          guards++;
          return [...messages, { role: "user", content: "guarded", timestamp: 0 }];
        },
        tools: [
          {
            name: "read",
            label: "Read",
            description: "Read",
            parameters: Type.Object({}),
            replay: "safe",
            execute: async () => ({ content: [{ type: "text", text: "read" }], details: {} }),
          },
        ],
      });
      return yield* agent.run({ messages: [{ role: "user", content: "Read", timestamp: 0 }] });
    }).pipe(Effect.provide(layer));
    const first = await Effect.runPromise(run);
    assert.equal(first.messages.length, 111);
    assert.equal(guards, calls);
    assert.equal(calls, 56);
    assert.deepEqual(await Effect.runPromise(run), first);
    assert.equal(calls, 56);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("durable context guard failure prevents a provider request despite native hook recovery", async () => {
  const directory = await mkdtemp(join(tmpdir(), "aster-pi-context-guard-"));
  let calls = 0;
  const layer = Layer.succeed(Models, {
    resolve: () =>
      Effect.succeed({
        model,
        getApiKey: () => "test",
        stream: () => {
          calls++;
          const stream = createAssistantMessageEventStream();
          stream.push({ type: "done", reason: "stop", message: assistant() });
          return stream;
        },
      }),
  });
  try {
    const run = Effect.gen(function* () {
      const agent = yield* Agent.make({
        name: "test",
        durable: { sessionId: "guard", requestId: "input", storageDirectory: directory },
        tools: [],
        transformContext: async () => {
          throw new Error("Context exceeds admitted budget");
        },
      });
      return yield* agent.run({ messages: [{ role: "user", content: "Read", timestamp: 0 }] });
    }).pipe(Effect.provide(layer), Effect.timeout("5 seconds"));
    await assert.rejects(Effect.runPromise(run), /submission failed: model_error/);
    await assert.rejects(Effect.runPromise(run), /submission failed: model_error/);
    assert.equal(calls, 0);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("durable token budgets cap the model window and are frozen for request replay", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "aster-pi-budget-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const windows: number[] = [];
  const models = Layer.succeed(Models, {
    resolve: () =>
      Effect.succeed({
        model,
        getApiKey: () => "unused",
        stream: (effective) => {
          windows.push(effective.contextWindow);
          const stream = createAssistantMessageEventStream();
          stream.push({ type: "done", reason: "stop", message: assistant() });
          return stream;
        },
      }),
  });
  const run = (contextTokens: number, requestId: string) =>
    Effect.gen(function* () {
      const agent = yield* Agent.make({
        name: "test",
        durable: {
          sessionId: "budget",
          requestId,
          storageDirectory: directory,
          contextBudget: { contextTokens, reserveTokens: 100 },
        },
      });
      return yield* agent.run({ messages: [{ role: "user", content: "Review", timestamp: 0 }] });
    }).pipe(Effect.provide(models));
  await Effect.runPromise(run(500, "first"));
  assert.deepEqual(windows, [500]);
  await Effect.runPromise(run(500, "first"));
  assert.deepEqual(windows, [500]);
  await assert.rejects(Effect.runPromise(run(600, "first")), /conflicts with its frozen input/);
  await Effect.runPromise(run(2000, "second"));
  assert.deepEqual(windows, [500, 1000]);
  assert.equal(model.contextWindow, 1000);
});

test("invalid durable budgets fail before acquiring storage or invoking a provider", async () => {
  const models = Layer.succeed(Models, {
    resolve: () =>
      Effect.succeed({
        model,
        getApiKey: () => "unused",
        stream: () => {
          throw new Error("Provider must not be called");
        },
      }),
  });
  for (const contextBudget of [
    { contextTokens: 0, reserveTokens: 100 },
    { contextTokens: Infinity, reserveTokens: 100 },
    { contextTokens: 500, reserveTokens: 500 },
    { contextTokens: 500, reserveTokens: -1 },
    { contextTokens: 2000, reserveTokens: 1500 },
  ]) {
    const result = await Effect.runPromise(
      Agent.make({
        name: "test",
        durable: { sessionId: "invalid", requestId: "invalid", contextBudget },
      }).pipe(Effect.provide(models), Effect.result),
    );
    assert.equal(result._tag, "Failure");
    if (result._tag === "Failure") assert.equal(result.failure._tag, "AgentError");
  }
});

test("durable Effect interruption joins Harness.close before returning", async () => {
  const started = Promise.withResolvers<void>();
  const closing = Promise.withResolvers<void>();
  const release = Promise.withResolvers<void>();
  const originalOpen = Harness.open;
  let closed = false;
  let returned = false;
  // Delay the real SDK close at its boundary: interruption must join it even
  // when cancellation of Submission.wait has already rejected the run.
  Harness.open = async (...args) => {
    const harness = await originalOpen(...args);
    const close = harness.close.bind(harness);
    harness.close = async (context) => {
      closing.resolve();
      await release.promise;
      await close(context);
      closed = true;
    };
    return harness;
  };
  const directory = await mkdtemp(join(tmpdir(), "aster-pi-cancel-"));
  const controller = new AbortController();
  try {
    const layer = Layer.succeed(Models, {
      resolve: () =>
        Effect.succeed({
          model,
          getApiKey: () => "test",
          stream: (_model, _context, options) => {
            const stream = createAssistantMessageEventStream();
            options?.signal?.addEventListener(
              "abort",
              () =>
                stream.push({
                  type: "error",
                  reason: "aborted",
                  error: { ...assistant(), stopReason: "aborted" },
                }),
              { once: true },
            );
            started.resolve();
            return stream;
          },
        }),
    });
    const run = Effect.runPromiseExit(
      Effect.gen(function* () {
        const agent = yield* Agent.make({
          name: "test",
          durable: { sessionId: "cancel", requestId: "cancel", storageDirectory: directory },
        });
        return yield* agent.run({ messages: [{ role: "user", content: "Wait", timestamp: 0 }] });
      }).pipe(Effect.provide(layer)),
      { signal: controller.signal },
    ).then((exit) => {
      returned = true;
      assert.equal(closed, true);
      return exit;
    });
    await started.promise;
    controller.abort();
    await closing.promise;
    assert.equal(returned, false);
    const conflict = await Effect.runPromise(
      Effect.scoped(PiStorageLease.acquire(directory, "other-owner").pipe(Effect.flip)),
    );
    assert.equal(conflict._tag, "PiStorageLeaseError");
    release.resolve();
    const exit = await run;
    assert.equal(exit._tag, "Failure");
    if (exit._tag === "Failure") assert.equal(Cause.hasInterrupts(exit.cause), true);
    await Effect.runPromise(Effect.scoped(PiStorageLease.acquire(directory, "next-owner")));
  } finally {
    release.resolve();
    Harness.open = originalOpen;
    await rm(directory, { recursive: true, force: true });
  }
});

test("durable Agent reopens a Goal conversation and deduplicates a request ID", async () => {
  let calls = 0;
  const layer = Layer.succeed(Models, {
    resolve: () =>
      Effect.succeed({
        model,
        stream: () => {
          calls++;
          const stream = createAssistantMessageEventStream();
          queueMicrotask(() => stream.push({ type: "done", reason: "stop", message: assistant() }));
          return stream;
        },
        getApiKey: () => "test",
      }),
  });
  const directory = await mkdtemp(join(tmpdir(), "aster-pi-durable-"));
  try {
    const run = (requestId: string) =>
      Effect.gen(function* () {
        const agent = yield* Agent.make({
          name: "test",
          durable: { sessionId: "goal", requestId, storageDirectory: directory },
        });
        return yield* agent.run({
          messages: [
            { role: "system", content: "Evaluate the Goal", timestamp: 0 },
            { role: "user", content: requestId, timestamp: 0 },
          ],
        });
      }).pipe(Effect.provide(layer));

    const first = await Effect.runPromise(run("evaluation-1"));
    const duplicate = await Effect.runPromise(run("evaluation-1"));
    const next = await Effect.runPromise(run("evaluation-2"));
    const replay = await Effect.runPromise(run("evaluation-1"));
    assert.equal(first.messages.filter((message) => message.role === "assistant").length, 1);
    // A duplicate request replays the committed answer so GoalActor can finish applying it
    // after a crash between Pi settlement and the mailbox acknowledgement.
    assert.equal(duplicate.messages.filter((message) => message.role === "assistant").length, 1);
    assert.equal(next.messages.filter((message) => message.role === "assistant").length, 1);
    assert.deepEqual(replay, first);
    assert.equal(calls, 2);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("durable admission freezes input before submission and commits results before callbacks", async () => {
  const directory = await mkdtemp(join(tmpdir(), "aster-pi-admission-"));
  const originalOpen = Harness.open;
  let failSubmission = true;
  let calls = 0;
  let callbacks = 0;
  Harness.open = async (...args) => {
    const harness = await originalOpen(...args);
    const root = harness.root.bind(harness);
    harness.root = async (...args) => {
      const conversation = await root(...args);
      const submit = conversation.submit.bind(conversation);
      conversation.submit = async (...args) => {
        if (failSubmission) {
          failSubmission = false;
          throw new Error("Transport failed after Aster admission and before Pi submission");
        }
        return submit(...args);
      };
      return conversation;
    };
    return harness;
  };
  const layer = Layer.succeed(Models, {
    resolve: () =>
      Effect.succeed({
        model,
        getApiKey: () => "private-model-key",
        stream: () => {
          calls++;
          const stream = createAssistantMessageEventStream();
          stream.push({ type: "done", reason: "stop", message: assistant() });
          return stream;
        },
      }),
  });
  const run = (
    requestId: string,
    options: {
      text?: string;
      instructions?: string;
      catalogueId?: string;
      callbackFailure?: boolean;
      reconcile?: boolean;
      owner?: "goals" | "tasks";
    } = {},
  ) =>
    Effect.runPromise(
      Effect.gen(function* () {
        const agent = yield* Agent.make({
          name: "test",
          durable: {
            sessionId: "goal",
            requestId,
            storageDirectory: directory,
            catalogueId: options.catalogueId,
            owner: options.owner,
            reconcile: options.reconcile,
          },
          onMessage: () => {
            callbacks++;
            if (options.callbackFailure) throw new Error("Caller lost its result acknowledgement");
          },
        });
        return yield* agent.run({
          messages: [
            { role: "system", content: options.instructions ?? "Review evidence", timestamp: 0 },
            { role: "user", content: options.text ?? "Evidence", timestamp: 0 },
          ],
        });
      }).pipe(Effect.provide(layer)),
    );
  try {
    await assert.rejects(run("one"), /before Pi submission/);
    await assert.rejects(run("two"), /requires recovery/);
    await assert.rejects(run("one", { text: "Changed" }), /frozen input/);
    await assert.rejects(run("one", { instructions: "Changed" }), /frozen input/);
    await assert.rejects(run("one", { catalogueId: "changed-tools" }), /frozen input/);
    await assert.rejects(run("one", { owner: "tasks" }), /another Aster owner/);
    assert.equal(calls, 0);
    await assert.rejects(run("one", { callbackFailure: true, reconcile: true }), /lost its result/);
    assert.equal(calls, 1);
    assert.equal(callbacks, 1);
    const replay = await run("one");
    assert.equal(calls, 1);
    assert.equal(replay.messages.length, 1);
    assert.deepEqual(
      await run("one", {
        reconcile: true,
        text: "New prompt version",
        catalogueId: "new-tools",
      }),
      replay,
    );
    assert.equal(calls, 1);
    await run("two");
    assert.equal(calls, 2);
    assert.deepEqual(await run("one"), replay);
    assert.equal(calls, 2);
  } finally {
    Harness.open = originalOpen;
    await rm(directory, { recursive: true, force: true });
  }
});

test("known failed durable results replay without model work and allow an explicit new attempt", async () => {
  const directory = await mkdtemp(join(tmpdir(), "aster-pi-failed-exchange-"));
  let calls = 0;
  const layer = Layer.succeed(Models, {
    resolve: () =>
      Effect.succeed({
        model,
        getApiKey: () => "test",
        stream: () => {
          calls++;
          const stream = createAssistantMessageEventStream();
          const message =
            calls <= 2
              ? assistant()
              : {
                  ...assistant(),
                  stopReason: "toolUse" as const,
                  content: [
                    { type: "toolCall" as const, id: "finish-call", name: "finish", arguments: {} },
                  ],
                };
          stream.push({
            type: "done",
            reason: message.stopReason === "toolUse" ? "toolUse" : "stop",
            message,
          });
          return stream;
        },
      }),
  });
  const run = (requestId: string) =>
    Effect.runPromise(
      Effect.gen(function* () {
        const agent = yield* Agent.make({
          name: "test",
          durable: { sessionId: "goal", requestId, storageDirectory: directory },
          resultTool: "finish",
          tools: [
            {
              name: "finish",
              label: "Finish",
              description: "Submit the result",
              replay: "safe",
              parameters: Type.Object({}),
              execute: async () => ({
                content: [{ type: "text", text: "Done" }],
                details: {},
                terminate: true,
              }),
            },
          ],
        });
        return yield* agent.run({ messages: [{ role: "user", content: "Read", timestamp: 0 }] });
      }).pipe(Effect.provide(layer)),
    );
  try {
    await assert.rejects(run("one"), (error: unknown) => {
      assert.ok(error instanceof AgentError);
      assert.equal(error.outcome, "failed");
      assert.match(error.message, /after one correction/);
      return true;
    });
    assert.equal(calls, 2);
    await assert.rejects(run("one"), (error: unknown) => {
      assert.ok(error instanceof AgentError);
      assert.equal(error.outcome, "failed");
      return true;
    });
    assert.equal(calls, 2);
    // A caller's ID may look like the old internal correction suffix.
    const retried = await run("one:correction");
    assert.equal(calls, 3);
    assert.equal(retried.messages.at(-1)?.role, "toolResult");
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("ownerless unsafe outcomes stop sibling tools and preserve uncertainty across restart", async () => {
  const directory = await mkdtemp(join(tmpdir(), "aster-pi-unsafe-round-"));
  let generations = 0;
  let writes = 0;
  let siblings = 0;
  const layer = Layer.succeed(Models, {
    resolve: () =>
      Effect.succeed({
        model,
        getApiKey: () => "test",
        stream: () => {
          generations++;
          const stream = createAssistantMessageEventStream();
          stream.push({
            type: "done",
            reason: "toolUse",
            message: {
              ...assistant(),
              stopReason: "toolUse",
              content: [
                { type: "toolCall", id: "write-one", name: "write", arguments: {} },
                { type: "toolCall", id: "sibling-two", name: "sibling", arguments: {} },
              ],
            },
          });
          return stream;
        },
      }),
  });
  const run = (requestId = "one") =>
    Effect.runPromise(
      Effect.gen(function* () {
        const agent = yield* Agent.make({
          name: "test",
          durable: { requestId, sessionId: "goal", storageDirectory: directory },
          tools: [
            {
              name: "write",
              label: "Write",
              description: "Write",
              parameters: Type.Object({}),
              replay: "never",
              execute: async () => {
                writes++;
                throw new Error("Write accepted but acknowledgement lost");
              },
            },
            {
              name: "sibling",
              label: "Sibling",
              description: "Sibling",
              parameters: Type.Object({}),
              replay: "safe",
              execute: async () => {
                siblings++;
                return { content: [{ type: "text", text: "Read" }], details: {} };
              },
            },
          ],
        });
        return yield* agent.run({
          messages: [{ role: "user", content: "Do the work", timestamp: 0 }],
        });
      }).pipe(Effect.provide(layer)),
    );
  try {
    await assert.rejects(run(), (error: unknown) => {
      assert.ok(error instanceof AgentError);
      assert.equal(error.outcome, "unknown");
      assert.match(error.message, /outcome is unknown/);
      return true;
    });
    await assert.rejects(run(), /outcome is unknown/);
    await assert.rejects(run("new-attempt"), /requires recovery/);
    assert.equal(generations, 1);
    assert.equal(writes, 1);
    assert.equal(siblings, 0);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

for (const replay of ["safe", "unsafe"] as const) {
  test(`ownerless ${replay} tool interruption preserves replay policy after reopening`, async () => {
    const directory = await mkdtemp(join(tmpdir(), `aster-ownerless-${replay}-`));
    let toolCalls = 0;
    let modelCalls = 0;
    const entered = Deferred.makeUnsafe<void>();
    const layer = Layer.succeed(Models, {
      resolve: () =>
        Effect.succeed({
          model,
          getApiKey: () => "test",
          stream: () => {
            modelCalls++;
            const stream = createAssistantMessageEventStream();
            const message =
              modelCalls === 1
                ? {
                    ...assistant(),
                    stopReason: "toolUse" as const,
                    content: [
                      { type: "toolCall" as const, id: "first", name: "work", arguments: {} },
                    ],
                  }
                : assistant();
            stream.push({
              type: "done",
              reason: message.stopReason === "toolUse" ? "toolUse" : "stop",
              message,
            });
            return stream;
          },
        }),
    });
    const run = Effect.gen(function* () {
      const agent = yield* Agent.make({
        name: "test",
        durable: { requestId: "one", sessionId: "goal", storageDirectory: directory },
        tools: [
          {
            name: "work",
            label: "Work",
            description: "Work",
            parameters: Type.Object({}),
            replay: replay === "safe" ? "safe" : "never",
            execute: async (_id, _args, signal) => {
              toolCalls++;
              if (toolCalls === 1) {
                await Effect.runPromise(Deferred.succeed(entered, undefined));
                await new Promise<void>((_resolve, reject) => {
                  if (signal?.aborted) reject(new Error("Interrupted"));
                  else
                    signal?.addEventListener("abort", () => reject(new Error("Interrupted")), {
                      once: true,
                    });
                });
              }
              return { content: [{ type: "text", text: "Done" }], details: {} };
            },
          },
        ],
      });
      return yield* agent.run({ messages: [{ role: "user", content: "Work", timestamp: 0 }] });
    }).pipe(Effect.provide(layer));
    try {
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const fiber = yield* run.pipe(Effect.forkScoped);
            yield* Deferred.await(entered);
            yield* Fiber.interrupt(fiber);
          }),
        ).pipe(Effect.timeout("5 seconds")),
      );
      const recovered = await Effect.runPromise(
        run.pipe(Effect.result, Effect.timeout("5 seconds")),
      );
      assert.equal(recovered._tag, replay === "safe" ? "Success" : "Failure");
      if (recovered._tag === "Failure")
        assert.match(recovered.failure.message, /outcome is unknown/);
      assert.equal(toolCalls, replay === "safe" ? 2 : 1);
      assert.equal(modelCalls, replay === "safe" ? 2 : 1);
    } finally {
      await rm(directory, { recursive: true, force: true });
    }
  });
}

test("ownerless known rejections and pre-execution validation allow model correction", async () => {
  const directory = await mkdtemp(join(tmpdir(), "aster-pi-known-rejection-"));
  let generations = 0;
  let writes = 0;
  const layer = Layer.succeed(Models, {
    resolve: () =>
      Effect.succeed({
        model,
        getApiKey: () => "test",
        stream: () => {
          generations++;
          const stream = createAssistantMessageEventStream();
          const message =
            generations <= 3
              ? {
                  ...assistant(),
                  stopReason: "toolUse" as const,
                  content: [
                    {
                      type: "toolCall" as const,
                      id: `call-${generations}`,
                      name: "write",
                      arguments: generations === 1 ? {} : { revision: generations - 1 },
                    },
                  ],
                }
              : assistant();
          stream.push({
            type: "done",
            reason: message.stopReason === "toolUse" ? "toolUse" : "stop",
            message,
          });
          return stream;
        },
      }),
  });
  const run = () =>
    Effect.runPromise(
      Effect.gen(function* () {
        const agent = yield* Agent.make({
          name: "test",
          durable: { requestId: "one", sessionId: "goal", storageDirectory: directory },
          tools: [
            {
              name: "write",
              label: "Write",
              description: "Write",
              parameters: Type.Object({ revision: Type.Number() }),
              replay: "never",
              execute: async (_id, args) => {
                writes++;
                return Schema.decodeUnknownSync(Schema.Struct({ revision: Schema.Number }))(args)
                  .revision === 1
                  ? rejectedToolResult("Stale revision: read revision 2 before retrying")
                  : { content: [{ type: "text", text: "Committed" }], details: {} };
              },
            },
          ],
        });
        return yield* agent.run({
          messages: [{ role: "user", content: "Do the work", timestamp: 0 }],
        });
      }).pipe(Effect.provide(layer)),
    );
  try {
    const result = await run();
    assert.equal(generations, 4);
    assert.equal(writes, 2);
    assert.equal(result.messages.at(-1)?.role, "assistant");
    assert.deepEqual(await run(), result);
    assert.equal(generations, 4);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("durable tools keep structured results in the Pi transcript", async () => {
  const layer = Layer.succeed(Models, {
    resolve: () =>
      Effect.succeed({
        model,
        stream: (_model: unknown, context: { messages: readonly unknown[] }) => {
          const stream = createAssistantMessageEventStream();
          const alreadyCalled = context.messages.some(
            (message) =>
              typeof message === "object" &&
              message !== null &&
              "role" in message &&
              message.role === "assistant" &&
              "content" in message &&
              Array.isArray(message.content) &&
              message.content.some(
                (content) =>
                  typeof content === "object" &&
                  content !== null &&
                  "type" in content &&
                  content.type === "toolCall",
              ),
          );
          queueMicrotask(() =>
            stream.push({
              type: "done",
              reason: "toolUse",
              message: alreadyCalled
                ? assistant()
                : {
                    ...assistant(),
                    stopReason: "toolUse",
                    content: [
                      {
                        type: "toolCall",
                        id: "save-call",
                        name: "save",
                        arguments: { value: 7 },
                      },
                    ],
                  },
            }),
          );
          return stream;
        },
        getApiKey: () => "test",
      }),
  });
  const directory = await mkdtemp(join(tmpdir(), "aster-pi-durable-tool-"));
  try {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const agent = yield* Agent.make({
          name: "test",
          resultTool: "save",
          tools: [
            {
              name: "save",
              label: "Save",
              description: "Save a value",
              parameters: Type.Object({ value: Type.Number() }),
              execute: async (_id, args) => ({
                content: [{ type: "text", text: "saved" }],
                details: args,
                terminate: true,
              }),
            },
          ],
          durable: { sessionId: "goal", requestId: "tool-1", storageDirectory: directory },
        });
        return yield* agent.run({
          messages: [{ role: "system", content: "Use tools", timestamp: 0 }],
        });
      }).pipe(Effect.provide(layer)),
    );
    const saved = result.messages.find(
      (message) => message.role === "toolResult" && message.toolName === "save",
    );
    assert.equal(saved?.role, "toolResult");
    assert.deepEqual(saved?.details, { value: 7 });
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("replaying a terminal tool round excludes later results even when call IDs repeat", async () => {
  let calls = 0;
  const layer = Layer.succeed(Models, {
    resolve: () =>
      Effect.succeed({
        model,
        getApiKey: () => "test",
        stream: () => {
          const stream = createAssistantMessageEventStream();
          stream.push({
            type: "done",
            reason: "toolUse",
            message: {
              ...assistant(),
              stopReason: "toolUse",
              content: [
                {
                  type: "toolCall",
                  id: "reused-call",
                  name: "save",
                  arguments: { value: ++calls },
                },
              ],
            },
          });
          return stream;
        },
      }),
  });
  const directory = await mkdtemp(join(tmpdir(), "aster-pi-terminal-replay-"));
  try {
    const run = (requestId: string) =>
      Effect.gen(function* () {
        const agent = yield* Agent.make({
          name: "test",
          resultTool: "save",
          durable: { sessionId: "terminal", requestId, storageDirectory: directory },
          tools: [
            {
              name: "save",
              label: "Save",
              description: "Save",
              parameters: Type.Object({ value: Type.Number() }),
              execute: async (_id, args) => ({
                content: [{ type: "text", text: "saved" }],
                details: args,
                terminate: true,
              }),
            },
          ],
        });
        return yield* agent.run({ messages: [{ role: "user", content: requestId, timestamp: 0 }] });
      }).pipe(Effect.provide(layer));
    const first = await Effect.runPromise(run("first"));
    const next = await Effect.runPromise(run("second"));
    assert.equal(first.messages.length, 2);
    assert.notDeepEqual(first, next);
    assert.deepEqual(await Effect.runPromise(run("first")), first);
    assert.equal(calls, 2);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("Goal and Task durable invocations share storage exclusivity before model admission", async () => {
  const directory = await mkdtemp(join(tmpdir(), "aster-ownerless-exclusive-"));
  try {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const entered = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          let calls = 0;
          const layer = Layer.succeed(Models, {
            resolve: () =>
              Effect.succeed({
                model,
                getApiKey: () => "test",
                stream: () => {
                  calls++;
                  const stream = createAssistantMessageEventStream();
                  Effect.runSync(Deferred.succeed(entered, undefined));
                  void Effect.runPromise(Deferred.await(release)).then(() =>
                    stream.push({ type: "done", reason: "stop", message: assistant() }),
                  );
                  return stream;
                },
              }),
          });
          const goal = yield* Agent.make({
            name: "test",
            durable: {
              owner: "goals",
              sessionId: "one",
              requestId: "request",
              storageDirectory: directory,
            },
          }).pipe(Effect.provide(layer));
          const task = yield* Agent.make({
            name: "test",
            durable: {
              owner: "tasks",
              sessionId: "one",
              requestId: "other",
              storageDirectory: directory,
            },
          }).pipe(Effect.provide(layer));
          const input = { messages: [{ role: "user" as const, content: "Read", timestamp: 0 }] };
          const running = yield* goal.run(input).pipe(Effect.forkScoped);
          yield* Deferred.await(entered);
          const failure = yield* task.run(input).pipe(Effect.flip);
          assert.match(failure.message, /owned by another process/);
          assert.equal(calls, 1);
          yield* Deferred.succeed(release, undefined);
          const original = yield* Fiber.join(running);
          assert.deepEqual(yield* goal.run(input), original);
          assert.equal(calls, 1);
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test("Unknown ownerless Harness close retains storage fencing after the invocation fails", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "aster-ownerless-close-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const open = Harness.open;
  t.mock.method(Harness, "open", async (...args: Parameters<typeof Harness.open>) => {
    const harness = await open(...args);
    const close = harness.close.bind(harness);
    harness.close = async (context) => {
      await close(context);
      throw new Error("Close acknowledgement lost");
    };
    return harness;
  });
  const layer = Layer.succeed(Models, {
    resolve: () =>
      Effect.succeed({
        model,
        getApiKey: () => "test",
        stream: () => {
          const stream = createAssistantMessageEventStream();
          stream.push({ type: "done", reason: "stop", message: assistant() });
          return stream;
        },
      }),
  });
  const failure = await Effect.runPromise(
    Effect.gen(function* () {
      const agent = yield* Agent.make({
        name: "test",
        durable: { sessionId: "one", requestId: "one", storageDirectory: directory },
      });
      return yield* agent
        .run({ messages: [{ role: "user", content: "Read", timestamp: 0 }] })
        .pipe(Effect.flip);
    }).pipe(Effect.provide(layer)),
  );
  assert.match(failure.message, /shutdown is uncertain/);
  assert.equal(failure.outcome, "unknown");
  const conflict = await Effect.runPromise(
    Effect.scoped(PiStorageLease.acquire(directory, "replacement").pipe(Effect.flip)),
  );
  assert.equal(conflict._tag, "PiStorageLeaseError");
});

test("reconciliation observes an accepted uncertain request without issuing another provider call", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "aster-pi-observe-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  const originalOpen = Harness.open;
  let loseReceipt = true;
  let calls = 0;
  t.mock.method(Harness, "open", async (...args: Parameters<typeof Harness.open>) => {
    const harness = await originalOpen(...args);
    const root = harness.root.bind(harness);
    harness.root = async (...rootArgs) => {
      const conversation = await root(...rootArgs);
      const submit = conversation.submit.bind(conversation);
      conversation.submit = async (...submitArgs) => {
        const accepted = await submit(...submitArgs);
        if (loseReceipt) {
          loseReceipt = false;
          throw new Error("Accepted input acknowledgement lost");
        }
        return accepted;
      };
      return conversation;
    };
    return harness;
  });
  const layer = Layer.succeed(Models, {
    resolve: () =>
      Effect.succeed({
        model,
        getApiKey: () => "test",
        stream: () => {
          calls++;
          return createAssistantMessageEventStream();
        },
      }),
  });
  const run = (reconcile: boolean) =>
    Effect.gen(function* () {
      const agent = yield* Agent.make({
        name: "test",
        durable: {
          sessionId: "uncertain",
          requestId: "one",
          storageDirectory: directory,
          reconcile,
        },
      });
      return yield* agent.run({ messages: [{ role: "user", content: "Research", timestamp: 0 }] });
    }).pipe(Effect.provide(layer), Effect.timeout("5 seconds"));
  await assert.rejects(Effect.runPromise(run(false)), /acknowledgement lost/);
  const admittedCalls = calls;
  const result = await Effect.runPromise(Effect.result(run(true)));
  assert.equal(calls, admittedCalls, "Recovery must not submit another provider request");
  assert.equal(result._tag, "Failure");
});
