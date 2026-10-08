import assert from "node:assert/strict";
import { test } from "node:test";
import { createHash } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import {
  createAssistantMessageEventStream,
  Type,
  type AssistantMessage,
} from "@earendil-works/pi-ai";
import { Harness, createRegistry, defineExtension, defineTool } from "@earendil-works/pi-durable";
import { openNodeJsonlStorage } from "@earendil-works/pi-durable/storage/jsonl/node";
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Option } from "effect";
import { Models, type ResolvedModel } from "../src/index.js";
import {
  AgentConversations,
  DurableHarness,
  type HarnessConversation,
} from "../src/harness/index.js";
import { conversationDriver } from "../src/harness/conversations.js";
import { durableModels } from "../src/harness/runtime.js";

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
const answer = (text = "Done"): AssistantMessage => ({
  role: "assistant",
  content: [{ type: "text", text }],
  api: "openai-completions",
  provider: "test",
  model: "test",
  stopReason: "stop",
  timestamp: 0,
  usage: {
    input: 0,
    output: 0,
    cacheRead: 0,
    cacheWrite: 0,
    totalTokens: 0,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
  },
});
const streamed = (message: AssistantMessage) => {
  const stream = createAssistantMessageEventStream();
  if (message.stopReason === "error" || message.stopReason === "aborted")
    stream.push({ type: "error", reason: message.stopReason, error: message });
  else {
    assert.notEqual(message.stopReason, "pending");
    stream.push({
      type: "done",
      reason: message.stopReason === "pending" ? "stop" : message.stopReason,
      message,
    });
  }
  return stream;
};
const call = (): AssistantMessage => ({
  ...answer(),
  stopReason: "toolUse",
  content: [{ type: "toolCall", id: "call-1", name: "read", arguments: {} }],
});
const options = {
  name: "test",
  owner: "/goals/sample",
  extensionName: "tools",
  instructions: "Answer",
};
const models = (stream: ResolvedModel["stream"]) =>
  Layer.succeed(Models, {
    resolve: () => Effect.succeed({ model, stream, getApiKey: () => "unused" }),
  });
const run = <A, E>(
  effect: Effect.Effect<A, E, DurableHarness | AgentConversations | import("effect").Scope.Scope>,
  stream: ResolvedModel["stream"],
  root?: string,
) => {
  const storage = root
    ? Layer.effect(AgentConversations, AgentConversations.make({ root }))
    : AgentConversations.memory;
  return Effect.runPromise(
    effect.pipe(
      Effect.provide(
        DurableHarness.layer.pipe(Layer.provide(models(stream)), Layer.provideMerge(storage)),
      ),
      Effect.scoped,
      Effect.timeout("8 seconds"),
    ),
  );
};
const request = (id: string, content = id) =>
  DurableHarness.use((harness) =>
    harness.withConversation(options, (conversation) =>
      conversation
        .submit({ requestId: id, content })
        .pipe(Effect.flatMap((submission) => submission.wait)),
    ),
  );

test("native answers retain exact request identity across later turns and reopen", async (t) => {
  const directory = await mkdtemp(join(tmpdir(), "aster-native-answer-"));
  t.after(() => rm(directory, { recursive: true, force: true }));
  let calls = 0;
  const seen: string[] = [];
  const stream: ResolvedModel["stream"] = (_model, context) => {
    seen.push(JSON.stringify(context.messages));
    return streamed(answer(`Answer ${++calls}`));
  };
  const first = await run(request("one", "Caller-owned input"), stream, directory);
  await run(request("two", "Later evidence"), stream, directory);
  assert.deepEqual(await run(request("one", "Caller-owned input"), stream, directory), first);
  assert.equal(calls, 2);
  assert.match(seen[1]!, /Caller-owned input/);
  assert.match(seen[1]!, /Later evidence/);
});

test("settled lookup and wait do not schedule or abort native work", async () => {
  let calls = 0;
  await run(
    Effect.gen(function* () {
      yield* request("one");
      const messages = yield* AgentConversations;
      const driver = yield* messages[conversationDriver](options.owner);
      const nativeSubmission = driver.harness.submission.bind(driver.harness);
      driver.harness.submission = async (...args) => {
        const submission = await nativeSubmission(...args);
        if (submission)
          submission.wait = async () => assert.fail("Terminal reads must not resume scheduling");
        return submission;
      };
      const nativeRoot = driver.harness.root.bind(driver.harness);
      driver.harness.root = async (...args) => {
        const root = await nativeRoot(...args);
        root.abort = async () => assert.fail("Terminal reads must not abort");
        root.waitForIdle = async () => assert.fail("Terminal reads must not resume scheduling");
        root.submit = async () => assert.fail("Terminal reads must not submit");
        return root;
      };
      yield* request("one");
    }),
    () => {
      calls++;
      return streamed(answer());
    },
  );
  assert.equal(calls, 1);
});

test("native terminal failure is replayed; explicit new input can execute", async () => {
  let calls = 0;
  await run(
    Effect.gen(function* () {
      for (let i = 0; i < 2; i++) {
        const result = yield* request("one").pipe(Effect.result);
        assert.equal(result._tag, "Failure");
        if (result._tag === "Failure") assert.equal(result.failure.outcome, "failed");
      }
      assert.equal(calls, 1);
      assert.deepEqual(yield* request("retry"), answer());
    }),
    () =>
      ++calls === 1
        ? streamed({ ...answer(), stopReason: "error", errorMessage: "Rejected model request" })
        : streamed(answer()),
  );
});

test("steering is a native submission with its own wait and retained answer", async () => {
  let calls = 0;
  await run(
    DurableHarness.use((harness) =>
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const tools = [
          {
            name: "read",
            label: "Read",
            description: "Read",
            parameters: Type.Object({}),
            replay: "safe" as const,
            execute: () =>
              Deferred.succeed(entered, undefined).pipe(
                Effect.andThen(Deferred.await(release)),
                Effect.as({
                  content: [{ type: "text" as const, text: "Evidence" }],
                  details: undefined,
                }),
              ),
          },
        ];
        yield* harness.withConversation({ ...options, tools }, (conversation) =>
          Effect.gen(function* () {
            const first = yield* conversation.submit({ requestId: "initial", content: "Analyze" });
            const waiting = yield* first.wait.pipe(Effect.forkScoped);
            yield* Deferred.await(entered);
            const steer = yield* conversation.submit({
              requestId: "follow",
              content: "Add regional analysis",
              whenBusy: "steer",
            });
            assert.equal((yield* steer.status).status, "queued");
            yield* Deferred.succeed(release, undefined);
            assert.deepEqual(yield* Fiber.join(waiting), answer());
            assert.deepEqual(yield* steer.wait, answer());
            assert.equal((yield* steer.status).status, "done");
          }),
        );
        yield* harness.withConversation(options, (conversation) =>
          Effect.gen(function* () {
            const steer = yield* conversation.submission("follow");
            assert.ok(Option.isSome(steer));
            assert.deepEqual(yield* steer.value.wait, answer());
          }),
        );
      }),
    ),
    (_model, context) => {
      if (++calls === 1) return streamed(call());
      assert.match(JSON.stringify(context.messages), /Add regional analysis/);
      return streamed(answer());
    },
  );
  assert.equal(calls, 2);
});

test("cancelling a wait keeps native work alive inside its owning scope", async () => {
  let calls = 0;
  await run(
    DurableHarness.use((harness) =>
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        yield* harness.withConversation(
          {
            ...options,
            tools: [
              {
                name: "read",
                label: "Read",
                description: "Read",
                parameters: Type.Object({}),
                replay: "safe",
                execute: () =>
                  Deferred.succeed(entered, undefined).pipe(
                    Effect.andThen(Deferred.await(release)),
                    Effect.as({ content: [], details: undefined }),
                  ),
              },
            ],
          },
          (conversation) =>
            Effect.gen(function* () {
              const submitted = yield* conversation.submit({
                requestId: "one",
                content: "Analyze",
              });
              const waiting = yield* submitted.wait.pipe(Effect.forkScoped);
              yield* Deferred.await(entered);
              yield* Fiber.interrupt(waiting);
              assert.equal((yield* submitted.status).status, "placed");
              yield* Deferred.succeed(release, undefined);
              assert.deepEqual(yield* submitted.wait, answer());
            }),
        );
      }),
    ),
    () => streamed(++calls === 1 ? call() : answer()),
  );
});

test("leaving an interrupted scope drains callbacks and retires its handles", async () => {
  let retired: HarnessConversation | undefined;
  await run(
    DurableHarness.use((harness) =>
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const released = yield* Deferred.make<void>();
        const fiber = yield* harness
          .withConversation(
            {
              ...options,
              tools: [
                {
                  name: "read",
                  label: "Read",
                  description: "Read",
                  parameters: Type.Object({}),
                  replay: "safe",
                  execute: () =>
                    Deferred.succeed(entered, undefined).pipe(
                      Effect.andThen(Effect.never),
                      Effect.ensuring(Deferred.succeed(released, undefined)),
                    ),
                },
              ],
            },
            (conversation) => {
              retired = conversation;
              return conversation
                .submit({ requestId: "one", content: "Analyze" })
                .pipe(Effect.flatMap((submission) => submission.wait));
            },
          )
          .pipe(Effect.forkScoped);
        yield* Deferred.await(entered);
        yield* Fiber.interrupt(fiber);
        assert.equal(yield* Deferred.isDone(released), true);
        const late = yield* retired!
          .submit({ requestId: "late", content: "No" })
          .pipe(Effect.result);
        assert.equal(late._tag, "Failure");
        assert.equal((yield* retired!.abort.pipe(Effect.result))._tag, "Failure");
        yield* harness.withConversation(options, () => Effect.void);
      }),
    ),
    () => streamed(call()),
  );
});

test("explicit abort settles native work without converting interruption into success", async () => {
  await run(
    DurableHarness.use((harness) =>
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        yield* harness.withConversation(
          {
            ...options,
            tools: [
              {
                name: "read",
                label: "Read",
                description: "Read",
                parameters: Type.Object({}),
                replay: "safe",
                execute: () =>
                  Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
              },
            ],
          },
          (conversation) =>
            Effect.gen(function* () {
              const submission = yield* conversation.submit({
                requestId: "one",
                content: "Analyze",
              });
              yield* Deferred.await(entered);
              yield* conversation.abort;
              const result = yield* submission.wait.pipe(Effect.result);
              assert.equal(result._tag, "Failure");
              assert.equal((yield* submission.status).status, "unanswered");
            }),
        );
      }),
    ),
    () => streamed(call()),
  );
});

for (const replay of ["safe", "unsafe"] as const) {
  test(`reopening a placed input uses Pi's ${replay} tool recovery policy`, async (t) => {
    const directory = await mkdtemp(join(tmpdir(), "aster-native-recovery-"));
    t.after(() => rm(directory, { recursive: true, force: true }));
    const storagePath = join(directory, createHash("sha256").update(options.owner).digest("hex"));
    const entered = Deferred.makeUnsafe<void>();
    const registry = createRegistry();
    const extension = defineExtension({
      name: "tools",
      tools: [
        defineTool({
          name: "read",
          description: "Read",
          parameters: Type.Object({}),
          replay,
          execute: (_args, _api, context) =>
            new Promise((_, reject) => {
              context.abortSignal!.addEventListener(
                "abort",
                () => reject(context.abortSignal!.reason),
                { once: true },
              );
              Effect.runSync(Deferred.succeed(entered, undefined));
            }),
        }),
      ],
    });
    registry.install(extension);
    const native = await Harness.open(
      await openNodeJsonlStorage(storagePath, BACKGROUND_CONTEXT),
      {
        registry,
        models: durableModels({ model, stream: () => streamed(call()), getApiKey: () => "unused" }),
      },
      BACKGROUND_CONTEXT,
    );
    const root = await native.root(BACKGROUND_CONTEXT, {
      agent: { model: { provider: "test", modelId: "test" }, extensions: [extension] },
    });
    await root.submit(
      {
        type: "input",
        content: "Analyze",
        requestId: JSON.stringify(["aster.agent.input", "one", "initial"]),
      },
      BACKGROUND_CONTEXT,
    );
    await Effect.runPromise(Deferred.await(entered).pipe(Effect.timeout("5 seconds")));
    await native.close(BACKGROUND_CONTEXT);
    let reruns = 0;
    let generations = 0;
    await run(
      DurableHarness.use((harness) =>
        harness.withConversation(
          {
            ...options,
            tools: [
              {
                name: "read",
                label: "Read",
                description: "Read",
                parameters: Type.Object({}),
                replay: replay === "safe" ? "safe" : "never",
                execute: () =>
                  Effect.sync(() => {
                    reruns++;
                    return { content: [], details: undefined };
                  }),
              },
            ],
          },
          (conversation) =>
            Effect.gen(function* () {
              const existing = yield* conversation.submission("one");
              assert.ok(Option.isSome(existing));
              assert.equal((yield* existing.value.status).status, "placed");
              assert.deepEqual(yield* existing.value.wait, answer());
            }),
        ),
      ),
      (_model, context) => {
        generations++;
        if (replay === "unsafe") assert.match(JSON.stringify(context.messages), /interrupted/);
        return streamed(answer());
      },
      directory,
    );
    assert.equal(reruns, replay === "safe" ? 1 : 0);
    assert.equal(generations, 1);
  });
}

test("response observers precede tools and terminal replay emits no new response", async () => {
  const events: string[] = [];
  let calls = 0;
  await run(
    DurableHarness.use((harness) =>
      Effect.gen(function* () {
        const configured = {
          ...options,
          onResponse: () =>
            Effect.sync(() => {
              events.push("response");
            }),
          tools: [
            {
              name: "read",
              label: "Read",
              description: "Read",
              parameters: Type.Object({}),
              replay: "safe" as const,
              execute: () =>
                Effect.sync(() => {
                  events.push("tool");
                  return { content: [], details: undefined };
                }),
            },
          ],
        };
        for (let i = 0; i < 2; i++)
          yield* harness.withConversation(configured, (conversation) =>
            conversation
              .submit({ requestId: "one", content: "Analyze" })
              .pipe(Effect.flatMap((submission) => submission.wait)),
          );
      }),
    ),
    () => streamed(++calls === 1 ? call() : answer()),
  );
  assert.deepEqual(events, ["response", "tool", "response"]);
});

test("terminal tool answers remain native assistant entries; tools are available in history", async () => {
  await run(
    DurableHarness.use((harness) =>
      Effect.gen(function* () {
        const result = yield* harness.withConversation(
          {
            ...options,
            tools: [
              {
                name: "read",
                label: "Read",
                description: "Read",
                parameters: Type.Object({}),
                replay: "safe",
                execute: () =>
                  Effect.succeed({
                    content: [{ type: "text", text: "Finished" }],
                    details: { saved: true },
                    terminate: true,
                  }),
              },
            ],
          },
          (conversation) =>
            conversation
              .submit({ requestId: "one", content: "Analyze" })
              .pipe(Effect.flatMap((submission) => submission.wait)),
        );
        assert.equal(result?.content[0]?.type, "toolCall");
        const messages = yield* AgentConversations;
        assert.match(JSON.stringify(yield* messages.tools(options.owner)), /Finished/);
      }),
    ),
    () => streamed(call()),
  );
});

test("callback defects escape native tool recovery", async () => {
  const defect = new Error("Invariant failed");
  let calls = 0;
  const exit = await run(
    DurableHarness.use((harness) =>
      harness.withConversation(
        {
          ...options,
          tools: [
            {
              name: "read",
              label: "Read",
              description: "Read",
              parameters: Type.Object({}),
              replay: "safe",
              execute: () => Effect.die(defect),
            },
          ],
        },
        (conversation) =>
          conversation
            .submit({ requestId: "one", content: "Analyze" })
            .pipe(Effect.flatMap((submission) => submission.wait)),
      ),
    ).pipe(Effect.exit),
    () => streamed(++calls === 1 ? call() : answer()),
  );
  assert.ok(Exit.isFailure(exit));
  assert.equal(Cause.squash(exit.cause), defect);
});

test("context budgets cap provider windows and reject unusable reserves", async () => {
  let calls = 0;
  await run(
    DurableHarness.use((harness) =>
      Effect.gen(function* () {
        const invalid = yield* harness
          .withConversation(
            { ...options, contextBudget: { contextTokens: 2000, reserveTokens: 1000 } },
            () => Effect.void,
          )
          .pipe(Effect.result);
        assert.equal(invalid._tag, "Failure");
        assert.equal(calls, 0);
        yield* harness.withConversation(
          { ...options, contextBudget: { contextTokens: 2000, reserveTokens: 100 } },
          (conversation) =>
            conversation
              .submit({ requestId: "one", content: "Analyze" })
              .pipe(Effect.flatMap((submission) => submission.wait)),
        );
      }),
    ),
    (selected) => {
      assert.equal(selected.contextWindow, 1000);
      calls++;
      return streamed(answer());
    },
  );
  assert.equal(calls, 1);
});

test("a queued conversation scope can be cancelled without waiting for the active owner", async () => {
  await run(
    DurableHarness.use((harness) =>
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const owner = yield* harness
          .withConversation(options, () =>
            Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
          )
          .pipe(Effect.forkScoped);
        yield* Deferred.await(entered);
        const queued = yield* harness
          .withConversation(options, () => Effect.die("Cancelled scope must not acquire the owner"))
          .pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        yield* Fiber.interrupt(queued);
        assert.equal(owner.pollUnsafe(), undefined);
        yield* Fiber.interrupt(owner);
      }),
    ),
    () => assert.fail("No model work requested"),
  );
});
