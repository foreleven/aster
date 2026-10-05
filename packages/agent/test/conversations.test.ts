import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createAssistantMessageEventStream, type AssistantMessage } from "@earendil-works/pi-ai";
import assert from "node:assert/strict";
import { test } from "node:test";
import { Deferred, Effect, Fiber, Layer } from "effect";
import { AgentConversations, AgentRunner, Models, Type } from "../src/index.js";

test("Pi messages deduplicate exact inputs, reject changed identities and isolate owners", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const messages = yield* AgentConversations;
        const first = yield* messages.append("/goals/a", "one", "goal.input", { text: "Hello" });
        const retry = yield* messages.append("/goals/a", "one", "goal.input", { text: "Hello" });
        assert.deepEqual(retry, first);
        const conflict = yield* messages
          .append("/goals/a", "one", "goal.input", { text: "Different" })
          .pipe(Effect.result);
        assert.equal(conflict._tag, "Failure");
        yield* messages.append("/tasks/b", "one", "task.input", { text: "Other owner" });
        assert.deepEqual(
          (yield* messages.read("/goals/a")).map((entry) => entry.data),
          [{ text: "Hello" }],
        );
        assert.deepEqual((yield* messages.get("/tasks/b", 0).pipe(Effect.result))._tag, "Failure");
      }).pipe(Effect.provide(AgentConversations.memory)),
    ),
  );
});

test("Pi message commits survive reopen, concurrent admission and changed retry payloads", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "aster-conversations-"));
  t.after(() => rm(root, { recursive: true, force: true }));
  const open = <A, E>(use: (messages: AgentConversations["Service"]) => Effect.Effect<A, E>) =>
    Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          return yield* use(yield* AgentConversations.make({ root }));
        }),
      ),
    );
  const entries = await open((messages) =>
    Effect.all(
      Array.from({ length: 80 }, (_, index) =>
        messages.append("/goals/a", `input-${index}`, "goal.input", { text: `Evidence ${index}` }),
      ),
      { concurrency: "unbounded" },
    ),
  );
  await open((messages) =>
    Effect.gen(function* () {
      assert.equal((yield* messages.read("/goals/a")).length, 80);
      assert.deepEqual(
        yield* messages.append("/goals/a", "input-0", "goal.input", { text: "Evidence 0" }),
        entries[0],
      );
      assert.equal(
        (yield* messages
          .append("/goals/a", "input-0", "goal.input", { text: "Changed" })
          .pipe(Effect.result))._tag,
        "Failure",
      );
      assert.deepEqual(yield* messages.read("/tasks/a"), []);
    }),
  );
});

test("shared Pi turn includes busy steering before completion and retains context for later work", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const conversations = yield* AgentConversations.makeMemory();
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        let calls = 0;
        const models = Layer.succeed(Models, {
          resolve: () =>
            Effect.succeed({
              model: {
                id: "test",
                name: "test",
                provider: "test",
                api: "openai-completions" as const,
                baseUrl: "http://unused",
                reasoning: false,
                input: ["text" as const],
                contextWindow: 10000,
                maxTokens: 100,
                cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
              },
              getApiKey: () => "test",
              stream: (_model, context) => {
                calls++;
                if (calls > 1)
                  assert.match(JSON.stringify(context.messages), /Add regional analysis/);
                const message: AssistantMessage = {
                  role: "assistant",
                  provider: "test",
                  model: "test",
                  api: "openai-completions",
                  timestamp: 0,
                  stopReason: calls === 1 ? "toolUse" : "stop",
                  content:
                    calls === 1
                      ? [{ type: "toolCall", id: "read", name: "read", arguments: {} }]
                      : [{ type: "text", text: `Answer ${calls}` }],
                  usage: {
                    input: 0,
                    output: 0,
                    cacheRead: 0,
                    cacheWrite: 0,
                    totalTokens: 0,
                    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
                  },
                };
                const stream = createAssistantMessageEventStream();
                stream.push({ type: "done", reason: calls === 1 ? "toolUse" : "stop", message });
                return stream;
              },
            }),
        });
        const runner = yield* AgentRunner.pipe(
          Effect.provide(
            AgentRunner.layer.pipe(
              Layer.provide(models),
              Layer.provide(Layer.succeed(AgentConversations, conversations)),
            ),
          ),
        );
        const execute = (requestId: string) =>
          runner.run((invoke) =>
            Effect.succeed({
              name: "test",
              durable: { owner: "tasks", sessionId: "work", requestId },
              messages: [{ role: "user", content: "Analyze", timestamp: 0 }],
              tools: [
                {
                  name: "read",
                  label: "Read",
                  description: "Read",
                  parameters: Type.Object({}),
                  replay: "safe",
                  execute: async (_id, _args, signal) => {
                    await invoke(
                      Deferred.succeed(entered, undefined).pipe(
                        Effect.andThen(Deferred.await(release)),
                      ),
                      signal,
                    );
                    return { content: [{ type: "text", text: "Evidence" }], details: undefined };
                  },
                },
              ],
            }),
          );
        const work = yield* execute("initial").pipe(Effect.forkScoped);
        yield* Deferred.await(entered);
        yield* conversations.append("/tasks/work", "followup", "task.input", {
          text: "Add regional analysis",
        });
        assert.equal(
          yield* conversations.steer("/tasks/work", "followup", "Add regional analysis"),
          true,
        );
        assert.equal(work.pollUnsafe(), undefined);
        yield* Deferred.succeed(release, undefined);
        const result = yield* Fiber.join(work);
        assert.match(JSON.stringify(result), /Answer 2/);
        assert.equal(yield* conversations.steer("/tasks/work", "idle", "Do not start"), false);
        assert.deepEqual(yield* execute("initial"), result);
        assert.equal(calls, 2);
        assert.match(JSON.stringify(yield* execute("later")), /Answer 3/);
      }),
    ).pipe(Effect.timeout("10 seconds")),
  );
});

test("interrupting a shared conversation drains tool callbacks before another turn can acquire it", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const conversations = yield* AgentConversations.makeMemory();
        const entered = yield* Deferred.make<void>();
        const released = yield* Deferred.make<void>();
        let calls = 0;
        const models = Layer.succeed(Models, {
          resolve: () =>
            Effect.succeed({
              model: {
                id: "test",
                name: "test",
                provider: "test",
                api: "openai-completions" as const,
                baseUrl: "http://unused",
                reasoning: false,
                input: ["text" as const],
                contextWindow: 10000,
                maxTokens: 100,
                cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
              },
              getApiKey: () => "test",
              stream: () => {
                calls++;
                const message: AssistantMessage = {
                  role: "assistant",
                  provider: "test",
                  model: "test",
                  api: "openai-completions",
                  timestamp: 0,
                  stopReason: "toolUse",
                  content: [{ type: "toolCall", id: "read", name: "read", arguments: {} }],
                  usage: {
                    input: 0,
                    output: 0,
                    cacheRead: 0,
                    cacheWrite: 0,
                    totalTokens: 0,
                    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
                  },
                };
                const stream = createAssistantMessageEventStream();
                stream.push({ type: "done", reason: "toolUse", message });
                return stream;
              },
            }),
        });
        const runner = yield* AgentRunner.pipe(
          Effect.provide(
            AgentRunner.layer.pipe(
              Layer.provide(models),
              Layer.provide(Layer.succeed(AgentConversations, conversations)),
            ),
          ),
        );
        const work = yield* runner
          .run((invoke) =>
            Effect.succeed({
              name: "test",
              durable: { owner: "tasks", sessionId: "cancel", requestId: "initial" },
              messages: [{ role: "user", content: "Analyze", timestamp: 0 }],
              tools: [
                {
                  name: "read",
                  label: "Read",
                  description: "Read",
                  parameters: Type.Object({}),
                  replay: "safe",
                  execute: async (_id, _args, signal) => {
                    await invoke(
                      Deferred.succeed(entered, undefined).pipe(
                        Effect.andThen(Effect.never),
                        Effect.ensuring(Deferred.succeed(released, undefined)),
                      ),
                      signal,
                    );
                    return { content: [{ type: "text", text: "Evidence" }], details: undefined };
                  },
                },
              ],
            }),
          )
          .pipe(Effect.forkScoped);
        yield* Deferred.await(entered);
        yield* Fiber.interrupt(work);
        assert.equal(yield* Deferred.isDone(released), true);
        assert.equal(yield* conversations.steer("/tasks/cancel", "late", "Too late"), false);
        const driver = yield* conversations.driver("/tasks/cancel");
        yield* driver.exclusive(Effect.void);
        yield* conversations.append("/tasks/cancel", "retained", "task.input", {
          text: "Retained after cancellation",
        });
        assert.equal(calls, 1);
        assert.equal((yield* conversations.read("/tasks/cancel")).length, 1);
      }),
    ).pipe(Effect.timeout("10 seconds")),
  );
});
