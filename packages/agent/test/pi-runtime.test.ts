import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { BACKGROUND_CONTEXT, withAbortSignal } from "@earendil-works/chord/context";
import { MemoryStorage } from "@earendil-works/pi-durable";
import { openNodeJsonlStorage } from "@earendil-works/pi-durable/storage/jsonl/node";
import {
  createAssistantMessageEventStream,
  Type,
  type AssistantMessage,
} from "@earendil-works/pi-ai";
import { Deferred, Effect, Fiber, Option } from "effect";
import {
  PiDurableAgentRuntime,
  PiRuntimeError,
  type ResolvedModel,
  type AgentTool,
  type PiExecutionHandle,
} from "../src/index.js";

const model: ResolvedModel["model"] = {
  id: "test",
  name: "test",
  provider: "test",
  api: "openai-completions",
  baseUrl: "http://unused",
  reasoning: false,
  input: ["text"],
  contextWindow: 10000,
  maxTokens: 100,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
};
const answer = (
  content: AssistantMessage["content"] = [{ type: "text", text: "Prepared task completed" }],
  stopReason: AssistantMessage["stopReason"] = "stop",
): AssistantMessage => ({
  role: "assistant",
  api: "openai-completions",
  provider: "test",
  model: "test",
  content,
  stopReason,
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
const resolved = (respond: () => AssistantMessage): ResolvedModel => ({
  model,
  getApiKey: () => "test",
  stream: () => {
    const stream = createAssistantMessageEventStream();
    const message = respond();
    stream.push({
      type: "done",
      reason: message.stopReason === "toolUse" ? "toolUse" : "stop",
      message,
    });
    return stream;
  },
});
const request = {
  requestId: "/delegations/run-1",
  prompt: "Analyze the prepared evidence",
  instructions: "Read-only analysis",
};
const openAt = (directory: string) =>
  Effect.tryPromise({
    try: (signal) =>
      openNodeJsonlStorage(directory, withAbortSignal(signal, BACKGROUND_CONTEXT), { fsync: true }),
    catch: (cause) => new PiRuntimeError({ operation: "open", cause }),
  });

test("Pi runtime persists one task-owned execution per request, rejects altered reuse and projects its result", async () => {
  const storage = new MemoryStorage();
  let calls = 0;
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* PiDurableAgentRuntime.make({
          ownerId: "test",
          openStorage: Effect.succeed(storage),
          catalogueId: "analysis.v1",
          resolved: resolved(() => {
            calls++;
            return answer();
          }),
        });
        assert.equal(Option.isNone(yield* runtime.lookup(request)), true);
        assert.equal(calls, 0, "inspection cannot start model work");
        const handle = yield* runtime.submit(request);
        assert.deepEqual(yield* runtime.wait(handle), {
          state: "completed",
          text: "Prepared task completed",
        });
        assert.deepEqual(yield* runtime.lookup(request), Option.some(handle));
        assert.equal(
          (yield* runtime.lookup({ ...request, prompt: "Altered evidence" }).pipe(Effect.flip))
            .operation,
          "lookup",
        );
        assert.equal(
          Option.isNone(yield* runtime.lookup({ ...request, requestId: "absent" })),
          true,
        );
        assert.equal(calls, 1);
        assert.deepEqual(yield* runtime.submit(request), handle);
        assert.deepEqual(yield* runtime.status(handle), {
          state: "completed",
          text: "Prepared task completed",
        });
        yield* runtime.resume(handle);
        assert.equal(calls, 1);
        const conflict = yield* runtime
          .submit({ ...request, prompt: "Changed evidence" })
          .pipe(Effect.flip);
        assert.equal(conflict.operation, "submit");
        const invalid = yield* runtime.status({ ...handle, sessionId: "9999" }).pipe(Effect.flip);
        assert.equal(invalid.operation, "status");
        const tasks = yield* Effect.promise(() =>
          storage.scanTasks({ kind: "app.aster.execution" }, 100, undefined, BACKGROUND_CONTEXT),
        );
        assert.equal(tasks.items.length, 1);
        const conversations = yield* Effect.promise(() =>
          storage.scanConversations(
            { ownerTaskId: tasks.items[0].id },
            10,
            undefined,
            BACKGROUND_CONTEXT,
          ),
        );
        assert.equal(conversations.items.length, 1);
        assert.equal(String(conversations.items[0].id), handle.sessionId);
        const entries = yield* Effect.promise(() =>
          storage.scanEntries(
            { conversationId: tasks.items[0].conversationId },
            100,
            undefined,
            BACKGROUND_CONTEXT,
          ),
        );
        assert.deepEqual(
          entries.items.map((entry) => entry.kind),
          ["app.aster.execution.result", "app.aster.execution.accepted"],
        );
        assert.ok(entries.items.every((entry) => entry.model === undefined));
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("Pi runtime reopens completed execution without calling the model again", async (t) => {
  const directory = mkdtempSync(join(tmpdir(), "aster-pi-execution-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  let calls = 0;
  const options = {
    ownerId: "test",
    openStorage: openAt(directory),
    catalogueId: "analysis.v1",
    resolved: resolved(() => {
      calls++;
      return answer();
    }),
  };
  const handle = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* PiDurableAgentRuntime.make(options);
        const handle = yield* runtime.submit(request);
        yield* runtime.wait(handle);
        return handle;
      }),
    ),
  );
  const wrongOwner = await Effect.runPromise(
    PiDurableAgentRuntime.make({ ...options, ownerId: "another-owner" }).pipe(
      Effect.scoped,
      Effect.flip,
    ),
  );
  assert.equal(wrongOwner.operation, "open");
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* PiDurableAgentRuntime.make(options);
        assert.equal((yield* runtime.status(handle)).state, "completed");
        assert.deepEqual(yield* runtime.lookup(request), Option.some(handle));
        assert.deepEqual(yield* runtime.submit(request), handle);
        assert.equal((yield* runtime.wait(handle)).state, "completed");
      }),
    ),
  );
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* PiDurableAgentRuntime.make({
          ...options,
          catalogueId: "analysis.v2",
        });
        assert.deepEqual(
          yield* runtime.lookup(request),
          Option.some(handle),
          "reading old admission uses its retained configuration",
        );
        assert.equal(
          (yield* runtime.submit(request).pipe(Effect.flip)).operation,
          "submit",
          "new submission still validates the complete frozen configuration",
        );
      }),
    ),
  );
  assert.equal(calls, 1);
});

for (const replay of ["safe", "unsafe"] as const) {
  test(`Pi runtime ${replay} tool recovery preserves the execution identity and unknown outcomes`, async (t) => {
    const directory = mkdtempSync(join(tmpdir(), `aster-pi-${replay}-`));
    t.after(() => rmSync(directory, { recursive: true, force: true }));
    let toolCalls = 0;
    let modelCalls = 0;
    const entered = Deferred.makeUnsafe<void>();
    const handleReady = Deferred.makeUnsafe<PiExecutionHandle>();
    const tool: AgentTool = {
      name: "work",
      label: "Work",
      description: "Test work",
      parameters: Type.Object({}),
      replay: replay === "safe" ? "safe" : "never",
      execute: async (_id, _args, signal) => {
        toolCalls++;
        if (toolCalls === 1) {
          await Effect.runPromise(Deferred.succeed(entered, undefined));
          await new Promise<void>((_resolve, reject) => {
            if (signal?.aborted) reject(new Error("interrupted"));
            else
              signal?.addEventListener("abort", () => reject(new Error("interrupted")), {
                once: true,
              });
          });
        }
        return { content: [{ type: "text", text: "Work done" }], details: {} };
      },
    };
    const options = {
      ownerId: "test",
      openStorage: openAt(directory),
      catalogueId: `test-${replay}.v1`,
      tools: [tool],
      resolved: resolved(() => {
        modelCalls++;
        if (modelCalls === 1 || replay === "unsafe")
          return answer(
            [{ type: "toolCall", id: `call-${modelCalls}`, name: "work", arguments: {} }],
            "toolUse",
          );
        return answer();
      }),
    };
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const running = yield* Effect.scoped(
            Effect.gen(function* () {
              const runtime = yield* PiDurableAgentRuntime.make(options);
              const handle = yield* runtime.submit(request);
              yield* Deferred.succeed(handleReady, handle);
              yield* runtime.wait(handle);
            }),
          ).pipe(Effect.forkScoped);
          yield* Deferred.await(entered);
          yield* Fiber.interrupt(running);
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
    const handle = await Effect.runPromise(Deferred.await(handleReady));
    const changedCatalogue = await Effect.runPromise(
      PiDurableAgentRuntime.make({ ...options, catalogueId: "changed.v2" }).pipe(
        Effect.scoped,
        Effect.flip,
      ),
    );
    assert.equal(changedCatalogue.operation, "open");
    const changedPolicy = await Effect.runPromise(
      PiDurableAgentRuntime.make({
        ...options,
        environment: {
          policyId: "different-authority.v1",
          open: async () => assert.fail("Changed policy must fail before environment access"),
        },
      }).pipe(Effect.scoped, Effect.flip),
    );
    assert.equal(changedPolicy.operation, "open");
    assert.match(String(changedPolicy.cause), /environment policy/);
    assert.equal(modelCalls, 1);
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const runtime = yield* PiDurableAgentRuntime.make(options);
          assert.equal((yield* runtime.status(handle)).state, "running");
          assert.deepEqual(yield* runtime.resume(handle), handle);
          const result = yield* runtime.wait(handle);
          assert.equal(result.state, replay === "unsafe" ? "unknown" : "completed");
          assert.equal(toolCalls, replay === "unsafe" ? 1 : 2);
          assert.equal(modelCalls, replay === "unsafe" ? 1 : 2);
          assert.deepEqual(yield* runtime.submit(request), handle);
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
  });
}

test("Pi runtime stops new tool calls after an unsafe tool throws with an uncertain outcome", async () => {
  let calls = 0;
  let generations = 0;
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* PiDurableAgentRuntime.make({
          ownerId: "test",
          openStorage: Effect.succeed(new MemoryStorage()),
          catalogueId: "unsafe.v1",
          resolved: resolved(() =>
            answer(
              [{ type: "toolCall", id: `call-${++generations}`, name: "write", arguments: {} }],
              "toolUse",
            ),
          ),
          tools: [
            {
              name: "write",
              label: "Write",
              description: "Unsafe test action",
              parameters: Type.Object({}),
              replay: "never",
              execute: async () => {
                calls++;
                throw new Error("Action completed but acknowledgement lost");
              },
            },
          ],
        });
        const handle = yield* runtime.submit(request);
        assert.equal((yield* runtime.wait(handle)).state, "unknown");
        assert.equal(calls, 1);
        assert.equal(generations, 1);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("Pi runtime fences a mixed tool round after a returned unknown write outcome", async () => {
  const calls: string[] = [];
  let generations = 0;
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const runtime = yield* PiDurableAgentRuntime.make({
          ownerId: "test",
          openStorage: Effect.succeed(new MemoryStorage()),
          catalogueId: "mixed.v1",
          resolved: resolved(() => {
            generations++;
            return answer(
              ["read", "write", "later"].map((name) => ({
                type: "toolCall" as const,
                id: name,
                name,
                arguments: {},
              })),
              "toolUse",
            );
          }),
          tools: ["read", "write", "later"].map((name): AgentTool => ({
            name,
            label: name,
            description: name,
            parameters: Type.Object({}),
            replay: name === "write" ? "never" : "safe",
            execute: async () => {
              calls.push(name);
              return {
                isError: name === "write",
                content: [
                  { type: "text", text: name === "write" ? "Acknowledgement lost" : "Read" },
                ],
                details: {},
              };
            },
          })),
        });
        const handle = yield* runtime.submit(request);
        assert.equal((yield* runtime.wait(handle)).state, "unknown");
        assert.deepEqual(calls, ["read", "write"]);
        assert.equal(generations, 1);
        assert.deepEqual(yield* runtime.submit(request), handle);
        assert.equal((yield* runtime.wait(handle)).state, "unknown");
        assert.equal(generations, 1);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});
