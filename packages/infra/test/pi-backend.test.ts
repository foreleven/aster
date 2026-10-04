import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { Deferred, Effect, Fiber, Layer } from "effect";
import {
  Models,
  PiDurableAgentRuntime,
  PiRuntimeError,
  PiStorageLease,
  type ResolvedModel,
} from "@aster/agent";
import { BACKGROUND_CONTEXT, withAbortSignal } from "@earendil-works/chord/context";
import { Harness, MemoryStorage } from "@earendil-works/pi-durable";
import { openNodeJsonlStorage } from "@earendil-works/pi-durable/storage/jsonl/node";
import {
  Type,
  createAssistantMessageEventStream,
  type AssistantMessage,
} from "@earendil-works/pi-ai";
import { PiDurableContext } from "../src/storage/pi-durable-context.js";
import { PiDurableBackend } from "../src/pi/backend.js";

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
const answer = (tool = false): AssistantMessage => ({
  role: "assistant",
  api: "openai-completions",
  provider: "test",
  model: "test",
  content: tool
    ? [{ type: "toolCall", id: "call-1", name: "read", arguments: {} }]
    : [{ type: "text", text: "Execution result" }],
  stopReason: tool ? "toolUse" : "stop",
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
const fakeModel = (reply: () => AssistantMessage): ResolvedModel => ({
  model,
  getApiKey: () => "test",
  stream: () => {
    const stream = createAssistantMessageEventStream();
    const message = reply();
    stream.push({
      type: "done",
      reason: message.stopReason === "toolUse" ? "toolUse" : "stop",
      message,
    });
    return stream;
  },
});
const record = { path: "/personal", description: "Personal", state: { value: 1 }, messages: [] };
const request = {
  requestId: "/delegations/task",
  prompt: "Analyze evidence",
  instructions: "Read only",
};

test("shared Pi owner permits Context commits while a task observer waits and closes storage once", async () => {
  const storage = new MemoryStorage();
  let closes = 0;
  let calls = 0;
  const close = storage.close.bind(storage);
  storage.close = async (context) => {
    closes++;
    await close(context);
  };
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const runtime = yield* PiDurableAgentRuntime.make({
          ownerId: "shared",
          catalogueId: "test.v1",
          openStorage: Effect.succeed(storage),
          resolved: fakeModel(() => answer(++calls === 1)),
          tools: [
            {
              name: "read",
              label: "Read",
              description: "Gated read",
              replay: "safe",
              parameters: Type.Object({}),
              execute: async (_id, _args, signal) => {
                await Effect.runPromise(Deferred.succeed(entered, undefined));
                await Effect.runPromise(Deferred.await(release), { signal });
                return { content: [{ type: "text", text: "Read evidence" }], details: {} };
              },
            },
          ],
        });
        const contexts = yield* PiDurableContext.fromRuntime(runtime);
        yield* contexts.commit(record, { expectedRevision: 0 });
        assert.equal(calls, 0);
        const handle = yield* runtime.submit(request);
        const observer = yield* runtime.wait(handle).pipe(Effect.forkScoped);
        yield* Deferred.await(entered);
        const changed = yield* contexts
          .commit({ ...record, state: { value: 2 } }, { expectedRevision: 1 })
          .pipe(Effect.timeout("1 second"));
        assert.equal(changed.revision, 2);
        // Cancelling only observation does not cancel the accepted durable execution.
        yield* Fiber.interrupt(observer);
        yield* Deferred.succeed(release, undefined);
        assert.equal((yield* runtime.wait(handle)).state, "completed");
        assert.equal(contexts.get(record.path)?.revision, 2);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
  assert.equal(closes, 1);
});

for (const failKind of ["app.aster.context.commit", "app.aster.execution.accepted"] as const) {
  test(`shared Pi owner recovers a lost ${failKind} acknowledgement without splitting storage ownership`, async (t) => {
    const directory = mkdtempSync(join(tmpdir(), "aster-pi-shared-"));
    t.after(() => rmSync(directory, { recursive: true, force: true }));
    let opens = 0;
    let closes = 0;
    let fault = false;
    let calls = 0;
    const openStorage = Effect.tryPromise({
      try: async (signal) => {
        assert.equal(opens, closes, "previous storage must be closed before reopening");
        const storage = await openNodeJsonlStorage(
          directory,
          withAbortSignal(signal, BACKGROUND_CONTEXT),
          { fsync: true },
        );
        opens++;
        const commit = storage.commit.bind(storage);
        const close = storage.close.bind(storage);
        storage.close = async (context) => {
          await close(context);
          closes++;
        };
        storage.commit = async (writes, context) => {
          const lost =
            fault &&
            writes.some((write) => write.type === "entry" && write.value.kind === failKind);
          const seq = await commit(writes, context);
          if (lost) {
            fault = false;
            throw new Error("Lost storage acknowledgement");
          }
          return seq;
        };
        return storage;
      },
      catch: (cause) => new PiRuntimeError({ operation: "open", cause }),
    });
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const runtime = yield* PiDurableAgentRuntime.make({
            ownerId: "shared",
            openStorage,
            catalogueId: "test.v1",
            resolved: fakeModel(() => {
              calls++;
              return answer();
            }),
          });
          const contexts = yield* PiDurableContext.fromRuntime(runtime);
          yield* contexts.commit(record, { expectedRevision: 0 });
          const first = yield* runtime.submit(request);
          yield* runtime.wait(first);
          fault = true;
          if (failKind === "app.aster.context.commit") {
            assert.equal(
              (yield* contexts
                .commit({ ...record, state: { value: 2 } }, { expectedRevision: 1 })
                .pipe(Effect.flip))._tag,
              "ContextCommitError",
            );
            assert.equal((yield* runtime.status(first).pipe(Effect.flip))._tag, "PiRuntimeError");
            yield* contexts.recover(record.path, (record) => record);
            assert.equal(contexts.get(record.path)?.revision, 2);
          } else {
            assert.equal(
              (yield* runtime.submit({ ...request, requestId: "next" }).pipe(Effect.flip))._tag,
              "PiRuntimeError",
            );
            yield* runtime.recover;
            const existing = yield* runtime.lookup({ ...request, requestId: "next" });
            assert.equal(existing._tag, "Some");
            assert.equal(calls, 1, "lookup cannot resume the recovered task");
            assert.equal((yield* runtime.wait(existing.value)).state, "completed");
            assert.equal(calls, 2);
          }
          assert.equal(opens, 2);
          assert.equal(closes, 1);
          assert.equal((yield* runtime.status(first)).state, "completed");
          const current = contexts.get(record.path)!;
          yield* contexts.commit(
            { ...current, messages: ["after recovery"] },
            { expectedRevision: current.revision! },
          );
          const stored = yield* runtime.withSession((session, context) =>
            session.commit(async (tx) => {
              const executions = await tx.scanTasks({ kind: "app.aster.execution" }, 10);
              return executions.items.length;
            }, context),
          );
          assert.equal(stored, failKind === "app.aster.context.commit" ? 1 : 2);
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
    assert.equal(opens, closes);
  });
}

test("combined Pi backend reopens Contexts and executor handles from one configured directory", async (t) => {
  const directory = mkdtempSync(join(tmpdir(), "aster-pi-combined-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  let calls = 0;
  const models = Layer.succeed(Models, {
    resolve: () =>
      Effect.succeed(
        fakeModel(() => {
          calls++;
          return answer();
        }),
      ),
  });
  const options = { model: "test", directory, shardId: "shared" };
  const handle = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const backend = yield* PiDurableBackend.make(options);
        yield* backend.contexts.commit(record, { expectedRevision: 0 });
        const handle = yield* backend.agent.submit(
          { instructions: "Analyze", input: [] },
          { requestId: "task" },
        );
        yield* backend.agent.wait(handle);
        return handle;
      }),
    ).pipe(Effect.provide(models)),
  );
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const backend = yield* PiDurableBackend.make(options);
        assert.equal(backend.contexts.get(record.path)?.revision, 1);
        assert.equal((yield* backend.agent.status(handle)).state, "completed");
      }),
    ).pipe(Effect.provide(models)),
  );
  assert.equal(calls, 1);
});

test("shared owner recovery retires active observers and preserves unknown unsafe execution outcomes", async (t) => {
  const directory = mkdtempSync(join(tmpdir(), "aster-pi-owner-retire-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  let generations = 0;
  let toolCalls = 0;
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const runtime = yield* PiDurableAgentRuntime.make({
          ownerId: "shared",
          catalogueId: "unsafe.v1",
          openStorage: Effect.tryPromise({
            try: (signal) =>
              openNodeJsonlStorage(directory, withAbortSignal(signal, BACKGROUND_CONTEXT), {
                fsync: true,
              }),
            catch: (cause) => new PiRuntimeError({ operation: "open", cause }),
          }),
          resolved: fakeModel(() => answer(++generations === 1)),
          tools: [
            {
              name: "read",
              label: "Unsafe action",
              description: "Test interrupted side effect",
              parameters: Type.Object({}),
              replay: "never",
              execute: async (_id, _args, signal) => {
                toolCalls++;
                await Effect.runPromise(Deferred.succeed(entered, undefined));
                await Effect.runPromise(Effect.never, { signal });
                return { content: [], details: {} };
              },
            },
          ],
        });
        const contexts = yield* PiDurableContext.fromRuntime(runtime);
        yield* contexts.commit(record, { expectedRevision: 0 });
        const handle = yield* runtime.submit(request);
        const retired = yield* runtime.wait(handle).pipe(Effect.flip, Effect.forkScoped);
        yield* Deferred.await(entered);
        yield* runtime.recover;
        assert.equal((yield* Fiber.join(retired))._tag, "PiRuntimeError");
        assert.equal((yield* runtime.wait(handle)).state, "unknown");
        assert.equal(toolCalls, 1);
        assert.equal(contexts.get(record.path)?.revision, 1);
        yield* contexts.commit({ ...record, state: { value: 2 } }, { expectedRevision: 1 });
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("wrong execution owner cannot contaminate a Context-only Pi archive", async (t) => {
  const directory = mkdtempSync(join(tmpdir(), "aster-pi-preflight-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const contexts = yield* PiDurableContext.directory({ directory, shardId: "original" });
        yield* contexts.commit(record, { expectedRevision: 0 });
      }),
    ),
  );
  const models = Layer.succeed(Models, {
    resolve: () =>
      Effect.succeed(
        fakeModel(() => {
          throw new Error("Preflight must not invoke a model");
        }),
      ),
  });
  await assert.rejects(
    Effect.runPromise(
      PiDurableBackend.make({ directory, shardId: "wrong", model: "test" }).pipe(
        Effect.scoped,
        Effect.provide(models),
      ),
    ),
    /PiRuntimeError/,
  );
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const contexts = yield* PiDurableContext.directory({ directory, shardId: "original" });
        assert.deepEqual(contexts.get(record.path), { ...record, revision: 1 });
      }),
    ),
  );
  await Effect.runPromise(
    PiDurableBackend.make({ directory, shardId: "original", model: "test" }).pipe(
      Effect.scoped,
      Effect.provide(models),
    ),
  );
});

test("Shared Pi shutdown uncertainty quarantines ownership across executor and Context openers", async (t) => {
  const directory = mkdtempSync(join(tmpdir(), "aster-shared-close-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const originalOpen = Harness.open;
  t.mock.method(Harness, "open", async (...args: Parameters<typeof Harness.open>) => {
    const harness = await originalOpen(...args);
    const close = harness.close.bind(harness);
    harness.close = async (context) => {
      await close(context);
      throw new Error("Unknown close outcome");
    };
    return harness;
  });
  const models = Layer.succeed(Models, {
    resolve: () =>
      Effect.succeed(
        fakeModel(() => {
          throw new Error("No model call expected");
        }),
      ),
  });
  const exit = await Effect.runPromiseExit(
    Effect.scoped(PiDurableBackend.make({ model: "test", directory, shardId: "shared" })).pipe(
      Effect.provide(models),
    ),
  );
  assert.equal(exit._tag, "Failure");
  const owners = await Effect.runPromise(PiStorageLease.inspect);
  assert.ok(owners.some((owner) => owner.ownerId === "shared" && owner.status === "quarantined"));
  const failure = await Effect.runPromise(
    Effect.scoped(PiDurableContext.directory({ directory, shardId: "shared" }).pipe(Effect.flip)),
  );
  assert.equal(failure._tag, "ContextRecoveryError");
});
