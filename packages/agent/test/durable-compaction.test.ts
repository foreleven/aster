import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { Effect, Deferred } from "effect";
import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import {
  CompactionTask,
  GenerationTask,
  Harness,
  configure,
  createRegistry,
  defineExtension,
  hook,
} from "@earendil-works/pi-durable";
import { openNodeJsonlStorage } from "@earendil-works/pi-durable/storage/jsonl/node";
import { createAssistantMessageEventStream, type AssistantMessage } from "@earendil-works/pi-ai";
import type { ResolvedModel } from "../src/index.js";
import { durableModels } from "../src/durable.js";
import { generationFence } from "../src/durable-tools.js";

const context = BACKGROUND_CONTEXT;
const message = (stopReason: "stop" | "aborted" = "stop"): AssistantMessage => ({
  role: "assistant",
  provider: "test",
  model: "test",
  api: "openai-completions",
  content: [{ type: "text", text: "Retained summary" }],
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
const open = async (directory: string, stream: ResolvedModel["stream"]) => {
  const registry = createRegistry();
  const fence = generationFence(() => harness, new Set(["write"]));
  const extension = defineExtension({
    name: "aster-compaction-test",
    hooks: [
      hook(GenerationTask, { beforeRequest: fence.beforeRequest }),
      hook(CompactionTask, { beforeCompact: fence.beforeCompact }),
    ],
  });
  registry.install(extension);
  const storage = await openNodeJsonlStorage(directory, context, { fsync: true });
  const harness = await Harness.open(
    storage,
    {
      registry,
      models: durableModels({ model, getApiKey: () => "unused", stream }, fence.beforeModel),
      settings: {
        compaction: { keepRecentTokens: 1, reserveTokens: 100 },
        retry: { enabled: false },
      },
    },
    context,
  );
  const root = await harness.root(context);
  await harness.commit(
    (tx) =>
      configure(tx, root.id, {
        model: { provider: "test", modelId: "test" },
        extensions: [extension],
      }),
    context,
  );
  return { harness, root };
};
const seed = async ({ harness, root }: Awaited<ReturnType<typeof open>>) => {
  await harness.commit(async (tx) => {
    for (let i = 0; i < 4; i++) {
      await tx.appendEntry(root.id, {
        kind: "test.input",
        model: [
          {
            role: "user",
            content: [{ type: "text", text: `Evidence ${i} ` + "detail ".repeat(100) }],
            timestamp: 0,
          },
        ],
      });
      await tx.appendEntry(root.id, { kind: "test.answer", model: [message()] });
    }
  }, context);
};
const unknown = async ({ harness, root }: Awaited<ReturnType<typeof open>>) => {
  await harness.commit(
    (tx) =>
      tx.appendEntry(root.id, {
        kind: "pi.tool-result",
        data: { diagnostics: [{ code: "aster.unknown" }] },
        model: [
          {
            role: "toolResult",
            toolCallId: "uncertain-write",
            toolName: "write",
            content: [{ type: "text", text: "Unknown external outcome" }],
            isError: true,
            timestamp: 0,
          },
        ],
      }),
    context,
  );
};

for (const uncertain of [false, true]) {
  test(`native compaction ${uncertain ? "declines unknown work" : "summarizes safe retained evidence"}`, async (t) => {
    const directory = mkdtempSync(join(tmpdir(), "aster-compaction-"));
    t.after(() => rmSync(directory, { recursive: true, force: true }));
    let calls = 0;
    const runtime = await open(directory, () => {
      calls++;
      const stream = createAssistantMessageEventStream();
      stream.push({ type: "done", reason: "stop", message: message() });
      return stream;
    });
    try {
      await seed(runtime);
      if (uncertain) await unknown(runtime);
      const task = await runtime.root.compact(undefined, context);
      runtime.harness.resume();
      const result = await runtime.harness.waitForTask(task, context);
      assert.equal(result.state.status, "terminal");
      assert.equal(result.state.outcome.status, "completed");
      assert.equal(calls, uncertain ? 0 : 1);
      if (uncertain && result.state.outcome.status === "completed")
        assert.deepEqual(result.state.outcome.result, {});
    } finally {
      await runtime.harness.close(context);
    }
  });
}

test("recovered native summarize checkpoint cannot bypass the unknown-outcome fence", async (t) => {
  const directory = mkdtempSync(join(tmpdir(), "aster-compaction-recovery-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const entered = Deferred.makeUnsafe<void>();
  let calls = 0;
  const runtime = await open(directory, (_model, _context, options) => {
    calls++;
    const stream = createAssistantMessageEventStream();
    options?.signal?.addEventListener(
      "abort",
      () => {
        stream.push({ type: "error", reason: "aborted", error: message("aborted") });
      },
      { once: true },
    );
    Effect.runSync(Deferred.succeed(entered, undefined));
    return stream;
  });
  let task: Awaited<ReturnType<typeof runtime.root.compact>>;
  try {
    await seed(runtime);
    task = await runtime.root.compact(undefined, context);
    runtime.harness.resume();
    await Effect.runPromise(Deferred.await(entered).pipe(Effect.timeout("5 seconds")));
    // A concurrent tool's unknown outcome commits after range selection. The
    // resumed summarize phase has no beforeCompact callback to catch it.
    await unknown(runtime);
  } finally {
    await runtime.harness.close(context);
  }
  const recovered = await open(directory, () => {
    calls++;
    assert.fail("Unattributed model invocation must not reach the provider");
  });
  try {
    recovered.harness.resume();
    const result = await recovered.harness.waitForTask(task!, context);
    assert.equal(result.state.outcome.status, "failed");
    if (result.state.outcome.status === "failed")
      assert.match(result.state.outcome.error.message, /outcome is unknown/);
    assert.equal(calls, 1);
  } finally {
    await recovered.harness.close(context);
  }
});
