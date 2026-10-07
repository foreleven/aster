import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { Effect, Layer } from "effect";
import { BACKGROUND_CONTEXT, withAbortSignal } from "@earendil-works/chord/context";
import { getOrThrow } from "@earendil-works/pi-durable/env";
import { Models, type ResolvedModel } from "@aster/agent";
import { createAssistantMessageEventStream, type AssistantMessage } from "@earendil-works/pi-ai";
import { evidenceEnvironment } from "../src/pi/sandbox.js";
import { makePiAgent } from "../src/pi/agent.js";

const context = BACKGROUND_CONTEXT;
const open = (namespace: string, prompt: string) =>
  evidenceEnvironment({ namespace, prompt, instructions: "Analyze evidence" });

test("prepared-evidence sandbox denies host access and every mutation even after cwd changes", async (t) => {
  const directory = mkdtempSync(join(tmpdir(), "aster-sandbox-host-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const hostFile = join(directory, "credential.txt");
  writeFileSync(hostFile, "host-secret");
  const env = await open("task-a", "Frozen evidence\nSecond line");
  env.cwd = directory;
  assert.equal(
    getOrThrow(await env.readTextFile("input.md", context)),
    "Frozen evidence\nSecond line",
  );
  const bytes = getOrThrow(await env.readBinaryFile("input.md", context));
  bytes.fill(0);
  assert.match(getOrThrow(await env.readTextFile("input.md", context)), /Frozen evidence/);
  for (const path of [
    hostFile,
    "../../etc/passwd",
    "/etc/passwd",
    "/proc/self/environ",
    "~/.ssh/id_rsa",
    "file:///etc/passwd",
    "input.md\0",
    "..\\credential.txt",
    "/task/other",
  ]) {
    for (const operation of [
      env.readTextFile,
      env.readBinaryFile,
      env.absolutePath,
      env.canonicalPath,
      env.fileInfo,
      env.exists,
    ]) {
      const result = await operation(path, context);
      assert.equal(result.ok, false, path);
      if (!result.ok) assert.equal(result.error.code, "permission_denied");
    }
  }
  for (const result of await Promise.all([
    env.writeFile(hostFile, "changed", context),
    env.appendFile("input.md", "changed", context),
    env.truncateFile("input.md", 0, context),
    env.flushFile("input.md", context),
    env.renameFile("input.md", hostFile, context),
    env.createDir("subdir", {}, context),
    env.remove(hostFile, { force: true }, context),
    env.createTempDir(undefined, context),
    env.createTempFile(undefined, context),
  ])) {
    assert.equal(result.ok, false);
    if (!result.ok) assert.equal(result.error.code, "permission_denied");
  }
  let output = false;
  const shell = await env.exec(
    "printf changed",
    {
      inheritEnv: true,
      env: { SECRET: "cannot-become-a-capability" },
      onOutput: () => {
        output = true;
      },
    },
    context,
  );
  assert.equal(shell.ok, false);
  if (!shell.ok) assert.equal(shell.error.code, "shell_unavailable");
  assert.equal(output, false);
  assert.equal(readFileSync(hostFile, "utf8"), "host-secret");
  const other = await open("task-b", "Other evidence");
  assert.notEqual(other.id, env.id);
  assert.equal(getOrThrow(await other.readTextFile("input.md", context)), "Other evidence");
  assert.equal((await open("task-a", "Frozen evidence\nSecond line")).id, env.id);
  const aborted = new AbortController();
  aborted.abort();
  const cancelled = await env.readTextFile("input.md", withAbortSignal(aborted.signal, context));
  assert.equal(cancelled.ok, false);
  if (!cancelled.ok) assert.equal(cancelled.error.code, "aborted");
});

test("production Pi read tool sees admitted evidence, rejects host files, and replays without new access", async (t) => {
  const directory = mkdtempSync(join(tmpdir(), "aster-sandbox-pi-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const hostFile = join(directory, "private.txt");
  writeFileSync(hostFile, "host-secret");
  let calls = 0;
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
  const models = Layer.succeed(Models, {
    resolve: () =>
      Effect.succeed({
        model,
        getApiKey: () => "model-private-credential",
        stream: (_model, context) => {
          calls++;
          assert.doesNotMatch(JSON.stringify(context), /host-secret|model-private-credential/);
          const results = context.messages.filter((message) => message.role === "toolResult");
          if (calls === 2) {
            assert.equal(results.length, 1);
            assert.match(JSON.stringify(results[0].content), /Frozen evidence/);
            assert.ok(!results[0].isError);
          }
          if (calls === 3) {
            assert.equal(results.length, 2);
            assert.equal(results[1].isError, true);
            assert.match(JSON.stringify(results[1].content), /permits only prepared task evidence/);
          }
          const message: AssistantMessage = {
            role: "assistant",
            api: "openai-completions",
            provider: "test",
            model: "test",
            content:
              calls < 3
                ? [
                    {
                      type: "toolCall",
                      id: `read-${calls}`,
                      name: "read",
                      arguments: { path: calls === 1 ? "/task/input.md" : hostFile },
                    },
                  ]
                : [{ type: "text", text: "Evidence reviewed; host access denied" }],
            stopReason: calls < 3 ? "toolUse" : "stop",
            timestamp: 0,
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
          stream.push({ type: "done", reason: calls < 3 ? "toolUse" : "stop", message });
          return stream;
        },
      }),
  });
  const options = { model: "test", directory: join(directory, "pi"), shardId: "sandbox-test" };
  const task = {
    instructions: "Review",
    input: [{ content: "Frozen evidence", sources: ["/source/chat"] }],
  };
  for (let attempt = 0; attempt < 2; attempt++) {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const agent = yield* makePiAgent(options);
          const handle = yield* agent.submit(task, { requestId: "sandbox-test" });
          assert.deepEqual(yield* agent.wait(handle), {
            state: "completed",
            result: { text: "Evidence reviewed; host access denied" },
          });
        }),
      ).pipe(Effect.provide(models), Effect.timeout("5 seconds")),
    );
    assert.equal(calls, 3);
  }
});
