import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { Effect, Layer, Option } from "effect";
import { Models, type ResolvedModel } from "@aster/agent";
import { createAssistantMessageEventStream, type AssistantMessage } from "@earendil-works/pi-ai";
import { makePiAgent } from "../src/pi/agent.js";

test("Pi reconciles a lost submission handle by request identity without another submit and persists history before acknowledgement", async (t) => {
  const directory = mkdtempSync(join(tmpdir(), "aster-pi-delegation-"));
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  let calls = 0;
  let submissions = 0;
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
        getApiKey: () => "private-test-credential",
        stream: (_model, context) => {
          calls++;
          assert.match(JSON.stringify(context.messages), /Frozen evidence/);
          assert.match(JSON.stringify(context.messages), /source\/chat/);
          const stream = createAssistantMessageEventStream();
          const message: AssistantMessage = {
            role: "assistant",
            api: "openai-completions",
            provider: "test",
            model: "test",
            content: [{ type: "text", text: "Task conclusion [source/chat]" }],
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
          };
          stream.push({ type: "done", reason: "stop", message });
          return stream;
        },
      }),
  });
  const options = { model: "test", directory: join(directory, "pi"), shardId: "test-owner" };
  const task = {
    instructions: "Analyze the new facts",
    input: [{ content: "Frozen evidence", sources: ["source/chat"] }],
  };
  const session = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const agent = yield* makePiAgent(options);
        const submission = { requestId: "/tasks/test" };
        submissions++;
        yield* agent.submit(task, submission);
        // Simulate losing the returned handle: recovery only looks up its identity.
        const located = yield* agent.lookupSubmission!(task, submission);
        assert.ok(Option.isSome(located));
        if (Option.isNone(located)) return assert.fail("Expected retained admission");
        const session = located.value;
        assert.deepEqual(yield* agent.wait(session), {
          state: "completed",
          result: { text: "Task conclusion [source/chat]" },
        });
        assert.deepEqual(yield* agent.submit(task, submission), session);
        assert.doesNotMatch(JSON.stringify(session), /private-test-credential|pi\.tool-result/);
        return session;
      }),
    ).pipe(Effect.provide(models), Effect.timeout("5 seconds")),
  );
  assert.equal(calls, 1);
  assert.equal(submissions, 1);
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const agent = yield* makePiAgent(options);
        assert.deepEqual(yield* agent.status(session), {
          state: "completed",
          result: { text: "Task conclusion [source/chat]" },
        });
        yield* agent.resume(session);
        assert.equal((yield* agent.wait(session)).state, "completed");
        const mismatch = yield* agent
          .status({ ...session, metadata: { ...session.metadata, shardId: "different-owner" } })
          .pipe(Effect.flip);
        assert.equal(mismatch._tag, "ExternalAgentError");
      }),
    ).pipe(Effect.provide(models), Effect.timeout("5 seconds")),
  );
  assert.equal(calls, 1);
});
