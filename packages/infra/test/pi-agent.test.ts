import assert from "node:assert/strict";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { Effect, Layer, Schema } from "effect";
import { ActorSystem, ActorTestKit } from "@aster/actor";
import { Models, type ResolvedModel } from "@aster/agent";
import { createAssistantMessageEventStream, type AssistantMessage } from "@earendil-works/pi-ai";
import {
  ContextRegistry,
  DelegationActor,
  DelegationState,
  ExternalAgents,
  ExternalAgentError,
  contextSpawnOptions,
  type DelegationUpdate,
} from "@aster/core";
import { makeContextRegistry } from "@aster/core/testing";
import { makePiAgent } from "../src/pi/agent.js";
import { makeFileContextStore } from "../src/storage/file-context-store.js";

test("Pi reconciles a lost submission handle through Delegation without another submit and persists history before acknowledgement", async (t) => {
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
        const registry = yield* makeContextRegistry(
          makeFileContextStore(join(directory, "contexts")),
        );
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(ContextRegistry, registry),
            Layer.succeed(ExternalAgents, {
              pi: {
                ...agent,
                submit: (task, submission) =>
                  Effect.gen(function* () {
                    submissions++;
                    yield* agent.submit(task, submission);
                    return yield* new ExternalAgentError({
                      operation: "submit",
                      message: "Injected lost handle acknowledgement",
                    });
                  }),
              },
            }),
          ),
        );
        const parent = yield* ActorTestKit.probe<DelegationUpdate>();
        const actor = yield* system.spawn(
          "execution",
          DelegationActor,
          contextSpawnOptions("/delegations/test"),
        );
        yield* actor.tell({
          _tag: "Start",
          request: { runPath: "/runs/test", agent: "pi", task },
          replyTo: parent.ref,
        });
        const submitted = yield* parent.take();
        assert.equal(submitted._tag, "Submitted");
        const finished = yield* parent.take();
        assert.equal(finished._tag, "Finished");
        if (finished._tag !== "Finished") return assert.fail("Expected terminal update");
        assert.deepEqual(finished.outcome, {
          _tag: "Completed",
          text: "Task conclusion [source/chat]",
        });
        const record = registry.get("/delegations/test")!;
        const state = Schema.decodeUnknownSync(DelegationState)(record.state);
        assert.equal(state.status, "completed");
        assert.equal(state.result, "Task conclusion [source/chat]");
        assert.ok(state.session);
        assert.equal(state.session.metadata?.requestId, "/delegations/test");
        assert.doesNotMatch(
          JSON.stringify(record),
          /private-test-credential|pi\.tool-result|pi\.assistant/,
        );
        assert.deepEqual(
          yield* agent.submit(task, { requestId: "/delegations/test" }),
          state.session,
        );
        return state.session;
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
