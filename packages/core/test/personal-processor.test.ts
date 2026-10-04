import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { AgentRunner, Models } from "@aster/agent";
import { ConfigProvider, Effect, Layer } from "effect";
import { makePersonalReasoner, type PersonalReadPort } from "../src/index.js";
import { createAssistantMessageEventStream } from "@earendil-works/pi-ai";

test("Personal processor uses controlled reads and replays a durable structured reply without another model call", async () => {
  const directory = await mkdtemp(join(tmpdir(), "aster-personal-processor-"));
  const reply = {
    text: "The release is ready.",
    approvalRequests: [
      {
        contextPath: "/signals/release/runs/one",
        contextRevision: 2,
        approvalsRevision: 1,
        approvalId: "/signals/release/runs/one:confirm",
      },
    ],
    tasks: [
      {
        agent: "test",
        task: {
          instructions: "Summarize release blockers",
          input: [{ content: "The release is ready.", sources: ["/project"] }],
        },
      },
    ],
    signalCommands: [
      {
        operation: "createSignal" as const,
        signalSlug: "personal--release",
        signalRevision: 0,
        active: true,
        definition: { when: "Release blockers change", task: "Inspect blockers", agent: "test" },
      },
    ],
  };
  let requests = 0;
  let reads = 0;
  let inspections = 0;
  const model = {
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
  };
  const models = Layer.succeed(Models, {
    resolve: () =>
      Effect.succeed({
        model,
        getApiKey: () => "test",
        stream: () => {
          const stream = createAssistantMessageEventStream();
          const call = [
            { name: "read_context", arguments: { path: "/project" } },
            { name: "inspect_delegation", arguments: { path: "/delegations/work" } },
            { name: "submit_reply", arguments: reply },
          ][requests++]!;
          stream.push({
            type: "done",
            reason: "toolUse",
            message: {
              role: "assistant",
              api: "openai-completions",
              provider: "test",
              model: "test",
              timestamp: 0,
              stopReason: "toolUse",
              content: [
                {
                  type: "toolCall",
                  id: `call-${requests}`,
                  name: call.name,
                  arguments: call.arguments,
                },
              ],
              usage: {
                input: 1,
                output: 1,
                cacheRead: 0,
                cacheWrite: 0,
                totalTokens: 2,
                cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
              },
            },
          });
          return stream;
        },
      }),
  });
  const readPort: PersonalReadPort = {
    inspectDelegation: (path) =>
      Effect.sync(() => {
        inspections++;
        assert.equal(path, "/delegations/work");
        return {
          path,
          revision: 3,
          runPath: "/signals/release/runs/one",
          agent: "test",
          status: "completed",
          instructions: "Review",
          sources: [],
          hasExecution: true,
          requests: [],
          result: "Ready",
        };
      }),
    executors: Effect.succeed(["test"]),
    list: Effect.succeed([]),
    read: (path) =>
      Effect.sync(() => {
        reads++;
        assert.equal(path, "/project");
        return { path, description: "Project", revision: 2, state: { ready: true }, messages: [] };
      }),
  };
  try {
    for (let restart = 0; restart < 2; restart++) {
      const layer = AgentRunner.layer.pipe(
        Layer.provide(models),
        Layer.provideMerge(
          ConfigProvider.layer(
            ConfigProvider.fromUnknown({
              config: { personal: { model: "test", storageDirectory: directory } },
            }),
          ),
        ),
      );
      const text = await Effect.runPromise(
        Effect.gen(function* () {
          const processor = yield* makePersonalReasoner();
          return yield* processor.run(
            {
              requestId: "one",
              causationId: "user-1",
              source: "user",
              target: "/personal",
              revision: 2,
              sequence: 1,
              createdAt: "2026-10-02T00:00:00.000Z",
              payload: { _tag: "UserInput", text: "Is the release ready?" },
            },
            readPort,
          );
        }).pipe(Effect.provide(layer)),
      );
      assert.deepEqual(text, reply);
      assert.equal(requests, 3);
      assert.equal(inspections, 1);
      assert.equal(reads, 1);
    }
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});
