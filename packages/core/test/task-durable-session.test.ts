import assert from "node:assert/strict";
import { test } from "node:test";
import { AgentConversations, DurableHarness } from "@aster/agent/harness";
import { Models, type ResolvedModel } from "@aster/agent";
import { createAssistantMessageEventStream, type AssistantMessage } from "@earendil-works/pi-ai";
import { Deferred, Effect, Layer, Schema } from "effect";
import { ExecutionCheckpoint } from "../src/tasks/execution/checkpoint.js";
import { TaskSnapshot } from "../src/index.js";
import { taskFixture, taskInput, retainedTask } from "./task-fixtures.js";
import { testConversations } from "./conversation-fixtures.js";

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
const answer = (): AssistantMessage => ({
  role: "assistant",
  provider: "test",
  model: "test",
  api: "openai-completions",
  timestamp: 0,
  stopReason: "stop",
  content: [{ type: "text", text: "Regional report" }],
  usage: {
    input: 0,
    output: 0,
    cacheRead: 0,
    cacheWrite: 0,
    totalTokens: 0,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
  },
});
const harnessFor = (
  conversations: AgentConversations["Service"],
  stream: ResolvedModel["stream"],
) =>
  DurableHarness.pipe(
    Effect.provide(
      DurableHarness.layer.pipe(
        Layer.provide([
          Layer.succeed(AgentConversations, conversations),
          Layer.succeed(Models, {
            resolve: () => Effect.succeed({ model, stream, getApiKey: () => "unused" }),
          }),
        ]),
      ),
    ),
  );

test("Task owns native steering coverage and commits one result for the accepted round", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const delivered = yield* Deferred.make<void>();
        const storage = yield* AgentConversations.makeMemory();
        const conversations = AgentConversations.of({
          ...storage,
          append: (...args) =>
            storage
              .append(...args)
              .pipe(
                Effect.tap((entry) =>
                  entry.kind === "task.execution" &&
                  Schema.decodeUnknownSync(ExecutionCheckpoint)(entry.data).deliveries.some(
                    (item) => item.requestId === "follow" && item.status === "accepted",
                  )
                    ? Deferred.succeed(delivered, undefined)
                    : Effect.void,
                ),
              ),
        });
        const started =
          yield* Deferred.make<ReturnType<typeof createAssistantMessageEventStream>>();
        let calls = 0;
        const harness = yield* harnessFor(conversations, (_model, context) => {
          const stream = createAssistantMessageEventStream();
          if (++calls === 1) Effect.runSync(Deferred.succeed(started, stream));
          else {
            assert.match(JSON.stringify(context.messages), /Include regional analysis/);
            stream.push({ type: "done", reason: "stop", message: answer() });
          }
          return stream;
        });
        const env = yield* taskFixture({ harness, conversations });
        const input = { ...taskInput(), agent: "internal" };
        yield* env.tasks.ask((replyTo) => ({ _tag: "StartTask", input, replyTo }));
        const stream = yield* Deferred.await(started);
        yield* env.tasks.ask((replyTo) => ({
          _tag: "Input",
          input: {
            requestId: "follow",
            target: input.target,
            source: input.replyTo,
            text: "Include regional analysis",
          },
          replyTo,
        }));
        yield* Deferred.await(delivered);
        stream.push({
          type: "done",
          reason: "toolUse",
          message: {
            ...answer(),
            stopReason: "toolUse",
            content: [{ type: "toolCall", id: "contexts", name: "list_contexts", arguments: {} }],
          },
        });
        const state = () =>
          Schema.decodeUnknownSync(TaskSnapshot)(env.registry.get(input.target)!.state);
        yield* env.wait(() => state().status === "completed");
        assert.equal(calls, 2);
        assert.ok(state().inputs.every((input) => input.status === "completed"));
        const results = (yield* conversations.read(input.target)).filter(
          (entry) => entry.kind === "task.result",
        );
        assert.equal(results.length, 1);
        assert.deepEqual(results[0]!.data, {
          roundId: input.requestId,
          status: "completed",
          text: "Regional report",
          covered: ["follow", input.requestId],
        });
      }),
    ).pipe(Effect.timeout("8 seconds")),
  );
});

test("Task restart finishes the native-answer to business-result handoff without another generation", async () => {
  const conversations = testConversations();
  let calls = 0;
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const input = { ...taskInput(), agent: "internal" };
        const record = yield* retainedTask(conversations, "running", input);
        const harness = yield* harnessFor(conversations, () => {
          calls++;
          const stream = createAssistantMessageEventStream();
          stream.push({ type: "done", reason: "stop", message: answer() });
          return stream;
        });
        yield* harness.withConversation(
          {
            name: "test",
            owner: input.target,
            instructions: "Answer",
            extensionName: `aster-task-tools:${input.target.split("/").at(-1)!}`,
          },
          (conversation) =>
            conversation
              .submit({ requestId: input.requestId, content: "Read evidence" })
              .pipe(Effect.flatMap((submission) => submission.wait)),
        );
        const env = yield* taskFixture({
          conversations,
          harness,
          records: new Map([[input.target, record]]),
        });
        const state = () =>
          Schema.decodeUnknownSync(TaskSnapshot)(env.registry.get(input.target)!.state);
        yield* env.wait(() => state().status === "completed");
        assert.equal(calls, 1);
        assert.ok(state().inputs.every((input) => input.status === "completed"));
        const results = (yield* conversations.read(input.target)).filter(
          (entry) => entry.kind === "task.result",
        );
        assert.equal(results.length, 1);
        assert.match(JSON.stringify(results[0]!.data), /Regional report/);
      }),
    ).pipe(Effect.timeout("8 seconds")),
  );
});

test("Task Check reads a failed native receipt and Retry waits only for its new submission", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const conversations = yield* AgentConversations.makeMemory();
        let calls = 0;
        const harness = yield* harnessFor(conversations, () => {
          const stream = createAssistantMessageEventStream();
          if (++calls === 2)
            stream.push({
              type: "error",
              reason: "error",
              error: {
                ...answer(),
                stopReason: "error",
                errorMessage: "Known request failure",
              },
            });
          else stream.push({ type: "done", reason: "stop", message: answer() });
          return stream;
        });
        const env = yield* taskFixture({ conversations, harness });
        const input = { ...taskInput(), agent: "internal" };
        const state = () =>
          Schema.decodeUnknownSync(TaskSnapshot)(env.registry.get(input.target)!.state);
        yield* env.tasks.ask((replyTo) => ({ _tag: "StartTask", input, replyTo }));
        yield* env.wait(() => state().status === "completed");
        yield* env.tasks.ask((replyTo) => ({
          _tag: "Input",
          input: {
            requestId: "follow",
            target: input.target,
            source: input.replyTo,
            text: "Analyze new evidence",
          },
          replyTo,
        }));
        yield* env.wait(() => state().status === "failed");
        yield* env.tasks.ask((replyTo) => ({
          _tag: "CheckTask",
          input: {
            requestId: "check",
            target: input.target,
            expectedRevision: env.registry.get(input.target)!.revision!,
          },
          replyTo,
        }));
        yield* env.wait(
          () => state().status === "failed" && state().inputs.at(-1)?.status === "completed",
        );
        assert.equal(calls, 2);
        yield* env.tasks.ask((replyTo) => ({
          _tag: "RetryTask",
          input: {
            requestId: "retry",
            target: input.target,
            expectedRevision: env.registry.get(input.target)!.revision!,
          },
          replyTo,
        }));
        yield* env.wait(() => state().status === "completed");
        assert.equal(calls, 3);
        assert.ok(state().inputs.every((input) => input.status === "completed"));
      }),
    ).pipe(Effect.timeout("8 seconds")),
  );
});
