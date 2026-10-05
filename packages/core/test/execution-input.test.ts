import assert from "node:assert/strict";
import { test, type TestContext } from "node:test";
import { AgentRunner, Agent, AgentError, Models, type ToolResultMessage } from "@aster/agent";
import { Cause, Clock, ConfigProvider, Deferred, Effect, Exit, Fiber, Layer } from "effect";
import { TestClock } from "effect/testing";
import {
  DEFAULT_EXECUTOR_PROMPT,
  ExternalAgents,
  makeStructuredReasoning,
  makeTaskExecution,
  SystemOneClient,
  MemoryRecall,
  MemoryRecallError,
  TaskPreparationError,
} from "../src/index.js";
import { fakeAgent } from "./fixtures.js";

import { makeExecutionInputBuilder } from "../src/tasks/build-execution-input.js";

const makeExecution = Effect.fnUntraced(function* (
  name: string,
  memory: MemoryRecall["Service"],
  executorPrompt: (name: string) => string | undefined = () => undefined,
) {
  const run = yield* makeStructuredReasoning(name, memory);
  return makeExecutionInputBuilder({ memory, run, executorPrompt });
});

const definition = {
  slug: "review",
  when: "changed",
  task: "Review the current evidence",
  agent: "test",
  mode: "confirm",
} as const;
const source = { path: "/goals/foo", description: "Current Goal", state: {}, messages: [] };
const task = { instructions: "Summarize the evidence", input: [] };
const models = Layer.succeed(Models, {
  resolve: () => Effect.die(new Error("Agent.make is mocked")),
});
const mockReasoning = (
  t: TestContext,
  run: (
    input: Parameters<Agent["run"]>[0],
    options: Parameters<typeof Agent.make>[0],
  ) => Effect.Effect<ToolResultMessage["details"], AgentError>,
) =>
  t.mock.method(Agent, "make", (options: Parameters<typeof Agent.make>[0]) =>
    Effect.succeed({
      run: (input) =>
        run(input, options).pipe(
          Effect.map((details) => ({
            messages: [
              {
                role: "toolResult" as const,
                toolCallId: "result",
                toolName: "submit_result",
                content: [],
                isError: false,
                timestamp: 0,
                details,
              },
            ],
          })),
        ),
    } satisfies Agent),
  );

test("execution input expands bounded recall before reasoning and uses the admitted source and caller Clock", async (t) => {
  const calls: string[] = [];
  const candidates = Array.from({ length: 10 }, (_, index) => ({
    obsId: `observation-${index}`,
    sessionId: "session",
    title: "Compact candidate",
  }));
  const refs = candidates.slice(0, 8).map(({ obsId, sessionId }) => ({ obsId, sessionId }));
  const expanded = { results: [{ text: "Previously verified evidence" }] };
  const stale = { ...source, state: { stale: true } };
  const related = { ...source, path: `${source.path}/runs/current` };
  const neighbour = { ...source, path: "/goals/foo-bar", state: { unrelated: true } };
  const linked = {
    ...source,
    path: "/signals/review/runs/linked",
    state: { sourcePath: source.path },
  };
  mockReasoning(t, ({ messages }, options) =>
    Effect.sync(() => {
      calls.push("reason");
      assert.deepEqual(calls, ["search", "expand", "reason"]);
      assert.ok(messages.every((message) => message.timestamp === 12345));
      const message = messages.find((message) => message.role === "user");
      assert.ok(message && Array.isArray(message.content));
      const text = message.content.find((item) => item.type === "text");
      assert.ok(text?.type === "text");
      const evidence = JSON.parse(
        text.text.split("\n").find((line) => line.startsWith('{"signal":'))!,
      );
      assert.deepEqual(evidence.recalled, expanded);
      assert.deepEqual(
        evidence.priorWork,
        [source, related, linked].map(({ path, description, state }) => ({
          path,
          description,
          state,
        })),
      );
      assert.ok(evidence.otherWork.some((item: { path: string }) => item.path === neighbour.path));
      assert.doesNotMatch(text.text, /Executor-specific policy/);
      const resultTool = options.tools!.find((tool) => tool.name === "submit_result")!;
      assert.ok("additionalProperties" in resultTool.parameters);
      assert.equal(resultTool.parameters.additionalProperties, false);
      return task;
    }),
  );
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const clock = yield* TestClock.make();
        yield* clock.adjust(12345);
        const internal = yield* makeExecution(
          "test",
          {
            search: () =>
              Effect.gen(function* () {
                calls.push("search");
                assert.equal(yield* Clock.currentTimeMillis, 12345);
                return { results: candidates };
              }),
            expand: (actual) =>
              Effect.sync(() => {
                calls.push("expand");
                assert.deepEqual(actual, refs);
                return expanded;
              }),
          },
          (executor) => (executor === "custom-executor" ? "Executor-specific policy" : ""),
        ).pipe(Effect.provide(AgentRunner.layer.pipe(Layer.provide(models))));
        const input = internal(
          definition,
          source,
          Object.fromEntries(
            [stale, related, neighbour, linked].map((record) => [record.path, record]),
          ),
        );
        assert.deepEqual(calls, []);
        const result = yield* input.pipe(Effect.provideService(Clock.Clock, clock));
        assert.equal(result.instructions, task.instructions);
        assert.deepEqual(
          result.input.map((item) => item.sources),
          [[source.path], refs.map((ref) => `memory:${ref.obsId}`)],
        );
        assert.match(result.input[1]!.content, /Previously verified evidence/);
      }),
    ),
  );
});

test("malformed memory candidates fail in the typed channel before model invocation", async (t) => {
  const model = t.mock.method(Agent, "make", () => Effect.die("Unexpected model invocation"));
  for (const recalled of [
    null,
    {},
    { results: null },
    { results: [null] },
    { results: [{ obsId: 1 }] },
    { results: [{ obsId: "id", sessionId: 1 }] },
  ]) {
    await Effect.runPromise(
      Effect.gen(function* () {
        const internal = yield* makeExecution("test", {
          search: () => Effect.succeed(recalled),
          expand: () => Effect.die("Unexpected expansion"),
        }).pipe(Effect.provide(AgentRunner.layer.pipe(Layer.provide(models))));
        const error = yield* internal(definition, source, {}).pipe(Effect.flip);
        assert.equal(error._tag, "TaskPreparationError");
        assert.equal(error.operation, "prepare");
        assert.ok(error.cause instanceof Error);
        assert.equal(error.cause.name, "SchemaError");
      }),
    );
  }
  assert.equal(model.mock.callCount(), 0);
});

test("execution input rejects malformed model output and retains executor policy only for its executor", async (t) => {
  let output: ToolResultMessage["details"] = task;
  mockReasoning(t, () => Effect.sync(() => output));
  await Effect.runPromise(
    Effect.gen(function* () {
      const internal = yield* makeExecution(
        "test",
        {
          search: () => Effect.succeed({ results: [] }),
          expand: () => Effect.die("Empty recall must not expand"),
        },
        (executor) => (executor === "custom-executor" ? "Executor-specific policy" : ""),
      ).pipe(Effect.provide(AgentRunner.layer.pipe(Layer.provide(models))));
      const result = yield* internal({ ...definition, agent: "custom-executor" }, source, {});
      assert.equal(result.instructions, `Executor-specific policy\n\n${task.instructions}`);
      assert.equal(result.input.length, 1);
      for (output of [null, {}, { ...task, instructions: "  \n " }, { ...task, input: [{}] }]) {
        const error = yield* internal(definition, source, {}).pipe(Effect.flip);
        assert.equal(error._tag, "TaskPreparationError");
        assert.ok(error.cause instanceof Error);
        assert.equal(error.cause.name, "SchemaError");
      }
    }),
  );
});

test("execution input uses its built-in prompt when no executor override is provided", async (t) => {
  mockReasoning(t, ({ messages }) =>
    Effect.sync(() => {
      const message = messages.find((message) => message.role === "user");
      assert.ok(message && Array.isArray(message.content));
      assert.ok(
        message.content.some(
          (item) => item.type === "text" && item.text.includes(DEFAULT_EXECUTOR_PROMPT),
        ),
      );
      return task;
    }),
  );
  await Effect.runPromise(
    Effect.gen(function* () {
      const internal = yield* makeExecution("test", {
        search: () => Effect.succeed({ results: [] }),
        expand: () => Effect.die("Empty recall must not expand"),
      }).pipe(Effect.provide(AgentRunner.layer.pipe(Layer.provide(models))));
      const result = yield* internal(definition, source, {});
      assert.equal(result.instructions, `${DEFAULT_EXECUTOR_PROMPT}\n\n${task.instructions}`);
    }),
  );
});

test("Task execution reads the selected executor's prompt and falls back to built-in instructions", async (t) => {
  mockReasoning(t, ({ messages }) =>
    Effect.sync(() => {
      assert.doesNotMatch(JSON.stringify(messages), /Unrelated adapter configuration/);
      return task;
    }),
  );
  const layer = Layer.mergeAll(
    Layer.succeed(SystemOneClient, { systemOne: () => Effect.die("No readiness expected") }),
    AgentRunner.layer,
  ).pipe(
    Layer.provideMerge(
      Layer.mergeAll(
        Models.layer([
          {
            name: "test",
            provider: "openai",
            model: "test",
            apiKey: "test",
            url: "http://unused.invalid",
          },
        ]),
        Layer.succeed(MemoryRecall, {
          search: () => Effect.succeed({ results: [] }),
          expand: () => Effect.die("Empty recall must not expand"),
        }),
        Layer.succeed(ExternalAgents, {
          custom: fakeAgent({ executorPrompt: "Custom executor instructions" }),
          plain: fakeAgent(),
        }),
      ),
    ),
    Layer.provideMerge(
      ConfigProvider.layer(
        ConfigProvider.fromUnknown({
          config: { agent: { model: "test" } },
          agents: { doubao: { prompt: "Unrelated adapter configuration" } },
        }),
      ),
    ),
  );
  await Effect.runPromise(
    Effect.gen(function* () {
      const internal = (yield* makeTaskExecution()).buildExecutionInput;
      const custom = yield* internal({ ...definition, agent: "custom" }, source, {});
      assert.equal(custom.instructions, `Custom executor instructions\n\n${task.instructions}`);
      const plain = yield* internal({ ...definition, agent: "plain" }, source, {});
      assert.equal(plain.instructions, `${DEFAULT_EXECUTOR_PROMPT}\n\n${task.instructions}`);
    }).pipe(Effect.provide(layer)),
  );
});

test("execution input retains recall errors and defects without invoking reasoning", async (t) => {
  const model = t.mock.method(Agent, "make", () => Effect.die("Unexpected model invocation"));
  const error = new MemoryRecallError({ message: "Recall unavailable" });
  const defect = new Error("Recall invariant failed");
  for (const { recall, isDefect } of [
    { recall: Effect.fail(error), isDefect: false },
    { recall: Effect.die(defect), isDefect: true },
  ]) {
    await Effect.runPromise(
      Effect.gen(function* () {
        const internal = yield* makeExecution("test", {
          search: () => recall,
          expand: () => Effect.die("Unexpected expansion"),
        }).pipe(Effect.provide(AgentRunner.layer.pipe(Layer.provide(models))));
        const exit = yield* Effect.exit(internal(definition, source, {}));
        assert.ok(Exit.isFailure(exit));
        const failure = Cause.squash(exit.cause);
        if (isDefect) assert.equal(failure, defect);
        else {
          assert.ok(failure instanceof TaskPreparationError);
          assert.equal(failure.cause, error);
        }
      }),
    );
  }
  assert.equal(model.mock.callCount(), 0);
});

test("cancelling mandatory recall or expansion releases its scope before reasoning", async (t) => {
  const model = t.mock.method(Agent, "make", () => Effect.die("Unexpected model invocation"));
  for (const stage of ["search", "expand"]) {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const entered = yield* Deferred.make<void>();
          const released = yield* Deferred.make<void>();
          const blocked = Deferred.succeed(entered, undefined).pipe(
            Effect.andThen(Effect.never),
            Effect.ensuring(Deferred.succeed(released, undefined)),
          );
          const internal = yield* makeExecution("test", {
            search: () =>
              stage === "search" ? blocked : Effect.succeed({ results: [{ obsId: "id" }] }),
            expand: () => blocked,
          }).pipe(Effect.provide(AgentRunner.layer.pipe(Layer.provide(models))));
          const fiber = yield* internal(definition, source, {}).pipe(Effect.forkScoped);
          yield* Deferred.await(entered);
          yield* Fiber.interrupt(fiber);
          assert.equal(yield* Deferred.isDone(released), true);
        }),
      ).pipe(Effect.timeout("2 seconds")),
    );
  }
  assert.equal(model.mock.callCount(), 0);
});
