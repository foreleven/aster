import type { EffectCallbacks } from "../src/shared/effect-tools.js";
import { DurableHarness, AgentConversations } from "../src/harness/index.js";
import assert from "node:assert/strict";
import { test } from "node:test";
import { Cause, Clock, Context, Deferred, Effect, Exit, Fiber, Layer, Option } from "effect";
import { TestClock } from "effect/testing";
import { createAssistantMessageEventStream } from "@earendil-works/pi-ai";
import {
  AgentError,
  Models,
  Type,
  type EffectTool,
  type AgentResult,
  type AssistantMessage,
} from "../src/index.js";
import { AgentRunner, type AgentInvocation } from "../src/agent/index.js";

class Caller extends Context.Service<Caller, { readonly name: string }>()("test/Caller") {}
const parameters = Type.Object({});
const response: AssistantMessage = {
  role: "assistant",
  content: [{ type: "text", text: "Done" }],
  timestamp: 0,
  api: "openai-completions",
  provider: "test",
  model: "test",
  stopReason: "stop",
  usage: {
    input: 0,
    output: 0,
    cacheRead: 0,
    cacheWrite: 0,
    totalTokens: 0,
    cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
  },
};
const readCaller: EffectTool<typeof parameters, never, Caller> = {
  name: "read",
  label: "Read",
  description: "Read injected caller",
  parameters,
  replay: "safe",
  execute: () =>
    Effect.gen(function* () {
      const caller = yield* Caller;
      const now = yield* Clock.currentTimeMillis;
      return { content: [{ type: "text", text: `${caller.name}:${now}` }], details: caller.name };
    }),
};

test("explicit agent and harness services isolate histories and retain native submissions", async () => {
  const requests: unknown[][] = [];
  const models = Models.of({
    resolve: () =>
      Effect.succeed({
        model: {
          id: "test",
          name: "test",
          provider: "test",
          api: "openai-completions",
          baseUrl: "http://unused",
          reasoning: false,
          input: ["text"],
          contextWindow: 1000,
          maxTokens: 100,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
        },
        getApiKey: () => "unused",
        stream: (_model, context) => {
          requests.push(
            context.messages
              .filter((message) => message.role === "user")
              .map((message) => message.content),
          );
          const stream = createAssistantMessageEventStream();
          stream.push({ type: "done", reason: "stop", message: response });
          return stream;
        },
      }),
  });
  await Effect.runPromise(
    Effect.gen(function* () {
      const runner = yield* AgentRunner;
      for (const content of ["isolated one", "isolated two"])
        yield* runner.run({ name: "test", messages: [{ role: "user", content, timestamp: 0 }] });
      const harness = yield* DurableHarness;
      const options = { name: "test", owner: "/test/runner", instructions: "Answer" };
      const run = (requestId: string, content: string) =>
        harness.withConversation(options, (conversation) =>
          conversation
            .submit({ requestId, content })
            .pipe(Effect.flatMap((submission) => submission.wait)),
        );
      const first = yield* run("one", "durable one");
      assert.deepEqual(yield* run("one", "durable one"), first);
      assert.equal(requests.length, 3);
      yield* run("two", "durable two");
    }).pipe(
      Effect.provide([
        AgentRunner.layer,
        DurableHarness.layer.pipe(Layer.provide(AgentConversations.memory)),
      ]),
      Effect.provideService(Models, models),
    ),
  );
  assert.deepEqual(requests, [
    ["isolated one"],
    ["isolated two"],
    ["durable one"],
    ["durable one", "durable two"],
  ]);
});

type NativeCallbacks = Pick<AgentInvocation, "tools" | "onResponse">;
type Execute = (request: NativeCallbacks) => Effect.Effect<AgentResult, AgentError>;
const boundaries = [
  {
    label: "agent",
    make: (execute: Execute) => {
      const runner = AgentRunner.make(execute);
      return {
        run: <E, R>(callbacks: EffectCallbacks<E, R>) =>
          runner.run({ name: "test", messages: [], ...callbacks }).pipe(Effect.asVoid),
      };
    },
  },
  {
    label: "harness",
    make: (execute: Execute) => {
      const harness = DurableHarness.make((options) =>
        Effect.succeed({
          submit: () =>
            Effect.succeed({
              status: Effect.die("Unused fake status"),
              wait: execute(options).pipe(Effect.as(undefined)),
            }),
          submission: () => Effect.succeed(Option.none()),
          abort: Effect.void,
        }),
      );
      return {
        run: <E, R>(callbacks: EffectCallbacks<E, R>) =>
          harness
            .withConversation(
              { name: "test", owner: "/test/callbacks", instructions: "Answer", ...callbacks },
              (conversation) =>
                conversation
                  .submit({ requestId: "one", content: "Evidence" })
                  .pipe(Effect.flatMap((submission) => submission.wait)),
            )
            .pipe(Effect.asVoid),
      };
    },
  },
];
for (const { label, make } of boundaries) {
  test(`${label}: runner captures each invocation's injected services for tools and response observers`, async () => {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const clock = yield* TestClock.make();
          yield* clock.adjust(1234);
          const seen: string[] = [];
          const record = (event: string) =>
            Caller.use((caller) =>
              Effect.sync(() => {
                seen.push(`${caller.name}:${event}`);
              }),
            );
          const runner = make((native) =>
            Effect.tryPromise({
              try: async (signal) => {
                const result = await native.tools![0]!.execute("read", {}, signal);
                const text = result.content[0];
                assert.ok(text?.type === "text");
                assert.equal(text.text, `${result.details}:1234`);
                await native.onResponse!(response, signal);
                return { messages: [] };
              },
              catch: (cause) => new AgentError("Fake SDK failed", [], { cause }),
            }),
          );
          const work = runner.run({
            tools: [readCaller],
            onResponse: () => record("response"),
          });
          // A missing dependency must remain visible in the runner's inferred Effect type.
          const dependencyIsPreserved: [Effect.Services<typeof work>] extends [Caller]
            ? [Caller] extends [Effect.Services<typeof work>]
              ? true
              : false
            : false = true;
          assert.equal(dependencyIsPreserved, true);
          yield* Effect.all(
            [
              work.pipe(Effect.provideService(Caller, { name: "one" })),
              work.pipe(Effect.provideService(Caller, { name: "two" })),
            ],
            { concurrency: "unbounded" },
          ).pipe(Effect.provideService(Clock.Clock, clock));
          assert.deepEqual(seen.sort(), ["one:response", "two:response"].sort());
        }),
      ),
    );
  });

  test(`${label}: SDK tool recovery cannot swallow Effect defects, and retired callbacks cannot run`, async () => {
    let native: NativeCallbacks | undefined;
    let calls = 0;
    const defect = new Error("Broken tool invariant");
    const runner = make((invocation) => {
      native = invocation;
      return Effect.promise(async () => {
        // A provider may turn a rejected Promise into an ordinary tool-error response.
        await invocation.tools![0]!.execute("read", {}).catch(() => undefined);
        return { messages: [] };
      });
    });
    const exit = await Effect.runPromise(
      runner
        .run({
          tools: [
            {
              ...readCaller,
              execute: () =>
                Effect.sync(() => {
                  calls++;
                }).pipe(Effect.andThen(Effect.die(defect))),
            },
          ],
        })
        .pipe(Effect.exit),
    );
    assert.ok(Exit.isFailure(exit));
    assert.equal(Cause.squash(exit.cause), defect);
    await assert.rejects(async () => native!.tools![0]!.execute("late", {}));
    assert.equal(calls, 1);
  });

  test(`${label}: runner interruption releases a dependency-using tool before SDK idle cleanup`, async () => {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const entered = yield* Deferred.make<void>();
          const released = yield* Deferred.make<void>();
          const idle = yield* Deferred.make<void>();
          const runner = make((native) =>
            Effect.acquireUseRelease(
              Effect.sync(() => {
                const work = native.tools![0]!.execute("read", {});
                return {
                  settled: work.then(
                    () => undefined,
                    () => undefined,
                  ),
                };
              }),
              ({ settled }) => Effect.promise(() => settled).pipe(Effect.as({ messages: [] })),
              ({ settled }) =>
                Effect.promise(() => settled).pipe(
                  Effect.andThen(Deferred.succeed(idle, undefined)),
                ),
            ),
          );
          const worker = yield* runner
            .run({
              tools: [
                {
                  ...readCaller,
                  execute: () =>
                    Caller.pipe(
                      Effect.andThen(Deferred.succeed(entered, undefined)),
                      Effect.andThen(Effect.never),
                      Effect.ensuring(Deferred.succeed(released, undefined)),
                    ),
                },
              ],
            })
            .pipe(Effect.provideService(Caller, { name: "one" }), Effect.forkScoped);
          yield* Deferred.await(entered);
          yield* Fiber.interrupt(worker);
          assert.equal(yield* Deferred.isDone(released), true);
          assert.equal(yield* Deferred.isDone(idle), true);
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
  });
}
