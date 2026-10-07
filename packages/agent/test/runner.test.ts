import assert from "node:assert/strict";
import { test } from "node:test";
import { Cause, Clock, Context, Deferred, Effect, Exit, Fiber } from "effect";
import { TestClock } from "effect/testing";
import {
  AgentRunner,
  AgentError,
  Type,
  type AgentInvocation,
  type EffectTool,
  type AssistantMessage,
} from "../src/index.js";

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

test("runner captures each invocation's injected services for tools and response observers", async () => {
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
        const runner = AgentRunner.make((native) =>
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
          name: "test",
          messages: [],
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

test("SDK tool recovery cannot swallow Effect defects, and retired callbacks cannot run", async () => {
  let native: AgentInvocation | undefined;
  let calls = 0;
  const defect = new Error("Broken tool invariant");
  const runner = AgentRunner.make((invocation) => {
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
        name: "test",
        messages: [],
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

test("runner interruption releases a dependency-using tool before SDK idle cleanup", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const released = yield* Deferred.make<void>();
        const idle = yield* Deferred.make<void>();
        const runner = AgentRunner.make((native) =>
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
              Effect.promise(() => settled).pipe(Effect.andThen(Deferred.succeed(idle, undefined))),
          ),
        );
        const worker = yield* runner
          .run({
            name: "test",
            messages: [],
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
