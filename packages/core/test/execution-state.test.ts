import assert from "node:assert/strict";
import { test } from "node:test";
import { Effect, Fiber, Schema, Stream } from "effect";
import { TaskState, TaskActor, contextSpawnOptions } from "../src/index.js";
import { taskFixture, taskInput, retainedTask } from "./task-fixtures.js";
import { testConversations } from "./conversation-fixtures.js";

test("Task state requires durable admission and input references", () => {
  const decode = Schema.decodeUnknownSync(TaskState);
  assert.throws(() => decode({ status: "completed" }));
  assert.throws(() => decode({ admission: { input: taskInput() }, status: "checking" }));
});

for (const status of ["completed", "failed", "cancelled", "uncertain"] as const) {
  test(`retained ${status} Task replays its committed result without an executor`, async () => {
    const conversations = testConversations();
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const record = yield* retainedTask(conversations, status);
          const env = yield* taskFixture({
            conversations,
            records: new Map([[record.path, record]]),
            agents: {},
          });
          // The readiness ask runs after restoration, while feedback is acknowledged asynchronously.
          for (let n = 0; !env.feedback.length && n < 100; n++) yield* Effect.yieldNow;
          assert.equal(env.feedback.length, 1);
          const feedback = env.feedback[0]!;
          assert.ok(feedback._tag === "SubmitInput" && feedback.input._tag === "ExecutionFeedback");
          if (feedback._tag === "SubmitInput" && feedback.input._tag === "ExecutionFeedback") {
            assert.equal(feedback.input.text, "Original result");
            assert.equal(feedback.input.status, status);
          }
          assert.deepEqual(env.registry.get(record.path), record);
          assert.equal(
            (yield* conversations.read(record.path)).filter((entry) => entry.kind === "task.result")
              .length,
            1,
          );
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
  });
}

test("malformed restored Task fails closed before execution or recovery writes", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const record = {
          path: "/broken",
          description: "Corrupt Task",
          messages: [],
          state: { status: "checking" },
        };
        const env = yield* taskFixture({ records: new Map([[record.path, record]]) });
        const before = env.registry.get(record.path);
        const stopped = yield* env.system.events.pipe(
          Stream.filter((event) => event._tag === "ActorStopped" && event.path === "/user/broken"),
          Stream.runHead,
          Effect.forkScoped,
        );
        yield* Effect.yieldNow;
        yield* env.system.spawn(
          "broken",
          TaskActor,
          contextSpawnOptions(record.path, { supervision: () => "stop" }),
        );
        assert.equal((yield* Fiber.join(stopped))._tag, "Some");
        assert.deepEqual(env.registry.get(record.path), before);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});
