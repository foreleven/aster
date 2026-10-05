import assert from "node:assert/strict";
import { test } from "node:test";
import { Effect, Option, Schema } from "effect";
import {
  TaskState,
  type TaskAdmissionReply,
  ExternalAgentError,
  makeApplicationApi,
} from "../src/index.js";
import { taskFixture, taskInput, retainedTask } from "./task-fixtures.js";
import { testConversations } from "./conversation-fixtures.js";
import { fakeAgent } from "./fixtures.js";

for (const result of ["found", "missing", "unsupported", "failed"] as const) {
  test(`Task reconciliation with ${result} submission never submits a replacement`, async () => {
    const conversations = testConversations();
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const record = yield* retainedTask(conversations);
          let lookups = 0;
          const env = yield* taskFixture({
            conversations,
            records: new Map([[record.path, record]]),
            agent: fakeAgent({
              submit: () => Effect.die("Recovery must not submit"),
              resume: () => Effect.die("Completed work must not resume"),
              ...(result === "unsupported"
                ? {}
                : {
                    lookupSubmission: (task, submission) =>
                      Effect.gen(function* () {
                        lookups++;
                        assert.equal(submission.requestId, record.path);
                        assert.match(task.instructions, /Test policy/);
                        if (result === "failed")
                          return yield* new ExternalAgentError({
                            operation: "lookup",
                            message: "Unavailable",
                          });
                        return result === "found"
                          ? Option.some({ sessionId: "original" })
                          : Option.none();
                      }),
                  }),
              status: () =>
                Effect.succeed({ state: "completed", result: { text: "Existing result" } }),
            }),
          });
          const response = yield* env.tasks.ask<TaskAdmissionReply>((replyTo) => ({
            _tag: "ResumeTask",
            input: {
              requestId: "reconcile",
              target: record.path,
              expectedRevision: env.registry.get(record.path)!.revision!,
            },
            replyTo,
          }));
          assert.equal(response._tag, "Accepted");
          const state = () =>
            Schema.decodeUnknownSync(TaskState)(env.registry.get(record.path)!.state);
          yield* env.wait(
            () => state().resumptions?.[0]?.status === (result === "found" ? "done" : "unknown"),
          );
          if (result === "found") {
            yield* env.wait(() => state().status === "completed");
            assert.equal(state().session?.sessionId, "original");
          } else assert.equal(state().status, "uncertain");
          assert.equal(lookups, result === "unsupported" ? 0 : 1);
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
  });
}

test("Task inspection excludes provider metadata and performs no execution", async () => {
  const conversations = testConversations();
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const original = yield* retainedTask(conversations, "completed");
        const record = {
          ...original,
          state: {
            ...original.state,
            session: { sessionId: "id", metadata: { credential: "private-token" } },
          },
        };
        const env = yield* taskFixture({
          conversations,
          records: new Map([[record.path, record]]),
          agents: {},
        });
        const api = makeApplicationApi({
          registry: env.registry,
          conversations,
          inspect: Effect.succeed(null),
        });
        const view = yield* api.inspectTask(record.path);
        assert.equal(view.instructions, taskInput().task.instructions);
        assert.equal(view.result, "Original result");
        assert.doesNotMatch(JSON.stringify(view), /private-token|metadata|sessionId/);
        assert.equal((yield* api.inspectTask("/personal").pipe(Effect.flip)).kind, "invalid-input");
        assert.equal(
          (yield* api.inspectTask(`/tasks/${"0".repeat(64)}`).pipe(Effect.flip)).kind,
          "not-found",
        );
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});
