import type { HarnessCall } from "./harness-fixtures.js";
import { makeHarness } from "./harness-fixtures.js";
import assert from "node:assert/strict";
import { test } from "node:test";
import { AgentError } from "@aster/agent";
import { Deferred, Effect, Schema } from "effect";
import {
  TaskSnapshot,
  ExternalAgentError,
  approvalEntries,
  type TaskAdmissionReply,
} from "../src/index.js";
import { taskFixture, taskInput, retainedTask, seedCheckpoint } from "./task-fixtures.js";
import { testConversations } from "./conversation-fixtures.js";
import { fakeAgent } from "./fixtures.js";

const run = <A, E>(effect: Effect.Effect<A, E, import("effect").Scope.Scope>) =>
  Effect.runPromise(Effect.scoped(effect).pipe(Effect.timeout("6 seconds")));

for (const mode of ["session", "rejected-submission"] as const) {
  test(`Check observes and only Retry restarts a known failed ${mode}`, async () => {
    await run(
      Effect.gen(function* () {
        const messages = testConversations();
        const record = yield* retainedTask(messages, "failed");
        yield* seedCheckpoint(messages, record.snapshot.path, {
          ...(mode === "session" ? { session: { sessionId: "original" } } : {}),
          deliveries: [
            {
              requestId: "task",
              roundId: "task",
              kind: "instruction",
              status: mode === "session" ? "accepted" : "rejected",
            },
          ],
        });
        let submits = 0,
          resumes = 0;
        const env = yield* taskFixture({
          conversations: messages,
          records: new Map([[record.snapshot.path, record]]),
          agent: fakeAgent({
            submit: (_task, identity) =>
              Effect.sync(() => {
                submits++;
                assert.equal(identity?.requestId, record.snapshot.path);
                return { sessionId: "new" };
              }),
            status: () =>
              Effect.succeed({ state: "failed", resumable: true, error: "Known failure" }),
            resume: (session) =>
              Effect.sync(() => {
                resumes++;
                assert.equal(session.sessionId, "original");
                return session;
              }),
          }),
        });
        const state = () =>
          Schema.decodeUnknownSync(TaskSnapshot)(env.registry.get(record.snapshot.path)!.state);
        const control = (tag: "CheckTask" | "RetryTask", requestId: string) => {
          const input = {
            requestId,
            target: record.snapshot.path,
            expectedRevision: env.registry.get(record.snapshot.path)!.revision!,
          };
          return env.tasks.ask<TaskAdmissionReply>((replyTo) => ({ _tag: tag, input, replyTo }));
        };
        {
          yield* control("CheckTask", "check");
          yield* env.wait(() => state().status === "failed" && state().inputs.length === 2);
          assert.equal(resumes, 0);
          assert.equal(submits, 0);
        }
        assert.equal((yield* control("RetryTask", "retry"))._tag, "Accepted");
        yield* env.wait(() => state().status === "completed");
        assert.equal(resumes, mode === "session" ? 1 : 0);
        assert.equal(submits, mode === "rejected-submission" ? 1 : 0);
        assert.equal(
          (yield* control("RetryTask", "retry"))._tag,
          "Rejected",
          "Changed revision cannot reuse a receipt payload",
        );
      }),
    );
  });
}

test("internal Check preserves the failed native identity; Retry executes the failed follow-up with a new identity", async () => {
  await run(
    Effect.gen(function* () {
      const calls: HarnessCall[] = [];
      const env = yield* taskFixture({
        harness: makeHarness((input) =>
          Effect.suspend(() => {
            calls.push(input);
            return calls.length === 2 || calls.length === 3
              ? Effect.fail(new AgentError("Known failure", [], { outcome: "failed" }))
              : Effect.succeed({ messages: [] });
          }),
        ),
      });
      const initial = { ...taskInput(), agent: "internal" };
      const state = () =>
        Schema.decodeUnknownSync(TaskSnapshot)(env.registry.get(initial.target)!.state);
      yield* env.tasks.ask((replyTo) => ({ _tag: "StartTask", input: initial, replyTo }));
      yield* env.wait(() => state().status === "completed");
      yield* env.tasks.ask((replyTo) => ({
        _tag: "Input",
        input: {
          requestId: "follow",
          target: initial.target,
          source: initial.replyTo,
          text: "Analyze the new evidence",
        },
        replyTo,
      }));
      yield* env.wait(() => state().status === "failed");
      yield* env.tasks.ask((replyTo) => ({
        _tag: "CheckTask",
        input: {
          requestId: "check",
          target: initial.target,
          expectedRevision: env.registry.get(initial.target)!.revision!,
        },
        replyTo,
      }));
      yield* env.wait(() => state().status === "failed" && state().inputs.length === 3);
      assert.equal(calls[2]!.requestId, "follow");
      const input = {
        requestId: "retry",
        target: initial.target,
        expectedRevision: env.registry.get(initial.target)!.revision!,
      };
      const retry = () =>
        env.tasks.ask<TaskAdmissionReply>((replyTo) => ({ _tag: "RetryTask", input, replyTo }));
      const receipt = yield* retry();
      yield* env.wait(() => state().status === "completed");
      assert.equal(calls[3]!.requestId, "retry");
      assert.match(JSON.stringify(calls[3]), /Analyze the new evidence/);
      assert.deepEqual(yield* retry(), receipt);
      assert.equal(calls.length, 4);
    }),
  );
});

for (const agent of ["internal", "test"] as const) {
  test(`cancellation of ${agent} work requires execution confirmation`, async () => {
    await run(
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const env = yield* taskFixture({
          harness: makeHarness(() =>
            Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
          ),
          agent: fakeAgent({
            wait: () => Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
          }),
        });
        const input = { ...taskInput(), agent };
        yield* env.tasks.ask((replyTo) => ({ _tag: "StartTask", input, replyTo }));
        if (agent === "test") {
          yield* env.wait(() =>
            approvalEntries(env.registry).some((entry) => entry.id === `${input.target}:confirm`),
          );
          yield* env.approvals.ask((replyTo) => ({
            _tag: "Resolve",
            id: `${input.target}:confirm`,
            response: { decision: "approve" },
            replyTo,
          }));
        }
        yield* Deferred.await(entered);
        const actor = yield* env.system.select(`/user${input.target}`).resolve();
        yield* actor.ask((replyTo) => ({ _tag: "Cancel", reason: "Stop", replyTo }));
        assert.equal(
          Schema.decodeUnknownSync(TaskSnapshot)(env.registry.get(input.target)!.state).status,
          agent === "internal" ? "cancelled" : "running",
        );
      }),
    );
  });
}

test("a rejected follow-up cannot be completed by the old provider round and only explicit Retry resends it", async () => {
  await run(
    Effect.gen(function* () {
      const messages = testConversations();
      const record = yield* retainedTask(messages, "running");
      const entered = yield* Deferred.make<void>();
      let followups = 0;
      const env = yield* taskFixture({
        conversations: messages,
        records: new Map([[record.snapshot.path, record]]),
        agent: fakeAgent({
          wait: () =>
            followups < 2
              ? Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never))
              : Effect.succeed({ state: "completed", result: { text: "New result" } }),
          followUp: (session) =>
            Effect.suspend(() => {
              followups++;
              return followups === 1
                ? Effect.fail(
                    new ExternalAgentError({
                      operation: "followUp",
                      outcome: "rejected",
                      message: "Not accepted",
                    }),
                  )
                : Effect.succeed(session);
            }),
          resume: () =>
            Effect.die("A rejected input must be delivered, not an unrelated provider resume"),
        }),
      });
      yield* Deferred.await(entered);
      const state = () =>
        Schema.decodeUnknownSync(TaskSnapshot)(env.registry.get(record.snapshot.path)!.state);
      yield* env.tasks.ask((replyTo) => ({
        _tag: "Input",
        input: {
          requestId: "follow",
          source: "/goals/personal",
          target: record.snapshot.path,
          text: "More work",
        },
        replyTo,
      }));
      yield* env.wait(() => state().status === "failed");
      yield* env.tasks.ask((replyTo) => ({
        _tag: "CheckTask",
        input: {
          requestId: "check",
          target: record.snapshot.path,
          expectedRevision: env.registry.get(record.snapshot.path)!.revision!,
        },
        replyTo,
      }));
      yield* env.wait(() => state().status === "failed" && state().inputs.length === 3);
      assert.equal(followups, 1);
      yield* env.tasks.ask((replyTo) => ({
        _tag: "RetryTask",
        input: {
          requestId: "retry",
          target: record.snapshot.path,
          expectedRevision: env.registry.get(record.snapshot.path)!.revision!,
        },
        replyTo,
      }));
      yield* env.wait(() => state().status === "completed");
      assert.equal(followups, 2);
    }),
  );
});
