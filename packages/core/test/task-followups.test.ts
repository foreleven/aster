import { testConversations } from "./conversation-fixtures.js";
import assert from "node:assert/strict";
import { test } from "node:test";
import { AgentRunner } from "@aster/agent";
import { Deferred, Effect, Schema } from "effect";
import {
  TaskState,
  approvalEntries,
  type ContextRecord,
  type TaskAdmissionReply,
} from "../src/index.js";
import { taskFixture, taskInput, retainedTask } from "./task-fixtures.js";
import { fakeAgent } from "./fixtures.js";

const run = <A, E>(effect: Effect.Effect<A, E, import("effect").Scope.Scope>) =>
  Effect.runPromise(Effect.scoped(effect).pipe(Effect.timeout("6 seconds")));

test("internal Tasks admit follow-ups while working, retain exact receipts, and reactivate", async () => {
  await run(
    Effect.gen(function* () {
      const entered = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      let calls = 0;
      const env = yield* taskFixture({
        runner: AgentRunner.make((input) =>
          Effect.gen(function* () {
            calls++;
            assert.equal(input.durable?.owner, "tasks");
            if (calls === 1) {
              yield* Deferred.succeed(entered, undefined);
              yield* Deferred.await(release);
            }
            return { messages: [] };
          }),
        ),
      });
      const input = { ...taskInput(), agent: "internal" };
      yield* env.tasks.ask((replyTo) => ({ _tag: "StartTask", input, replyTo }));
      yield* Deferred.await(entered);
      const state = () =>
        Schema.decodeUnknownSync(TaskState)(env.registry.get(input.target)!.state);
      const follow = (requestId: string, text = "Add analysis") =>
        env.tasks.ask<TaskAdmissionReply>((replyTo) => ({
          _tag: "FollowupTask",
          input: { requestId, text, target: input.target, source: input.replyTo },
          replyTo,
        }));
      const receipt = yield* follow("follow");
      assert.equal(receipt._tag, "Accepted");
      assert.equal(calls, 1, "Admission is responsive while execution is still blocked");
      assert.equal((yield* follow("follow", "Different"))._tag, "Rejected");
      yield* Deferred.succeed(release, undefined);
      yield* env.wait(() => state().status === "completed");
      assert.equal(calls, 2);
      assert.ok(state().inputs.every((input) => input.status === "completed"));
      assert.deepEqual(yield* follow("follow"), receipt);
      yield* follow("later", "Continue the report");
      yield* env.wait(() => state().status === "completed");
      assert.equal(calls, 3);
      assert.equal(state().inputs.length, 3);
      assert.doesNotMatch(JSON.stringify(state()), /Add analysis|Continue the report/);
      assert.equal(
        (yield* env.conversations.read(input.target)).filter(
          (entry) => entry.kind === "task.result",
        ).length,
        2,
      );
    }),
  );
});

test("external follow-up supersedes late completion from the earlier execution", async () => {
  await run(
    Effect.gen(function* () {
      const waiting = yield* Deferred.make<void>();
      const oldResult = yield* Deferred.make<void>();
      const nextResult = yield* Deferred.make<void>();
      const env = yield* taskFixture({
        agent: fakeAgent({
          submit: () => Effect.succeed({ sessionId: "first" }),
          followUp: (session, input) => {
            assert.equal(session.sessionId, "first");
            assert.equal(input.text, "More work");
            return Effect.succeed({ sessionId: "next" });
          },
          wait: (session) =>
            session.sessionId === "first"
              ? Deferred.succeed(waiting, undefined).pipe(
                  Effect.andThen(Deferred.await(oldResult)),
                  Effect.as({ state: "completed", result: { text: "Old result" } }),
                )
              : Deferred.await(nextResult).pipe(
                  Effect.as({ state: "completed", result: { text: "New result" } }),
                ),
        }),
      });
      const input = taskInput();
      const state = () =>
        Schema.decodeUnknownSync(TaskState)(env.registry.get(input.target)!.state);
      yield* env.tasks.ask((replyTo) => ({ _tag: "StartTask", input, replyTo }));
      yield* env.wait(() =>
        approvalEntries(env.registry).some((entry) => entry.id === `${input.target}:confirm`),
      );
      yield* env.approvals.ask((replyTo) => ({
        _tag: "Resolve",
        id: `${input.target}:confirm`,
        response: { decision: "approve" },
        replyTo,
      }));
      yield* env.approvals.tell({ _tag: "Deliver" });
      yield* Deferred.await(waiting);
      yield* env.tasks.ask((replyTo) => ({
        _tag: "FollowupTask",
        input: {
          target: input.target,
          source: input.replyTo,
          requestId: "follow",
          text: "More work",
        },
        replyTo,
      }));
      yield* env.wait(() => state().session?.sessionId === "next");
      yield* Deferred.succeed(oldResult, undefined);
      const actor = yield* env.system.select(`/user${input.target}`).resolve();
      yield* actor.ask((replyTo) => ({ _tag: "Ready", replyTo }));
      assert.equal(state().status, "running");
      yield* Deferred.succeed(nextResult, undefined);
      yield* env.wait(() => state().status === "completed");
      const outcome = yield* env.conversations.get(input.target, state().outcomeEntryId!);
      assert.deepEqual(
        Schema.decodeUnknownSync(Schema.Struct({ text: Schema.String, status: Schema.String }))(
          outcome.data,
        ),
        { text: "New result", status: "completed" },
      );
    }),
  );
});

test("Pi result admission recovers an interrupted Actor handoff without executing again", async () => {
  const conversations = testConversations();
  const records = new Map<string, ContextRecord>();
  let previous: ContextRecord | undefined;
  let injected = false;
  let calls = 0;
  await run(
    Effect.gen(function* () {
      const cut = yield* Deferred.make<void>();
      const input = { ...taskInput(), agent: "internal" };
      const env = yield* taskFixture({
        conversations,
        records,
        runner: AgentRunner.make(() =>
          Effect.sync(() => {
            calls++;
            return { messages: [] };
          }),
        ),
        saved: (record) => {
          if (record.path !== input.target) return;
          const state = Schema.decodeUnknownSync(TaskState)(record.state);
          if (!injected && state.status === "completed") {
            injected = true;
            records.set(record.path, previous!);
            Effect.runSync(Deferred.succeed(cut, undefined));
            throw new Error("Actor result commit interrupted");
          }
          previous = record;
        },
      });
      yield* env.tasks.ask((replyTo) => ({ _tag: "StartTask", input, replyTo }));
      yield* Deferred.await(cut);
      yield* env.wait(
        () =>
          (env.registry.get(input.target)?.state as { status?: string })?.status === "completed",
      );
      assert.equal(calls, 1);
      assert.equal(
        (yield* conversations.read(input.target)).filter((entry) => entry.kind === "task.result")
          .length,
        1,
      );
    }),
  );
});

test("restoring an interrupted external follow-up cannot complete it from the preceding handle", async () => {
  const conversations = testConversations();
  await run(
    Effect.gen(function* () {
      const record = yield* retainedTask(conversations, "running");
      const requestId = "interrupted-followup";
      const receipt = { requestId, revision: 3 };
      const entry = yield* conversations.append(record.path, requestId, "task.input", {
        requestId,
        target: record.path,
        source: "/goals/personal",
        text: "More work",
        receipt,
      });
      const pending: TaskState = {
        ...record.state,
        inputs: [
          ...record.state.inputs,
          { requestId, entryId: entry.id, receipt, status: "sending" },
        ],
      };
      const env = yield* taskFixture({
        conversations,
        records: new Map([[record.path, { ...record, state: pending }]]),
        agent: fakeAgent({
          followUp: () => Effect.die("Must not resubmit unknown work"),
          status: () => Effect.die("The preceding handle cannot prove follow-up acceptance"),
          wait: () => Effect.die("Must not complete unknown work from an older execution"),
        }),
      });
      const state = () => Schema.decodeUnknownSync(TaskState)(env.registry.get(record.path)!.state);
      assert.equal(state().status, "uncertain");
      assert.equal(state().inputs.at(-1)?.status, "unknown");
      yield* env.tasks.ask((replyTo) => ({
        _tag: "ResumeTask",
        input: {
          target: record.path,
          requestId: "reconcile",
          expectedRevision: env.registry.get(record.path)!.revision!,
        },
        replyTo,
      }));
      yield* env.wait(() => state().resumptions?.[0]?.status === "unknown");
      assert.equal(state().status, "uncertain");
      assert.equal(state().inputs.at(-1)?.status, "unknown");
    }),
  );
});

for (const status of ["completed", "failed"] as const) {
  test(`Pi-only follow-up reactivates a ${status} Task after interrupted Actor admission`, async () => {
    const conversations = testConversations();
    await run(
      Effect.gen(function* () {
        const original = yield* retainedTask(conversations, status);
        const record = {
          ...original,
          state: { ...original.state, session: { sessionId: "original" } },
        };
        const input = {
          requestId: "orphan",
          target: record.path,
          source: "/goals/personal",
          text: "Continue the analysis",
        };
        const receipt = { requestId: input.requestId, revision: 3 };
        yield* conversations.append(record.path, input.requestId, "task.input", {
          ...input,
          receipt,
        });
        let deliveries = 0;
        const env = yield* taskFixture({
          conversations,
          records: new Map([[record.path, record]]),
          agent: fakeAgent({
            submit: () => Effect.die("Follow-up must retain its original session"),
            followUp: (session, followup) =>
              Effect.sync(() => {
                deliveries++;
                assert.equal(session.sessionId, "original");
                assert.equal(followup.text, input.text);
                return { sessionId: "continued" };
              }),
            wait: () =>
              Effect.succeed({ state: "completed", result: { text: "Continued result" } }),
          }),
        });
        const state = () =>
          Schema.decodeUnknownSync(TaskState)(env.registry.get(record.path)!.state);
        yield* env.wait(() => state().status === "completed" && state().inputs.length === 2);
        assert.equal(deliveries, 1);
        assert.equal(state().inputs.at(-1)?.status, "completed");
        const retry = yield* env.tasks.ask<TaskAdmissionReply>((replyTo) => ({
          _tag: "FollowupTask",
          input,
          replyTo,
        }));
        assert.deepEqual(retry, { _tag: "Accepted", receipt });
      }),
    );
  });
}

test("follow-up does not discard a successful answer to an earlier approval request", async () => {
  await run(
    Effect.gen(function* () {
      const answering = yield* Deferred.make<void>();
      const releaseAnswer = yield* Deferred.make<void>();
      const releaseResult = yield* Deferred.make<void>();
      const env = yield* taskFixture({
        agent: fakeAgent({
          submit: () => Effect.succeed({ sessionId: "original" }),
          wait: (session) =>
            session.sessionId === "original"
              ? Effect.succeed({
                  state: "waiting_input",
                  requests: [{ id: "question", kind: "input", prompt: "Choose a region" }],
                })
              : Deferred.await(releaseResult).pipe(
                  Effect.as({ state: "completed", result: { text: "Updated report" } }),
                ),
          respond: () =>
            Deferred.succeed(answering, undefined).pipe(
              Effect.andThen(Deferred.await(releaseAnswer)),
            ),
          followUp: () => Effect.succeed({ sessionId: "next" }),
        }),
      });
      const input = taskInput();
      const state = () =>
        Schema.decodeUnknownSync(TaskState)(env.registry.get(input.target)!.state);
      const resolve = (id: string, response: { decision: "approve" } | { text: string }) =>
        env.approvals.ask((replyTo) => ({ _tag: "Resolve", id, response, replyTo }));
      yield* env.tasks.ask((replyTo) => ({ _tag: "StartTask", input, replyTo }));
      yield* env.wait(() =>
        approvalEntries(env.registry).some((entry) => entry.id === `${input.target}:confirm`),
      );
      yield* resolve(`${input.target}:confirm`, { decision: "approve" });
      yield* env.approvals.tell({ _tag: "Deliver" });
      const question = `${input.target}:input:question`;
      yield* env.wait(() => approvalEntries(env.registry).some((entry) => entry.id === question));
      yield* resolve(question, { text: "Europe" });
      yield* env.approvals.tell({ _tag: "Deliver" });
      yield* Deferred.await(answering);
      yield* env.tasks.ask((replyTo) => ({
        _tag: "FollowupTask",
        input: {
          requestId: "more",
          target: input.target,
          source: input.replyTo,
          text: "Include Asia",
        },
        replyTo,
      }));
      yield* env.wait(() => state().session?.sessionId === "next");
      yield* Deferred.succeed(releaseAnswer, undefined);
      yield* env.wait(
        () =>
          approvalEntries(env.registry).find((entry) => entry.id === question)?.status ===
          "acknowledged",
      );
      assert.equal(state().responses?.[0]?.status, "sent");
      assert.equal(state().session?.sessionId, "next");
      yield* Deferred.succeed(releaseResult, undefined);
      yield* env.wait(() => state().status === "completed");
    }),
  );
});
