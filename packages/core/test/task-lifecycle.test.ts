import { TaskDeliveryInput } from "@aster/api-contracts";
import { SignalRootActor } from "../src/signals/root.js";
import assert from "node:assert/strict";
import { test } from "node:test";
import { Deferred, Effect, Fiber, Stream, Schema } from "effect";
import { approvalEntries, DEFAULT_EXECUTOR_PROMPT } from "../src/index.js";
import { TaskSnapshot } from "../src/tasks/state/snapshot.js";
import type { TaskAdmissionReply } from "../src/tasks/protocol.js";
import { taskFixture, taskInput } from "./task-fixtures.js";
import { fakeAgent } from "./fixtures.js";
const run = <A, E>(effect: Effect.Effect<A, E, import("effect").Scope.Scope>) =>
  Effect.runPromise(Effect.scoped(effect).pipe(Effect.timeout("6 seconds")));

test("Task admission freezes executor policy, cannot bypass approval, and retries reuse the receipt", async () => {
  await run(
    Effect.gen(function* () {
      let submissions = 0;
      const submitted = yield* Deferred.make<void>();
      const env = yield* taskFixture({
        agent: fakeAgent({
          submit: (task) =>
            Effect.gen(function* () {
              submissions++;
              assert.match(task.instructions, /Read evidence/);
              assert.ok(task.instructions.includes(DEFAULT_EXECUTOR_PROMPT));
              yield* Deferred.succeed(submitted, undefined);
              return { sessionId: "once" };
            }),
        }),
      });
      const input = taskInput();
      const send = (value: typeof input) =>
        env.tasks.ask<TaskAdmissionReply>((replyTo) => ({
          _tag: "StartTask",
          input: value,
          replyTo,
        }));
      const first = yield* send(input);
      assert.equal(first._tag, "Accepted");
      assert.deepEqual(
        Schema.decodeUnknownSync(TaskDeliveryInput)(
          (yield* env.conversations.get(
            input.target,
            Schema.decodeUnknownSync(TaskSnapshot)(env.records.get(input.target)!.state).inputs[0]!
              .entryId,
          )).data,
        ),
        input,
      );
      assert.deepEqual(yield* send(input), first);
      assert.equal(
        (yield* send({ ...input, task: { instructions: "Other", input: [] } }))._tag,
        "Rejected",
      );
      const actor = yield* env.system.select(`/user${input.target}`).resolve();
      const processed = yield* Stream.runHead(
        env.system.events.pipe(
          Stream.filter(
            (event) =>
              event._tag === "CommandProcessed" &&
              event.path === `/user${input.target}` &&
              event.commandTag === "ApprovalResolved",
          ),
        ),
      ).pipe(Effect.forkScoped({ startImmediately: true }));
      yield* actor.tell({
        _tag: "ApprovalResolved",
        requestId: `${input.target}:confirm`,
        response: { decision: "approve" },
      });
      yield* Fiber.join(processed);
      assert.equal(submissions, 0);
      const resume = yield* env.tasks.ask<TaskAdmissionReply>((replyTo) => ({
        _tag: "CheckTask",
        input: {
          requestId: "bypass",
          target: input.target,
          expectedRevision: env.registry.get(input.target)!.revision,
        },
        replyTo,
      }));
      assert.equal(resume._tag, "Rejected");
      yield* env.wait(() => approvalEntries(env.registry).length === 1);
      const decide = () =>
        env.approvals.ask((replyTo) => ({
          _tag: "Resolve",
          id: `${input.target}:confirm`,
          response: { decision: "approve" },
          replyTo,
        }));
      assert.deepEqual(yield* decide(), {});
      assert.deepEqual(yield* decide(), {});
      yield* Deferred.await(submitted);
      yield* env.wait(
        () =>
          Schema.decodeUnknownSync(TaskSnapshot)(env.registry.get(input.target)!.state).status ===
          "completed",
      );
      assert.equal(submissions, 1);
    }),
  );
});

test("Context Signal executes its frozen Delegate Task through the shared Run root", async () => {
  await run(
    Effect.gen(function* () {
      const env = yield* taskFixture();
      const signals = yield* env.system.spawn("signals", SignalRootActor);
      const target = "/signals/personal--watch";
      const configured = yield* signals.ask<
        import("../src/signals/protocol.js").SignalCommandReply
      >((replyTo) => ({
        _tag: "Change",
        replyTo,
        input: {
          requestId: "create",
          source: "/goals/personal",
          target,
          causal: { rootRequestId: "create", remainingAgentTurns: 3 },
          change: {
            operation: "create",
            definition: {
              trigger: { _tag: "Context", when: "Evidence changes" },
              task: {
                _tag: "Delegate",
                agent: "test",
                task: { instructions: "Read the update", input: [] },
                replyTo: "/goals/personal",
              },
            },
          },
        },
      }));
      assert.equal(configured._tag, "Accepted");
      const reaction = {
        requestId: "change",
        causationId: "source",
        source: "/system-one" as const,
        target,
        expectedRevision: env.registry.get(target)!.revision,
        sourceContext: {
          path: "/source",
          description: "Evidence",
          revision: 1,
          state: { text: "frozen" },
          messages: [],
        },
      };
      const accepted = yield* signals.ask((replyTo) => ({
        _tag: "React",
        input: reaction,
        replyTo,
      }));
      assert.deepEqual(
        yield* signals.ask((replyTo) => ({ _tag: "React", input: reaction, replyTo })),
        accepted,
      );
      yield* env.wait(() =>
        Object.keys(env.registry.snapshot()).some((path) => /^\/tasks\/[a-f0-9]{64}$/.test(path)),
      );
      const records = Object.values(env.registry.snapshot()).filter((record) =>
        /^\/tasks\/[a-f0-9]{64}$/.test(record.path),
      );
      assert.equal(records.length, 1);
      const state = Schema.decodeUnknownSync(TaskSnapshot)(records[0]!.state);
      const admission = Schema.decodeUnknownSync(TaskDeliveryInput)(
        (yield* env.conversations.get(records[0]!.path, state.inputs[0]!.entryId)).data,
      );
      assert.deepEqual(admission.evidence, reaction.sourceContext);
      assert.deepEqual(admission.task, { instructions: "Read the update", input: [] });
    }),
  );
});
