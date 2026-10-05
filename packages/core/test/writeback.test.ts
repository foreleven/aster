import { testConversations } from "./conversation-fixtures.js";
import { taskFixture, taskInput } from "./task-fixtures.js";
import type { TaskDeliveryInput } from "@aster/api-contracts";
import assert from "node:assert/strict";
import { test } from "node:test";

import { Deferred, Effect, Schema } from "effect";
import {
  ChannelWrites,
  ChannelWriteError,
  approvalEntries,
  writebackApprovalId,
  WritebackOperation,
  type ContextRecord,
} from "../src/index.js";

import { TaskState } from "../src/tasks/state.js";
import { taskActorPath } from "../src/tasks/address.js";

const definition: TaskDeliveryInput = {
  ...taskInput("publish"),
  action: { _tag: "PublishResult", channelPath: "/lark/im/chats/oc_test", identity: "user" },
};
const fixture = (options: {
  records: Map<string, ContextRecord>;
  conversations?: ReturnType<typeof testConversations>;
  publish: ChannelWrites["Service"]["publish"];
  definition?: TaskDeliveryInput;
  saved?: (record: ContextRecord) => void;
}) =>
  Effect.gen(function* () {
    const env = yield* taskFixture(options);
    const record = () =>
      Object.values(env.registry.snapshot()).find((record) => /^\/tasks\/[^/]+$/.test(record.path));
    const state = () => record() && Schema.decodeUnknownSync(TaskState)(record()!.state);
    const decide = (id: string, decision: "approve" | "reject") =>
      env.approvals.ask<{ error?: string }>((replyTo) => ({
        _tag: "Resolve",
        id,
        response: { decision },
        replyTo,
      }));
    const completed = Effect.gen(function* () {
      yield* env.tasks.ask((replyTo) => ({
        _tag: "StartTask",
        input: options.definition ?? definition,
        replyTo,
      }));
      yield* env.wait(() =>
        approvalEntries(env.registry).some((entry) => entry.id.endsWith(":confirm")),
      );
      assert.deepEqual(yield* decide(`${record()!.path}:confirm`, "approve"), {});
      yield* env.wait(() => state()?.status === "completed");
    });
    const waiting = env.wait(() =>
      approvalEntries(env.registry).some((entry) => entry.id.includes(":writeback:")),
    );
    return { ...env, until: env.wait, record, state, decide, completed, waiting };
  });

test("Run persists the result and exact writeback before a separate approval; forged commands cannot publish", async () => {
  let calls = 0;
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const records = new Map<string, ContextRecord>();
        const conversations = testConversations();
        const env = yield* fixture({
          records,
          conversations,
          publish: (request, authorization) =>
            Effect.sync(() => {
              calls++;
              const retained = Schema.decodeUnknownSync(TaskState)(
                records.get(request.source)!.state,
              );
              assert.equal(retained.status, "completed");
              assert.equal(retained.writeback!.status, "sending");
              assert.deepEqual(retained.writeback!.request, request);
              assert.equal(authorization.approvalId, writebackApprovalId(request));
              assert.ok(authorization.approvalsRevision > 0);
              assert.equal(request.content, "done");
              assert.equal(request.action.identity, "user");
              assert.equal(request.causal.remainingAgentTurns, 0);
              assert.equal(request.requestId.length, 48);
              return { externalId: "om_one" };
            }),
        });
        yield* env.completed;
        yield* env.waiting;
        assert.equal(calls, 0, "Task confirmation is not publication approval");
        const operation = env.state()!.writeback!;
        assert.deepEqual(env.state()!.writeback!.request.action, operation.request.action);
        const id = writebackApprovalId(operation.request);
        const entry = approvalEntries(env.registry).find((entry) => entry.id === id)!;
        assert.match(entry.request.prompt, /oc_test as user/);
        assert.match(entry.request.prompt, /\n\ndone\n\n/);
        const actor = yield* env.system.select(taskActorPath(env.record()!.path)).resolve();
        yield* actor.tell({
          _tag: "ApprovalResolved",
          requestId: id,
          response: { decision: "approve" },
        });
        // A mailbox barrier: the following acknowledgement is processed after the forged message.
        yield* actor.ask<void>((replyTo) => ({ _tag: "Cancel", reason: "barrier", replyTo }));
        assert.equal(calls, 0);
        assert.equal(env.state()!.writeback!.status, "waiting-approval");
        assert.deepEqual(yield* env.decide(id, "approve"), {});
        yield* env.until(() => env.state()?.writeback?.status === "published");
        yield* actor.tell({
          _tag: "ApprovalResolved",
          requestId: id,
          response: { decision: "approve" },
        });
        yield* actor.tell({ _tag: "Finished", outcome: { _tag: "Completed", text: "done" } });
        yield* actor.ask<void>((replyTo) => ({ _tag: "Cancel", reason: "barrier", replyTo }));
        assert.equal(calls, 1);
        const view = env.registry.views.project(env.record()!);
        assert.equal(
          Schema.decodeUnknownSync(Schema.Struct({ writeback: WritebackOperation }))(view.state)
            .writeback.status,
          "published",
        );
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

for (const outcome of ["published", "unknown", "rejected"] as const) {
  test(`writeback ${outcome} survives restart without another external submission`, async () => {
    const records = new Map<string, ContextRecord>();
    const conversations = testConversations();
    let calls = 0;
    for (let restart = 0; restart < 2; restart++) {
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const env = yield* fixture({
              records,
              conversations,
              publish: () =>
                Effect.suspend(() => {
                  calls++;
                  return outcome === "published"
                    ? Effect.succeed({ externalId: "om_once" })
                    : Effect.fail(
                        new ChannelWriteError({ outcome, message: "Injected publication outcome" }),
                      );
                }),
            });
            if (restart === 0) {
              yield* env.completed;
              yield* env.waiting;
              yield* env.decide(writebackApprovalId(env.state()!.writeback!.request), "approve");
            }
            yield* env.until(() => env.state()?.writeback?.status === outcome);

            const actor = yield* env.system.select(taskActorPath(env.record()!.path)).resolve();
            yield* actor.tell({ _tag: "Resume", path: env.record()!.path });
            yield* actor.ask<void>((replyTo) => ({ _tag: "Cancel", reason: "barrier", replyTo }));
            assert.equal(calls, 1);
          }),
        ).pipe(Effect.timeout("5 seconds")),
      );
    }
  });
}

test("interrupted publication remains unknown on recovery, even when the external call might have completed", async () => {
  const records = new Map<string, ContextRecord>();
  const conversations = testConversations();
  const entered = Deferred.makeUnsafe<void>();
  let calls = 0;
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const env = yield* fixture({
          records,
          conversations,
          publish: () =>
            Effect.gen(function* () {
              calls++;
              yield* Deferred.succeed(entered, undefined);
              return yield* Effect.never;
            }),
        });
        yield* env.completed;
        yield* env.waiting;
        yield* env.decide(writebackApprovalId(env.state()!.writeback!.request), "approve");
        yield* Deferred.await(entered);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const env = yield* fixture({
          records,
          conversations,
          publish: () => Effect.die("Unknown publication must not retry"),
        });
        yield* env.until(() => env.state()?.writeback?.status === "unknown");
        assert.equal(calls, 1);
        assert.match(env.state()!.writeback!.error!, /interrupted/);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("rejecting publication keeps the local result and performs no external write", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const env = yield* fixture({
          records: new Map(),
          definition,
          publish: () => Effect.die("Rejected publication cannot send"),
        });
        yield* env.completed;
        yield* env.waiting;
        yield* env.decide(writebackApprovalId(env.state()!.writeback!.request), "reject");
        yield* env.until(() => env.state()?.writeback?.status === "rejected");
        assert.equal(env.state()!.status, "completed");
        assert.notEqual(env.state()!.outcomeEntryId, undefined);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("a completed Run without an explicit Signal action stays local", async () => {
  const { action: _action, ...localOnly } = definition;
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const env = yield* fixture({
          records: new Map(),
          definition: localOnly,
          publish: () => Effect.die(new Error("Local results cannot publish")),
        });
        yield* env.completed;
        const actor = yield* env.system.select(taskActorPath(env.record()!.path)).resolve();
        yield* actor.ask<void>((replyTo) => ({ _tag: "Cancel", reason: "barrier", replyTo }));
        assert.equal(env.state()!.writeback, undefined);
        assert.equal(
          approvalEntries(env.registry).some((entry) => entry.id.includes(":writeback:")),
          false,
        );
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

for (const phase of ["sending", "published"] as const) {
  test(`lost ${phase} commit acknowledgement does not repeat publication after owner restart`, async () => {
    const records = new Map<string, ContextRecord>();
    const conversations = testConversations();
    let calls = 0;
    let lost = false;
    const acknowledgementLost = Deferred.makeUnsafe<void>();
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const env = yield* fixture({
            records,
            conversations,
            saved: (record) => {
              if (
                !lost &&
                record.path.includes("/tasks/") &&
                Schema.decodeUnknownSync(TaskState)(record.state).writeback?.status === phase
              ) {
                lost = true;
                Deferred.doneUnsafe(acknowledgementLost, Effect.void);
                throw new Error("Injected commit acknowledgement loss");
              }
            },
            publish: () =>
              Effect.sync(() => {
                calls++;
                return { externalId: "om_once" };
              }),
          });
          yield* env.completed;
          yield* env.waiting;
          yield* env.decide(writebackApprovalId(env.state()!.writeback!.request), "approve");
          yield* Deferred.await(acknowledgementLost);
          assert.equal(lost, true);
          assert.equal(calls, phase === "sending" ? 0 : 1);
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const env = yield* fixture({
            records,
            conversations,
            publish: () => Effect.die(new Error("Uncertain submission cannot retry")),
          });
          const status = phase === "sending" ? "unknown" : "published";
          yield* env.until(() => env.state()?.writeback?.status === status);

          const actor = yield* env.system.select(taskActorPath(env.record()!.path)).resolve();
          yield* actor.tell({ _tag: "Resume", path: env.record()!.path });
          yield* actor.ask<void>((replyTo) => ({ _tag: "Cancel", reason: "barrier", replyTo }));
          assert.equal(calls, phase === "sending" ? 0 : 1);
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
  });
}
