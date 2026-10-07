import { testConversations } from "./conversation-fixtures.js";
import { readSignalHistory } from "../src/signals/state/store.js";
import { taskFixture } from "./task-fixtures.js";
import { TestClock } from "effect/testing";
import assert from "node:assert/strict";
import { test } from "node:test";

import { Clock, Effect, Fiber, Stream } from "effect";
import { SignalRootActor, type StoredContext } from "../src/index.js";

import type { SignalCommandReply } from "../src/signals/protocol.js";
import type { SignalChangeInput } from "../src/signals/protocol.js";
const input: SignalChangeInput = {
  requestId: "create",
  source: "/goals/personal",
  target: "/signals/personal--watch",
  change: {
    operation: "create",
    definition: {
      task: { _tag: "Goal", target: "/goals/personal", text: "Notify the Goal" },
      trigger: { _tag: "Schedule", schedule: { type: "once", at: "2099-01-01T00:00:00Z" } },
    },
  },
  causal: { rootRequestId: "user", remainingAgentTurns: 3 },
};
const setup = Effect.fnUntraced(function* (
  records: Map<string, StoredContext>,
  clock?: Clock.Clock,
  conversations = testConversations(),
) {
  const env = yield* taskFixture({ records, clock, conversations });
  const root = yield* env.system.spawn("signals", SignalRootActor);
  const command = (value: SignalChangeInput) =>
    root.ask<SignalCommandReply>((replyTo) => ({
      _tag: "Change",
      input: value,
      replyTo,
    }));
  return { ...env, command };
});
test("Signal owns direct command receipts across restarts, rejects stale changes and preserves ownership", async () => {
  const records = new Map<string, StoredContext>();
  const conversations = testConversations();
  for (const restart of [false, true])
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const env = yield* setup(records, undefined, conversations);
          assert.deepEqual(yield* env.command(input), {
            _tag: "Accepted",
            receipt: { requestId: "create", revision: 1 },
          });
          const before = env.registry.get(input.target)!;
          assert.equal(
            (yield* env.command({
              ...input,
              change: {
                operation: "create",
                definition: {
                  trigger: { _tag: "Context", when: "Different" },
                  task: { _tag: "Goal", target: "/goals/personal", text: "Different" },
                },
              },
            }))._tag,
            "Rejected",
          );
          assert.deepEqual(env.registry.get(input.target), before);
          if (!restart) return;
          const update: SignalChangeInput = {
            ...input,
            requestId: "update",
            change: {
              operation: "update",
              version: 1,
              definition: {
                task: { _tag: "Goal", target: "/goals/personal", text: "Notify the Goal" },
                trigger: { _tag: "Context", when: "New condition" },
              },
            },
          };
          assert.equal((yield* env.command(update))._tag, "Accepted");
          assert.equal(
            (env.registry.get(input.target)!.state as { schedule?: unknown }).schedule,
            undefined,
          );
          assert.equal((yield* env.command({ ...update, requestId: "stale" }))._tag, "Rejected");
          assert.equal((yield* env.command({ ...input, source: "/goals/other" }))._tag, "Rejected");
          assert.deepEqual(yield* env.command(input), {
            _tag: "Accepted",
            receipt: { requestId: "create", revision: 1 },
          });
          assert.equal(
            (yield* env.command({
              ...input,
              requestId: "delete",
              change: { operation: "delete", version: 2 },
            }))._tag,
            "Accepted",
          );
          assert.equal(
            (env.registry.get(input.target)!.state as { status: string }).status,
            "deleted",
          );
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
});

test("Signal timer lives outside conversation, reschedules by version and executes its Goal Task exactly once", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const clock = yield* TestClock.make();
        const start = Date.parse("2026-10-01T00:00:00Z");
        yield* clock.adjust(start);
        const env = yield* setup(new Map(), clock);
        yield* env.command({
          ...input,
          change: {
            operation: "create",
            definition: {
              task: { _tag: "Goal", target: "/goals/personal", text: "Notify" },
              trigger: {
                _tag: "Schedule",
                schedule: { type: "once", at: new Date(start + 10000).toISOString() },
              },
            },
          },
        });
        yield* env.wait(
          () =>
            (env.registry.get(input.target)?.state as { nextDue?: string }).nextDue ===
            new Date(start + 10000).toISOString(),
        );
        yield* env.command({
          ...input,
          requestId: "reschedule",
          change: {
            operation: "update",
            version: 1,
            definition: {
              task: { _tag: "Goal", target: "/goals/personal", text: "Notify" },
              trigger: {
                _tag: "Schedule",
                schedule: { type: "once", at: new Date(start + 20000).toISOString() },
              },
            },
          },
        });
        yield* clock.adjust(11000);
        assert.equal(
          (yield* readSignalHistory(env.conversations, input.target)).deliveries.length,
          0,
        );
        const delivered = yield* Stream.runHead(
          env.system.events.pipe(
            Stream.filter(
              (event) =>
                event._tag === "CommandProcessed" &&
                event.path === "/user/signals/personal--watch" &&
                event.commandTag === "Delivered",
            ),
          ),
        ).pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        yield* clock.adjust(10000);
        yield* Fiber.join(delivered);
        const history = yield* readSignalHistory(env.conversations, input.target);
        assert.equal(history.deliveries[0]?.status, "delivered");
        assert.equal(history.snapshot?.nextDue, null);
        assert.equal(
          Object.keys(env.registry.snapshot()).some((path) => path.includes("/tasks/")),
          false,
        );
        yield* clock.adjust(60000);
        assert.equal(
          (yield* readSignalHistory(env.conversations, input.target)).deliveries.length,
          1,
        );
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});
