import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem, ActorTestKit } from "@aster/actor";
import { Effect, Layer, Schema } from "effect";
import {
  defineContext,
  ContextRegistry,
  ExternalAgents,
  SignalDefinitions,
  SignalRootActor,
  makeContextRegistry,
  type ContextRecord,
  type GoalCommand,
} from "../src/index.js";
import type { SignalCommandReply, SignalConfigureReply } from "../src/signals/actors.js";
import type { GoalSignalInput } from "../src/signals/goal-command.js";
import { fakeAgent, preparationLayer } from "./fixtures.js";

const input: GoalSignalInput = {
  requestId: "goal-signal-1",
  evaluationId: "evaluation-1",
  source: "/goals/release",
  target: "/signals/release--watch",
  expectedRevision: 0,
  operation: "create",
  definition: {
    slug: "release--watch",
    when: "Blockers change",
    task: "Review blockers",
    agent: "test",
    mode: "confirm",
    schedule: { type: "once", at: "2099-01-01T00:00:00Z" },
  },
  causal: { rootRequestId: "user-1", remainingAgentTurns: 3 },
  createdAt: "2026-10-02T00:00:00Z",
};
const fixture = (records: Map<string, ContextRecord>) =>
  Effect.gen(function* () {
    const registry = yield* makeContextRegistry({
      loadAll: () => [...records.values()],
      save: (record) => {
        records.set(record.path, structuredClone(record));
      },
    });
    yield* registry.register(
      "/goals/release",
      defineContext({
        identity: "Goal publisher",
        state: Schema.Record(Schema.String, Schema.Unknown),
        message: Schema.Unknown,
      }),
    );
    const system = yield* ActorSystem.make().pipe(
      ActorSystem.provide(
        Layer.succeed(ContextRegistry, registry),
        Layer.succeed(SignalDefinitions, []),
        preparationLayer,
        Layer.succeed(ExternalAgents, {
          test: fakeAgent({ submit: () => Effect.die("Unexpected execution") }),
        }),
      ),
    );
    const root = yield* system.spawn("signals", SignalRootActor);
    const subscriber = yield* ActorTestKit.probe<GoalCommand>();
    const command = (value: GoalSignalInput) =>
      root.ask<SignalCommandReply>((replyTo) => ({
        _tag: "ApplyGoalCommand",
        input: value,
        subscriber: subscriber.ref,
        replyTo,
      }));
    const publish = (value: GoalSignalInput, tasks: readonly unknown[] = []) =>
      Effect.gen(function* () {
        const previous = registry.get(value.source);
        yield* registry.commit(
          {
            path: value.source,
            description: "Release",
            state: {
              status: "active",
              tasks,
              signalOutbox: [{ input: value, status: "pending", attempts: 0 }],
            },
            messages: [],
          },
          { expectedRevision: previous?.revision ?? 0 },
        );
      });
    return { registry, root, subscriber, command, publish };
  });

test("Goal Signal receipt survives restart and unchanged reattachment without a second revision", async () => {
  const records = new Map<string, ContextRecord>();
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const env = yield* fixture(records);
        yield* env.publish(input);
        assert.deepEqual(yield* env.command(input), {
          _tag: "Accepted",
          receipt: { requestId: input.requestId, revision: 1 },
        });
        const snapshot = env.registry.get(input.target)!;
        assert.equal(
          (snapshot.state as { goalCommandReceipts: unknown[] }).goalCommandReceipts.length,
          1,
        );
        assert.equal(
          (yield* env.command({ ...input, definition: { ...input.definition, task: "Changed" } }))
            ._tag,
          "Rejected",
        );
        assert.deepEqual(env.registry.get(input.target), snapshot);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
  const original = structuredClone(records.get(input.target)!);
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const env = yield* fixture(records);
        const reattach = yield* env.root.ask<SignalConfigureReply>((replyTo) => ({
          _tag: "Upsert",
          definition: input.definition,
          goal: "release",
          active: true,
          subscriber: env.subscriber.ref,
          replyTo,
        }));
        assert.equal(reattach._tag, "Accepted");
        assert.deepEqual(env.registry.get(input.target), original);
        assert.deepEqual(yield* env.command(input), {
          _tag: "Accepted",
          receipt: { requestId: input.requestId, revision: 1 },
        });
        const update: GoalSignalInput = {
          ...input,
          requestId: "update",
          expectedRevision: 1,
          operation: "update",
          definition: {
            slug: input.definition.slug,
            when: "Changed",
            task: "Inspect",
            mode: "confirm",
            agent: "test",
          },
        };
        yield* env.publish(update);
        assert.equal((yield* env.command(update))._tag, "Accepted");
        assert.equal(env.registry.get(input.target)?.revision, 2);
        assert.equal("schedule" in env.registry.get(input.target)!.state, false);
        assert.equal("nextDue" in env.registry.get(input.target)!.state, false);
        // Original replay remains stable even after a later update and publisher journal change.
        assert.deepEqual(yield* env.command(input), {
          _tag: "Accepted",
          receipt: { requestId: input.requestId, revision: 1 },
        });
        const stale = { ...update, requestId: "stale" };
        yield* env.publish(stale);
        assert.equal((yield* env.command(stale))._tag, "Rejected");
        assert.equal(env.registry.get(input.target)?.revision, 2);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("Goal Signal receiver requires published intent, live Task and correct ownership", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const env = yield* fixture(new Map());
        assert.equal((yield* env.command(input))._tag, "Rejected");
        const missingTask = { ...input, definition: { ...input.definition, taskId: "missing" } };
        yield* env.publish(missingTask);
        assert.equal((yield* env.command(missingTask))._tag, "Rejected");
        assert.equal(env.registry.get(input.target), undefined);
        yield* env.publish(missingTask, [{ id: "missing", status: "open" }]);
        assert.equal((yield* env.command(missingTask))._tag, "Accepted");
        const snapshot = env.registry.get(input.target)!;
        const stolen = yield* env.root.ask<SignalConfigureReply>((replyTo) => ({
          _tag: "Upsert",
          definition: input.definition,
          goal: "other",
          active: true,
          subscriber: env.subscriber.ref,
          replyTo,
        }));
        assert.equal(stolen._tag, "Rejected");
        assert.deepEqual(env.registry.get(input.target), snapshot);
        // A deleted Task must not prevent removing its monitor.
        const remove: GoalSignalInput = {
          ...missingTask,
          requestId: "delete",
          operation: "delete",
          expectedRevision: 1,
        };
        yield* env.publish(remove, [{ id: "missing", status: "deleted" }]);
        assert.equal((yield* env.command(remove))._tag, "Accepted");
        assert.equal((env.registry.get(input.target)!.state as { deleted: boolean }).deleted, true);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});
