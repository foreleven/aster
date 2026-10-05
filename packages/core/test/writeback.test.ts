import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem } from "@aster/actor";
import { Deferred, Effect, Layer, Schema, Stream } from "effect";
import {
  ApprovalQueueActor,
  ChannelWrites,
  ChannelWriteError,
  ContextRegistry,
  ExternalAgents,
  SignalDefinitions,
  SignalRootActor,
  approvalEntries,
  writebackApprovalId,
  WritebackOperation,
  type ContextRecord,
  SignalDefinition,
} from "../src/index.js";
import { makeContextRegistry } from "../src/testing/context.js";
import { RunState } from "../src/tasks/run-state.js";
import { runActorPath } from "../src/tasks/address.js";
import { preparationLayer, fakeAgent } from "./fixtures.js";

const definition: SignalDefinition = {
  slug: "publish",
  when: "A report is due",
  task: "Draft a report",
  agent: "test",
  mode: "confirm",
  action: { _tag: "PublishResult", channelPath: "/lark/im/chats/oc_test", identity: "user" },
};
const fixture = (options: {
  records: Map<string, ContextRecord>;
  publish: ChannelWrites["Service"]["publish"];
  definition?: SignalDefinition;
  saved?: (record: ContextRecord) => void;
}) =>
  Effect.gen(function* () {
    const registry = yield* makeContextRegistry({
      loadAll: () => [...options.records.values()],
      save: (record) => {
        options.records.set(record.path, structuredClone(record));
        options.saved?.(record);
      },
    });
    const system = yield* ActorSystem.make().pipe(
      ActorSystem.provide(
        Layer.succeed(ContextRegistry, registry),
        preparationLayer,
        Layer.succeed(ExternalAgents, { test: fakeAgent() }),
        Layer.succeed(SignalDefinitions, [options.definition ?? definition]),
        Layer.succeed(ChannelWrites, { publish: options.publish }),
      ),
    );
    const approvals = yield* system.spawn("approvals", ApprovalQueueActor);
    const signals = yield* system.spawn("signals", SignalRootActor);
    const until = (condition: () => boolean) =>
      Effect.gen(function* () {
        const changes = yield* registry.subscribe;
        if (!condition())
          yield* changes.pipe(
            Stream.filter(() => condition()),
            Stream.take(1),
            Stream.runDrain,
          );
      });
    const record = () =>
      Object.values(registry.snapshot()).find((record) => record.path.includes("/runs/"));
    const state = () => record() && Schema.decodeUnknownSync(RunState)(record()!.state);
    const trigger = signals.tell({
      _tag: "Trigger",
      slug: "publish",
      sourceContext: {
        path: "/evidence",
        description: "Evidence",
        state: { text: "Report facts" },
        messages: [],
      },
    });
    const decide = (id: string, decision: "approve" | "reject") =>
      approvals.ask<{ error?: string }>((replyTo) => ({
        _tag: "Resolve",
        id,
        response: { decision },
        replyTo,
      }));
    const completed = Effect.gen(function* () {
      yield* trigger;
      if ((options.definition ?? definition).mode === "confirm") {
        yield* until(() =>
          approvalEntries(registry).some((entry) => entry.id.endsWith(":confirm")),
        );
        assert.deepEqual(yield* decide(`${record()!.path}:confirm`, "approve"), {});
      }
      yield* until(() => state()?.status === "completed");
    });
    const waiting = until(() =>
      approvalEntries(registry).some((entry) => entry.id.includes(":writeback:")),
    );
    return { registry, system, signals, until, record, state, decide, completed, waiting };
  });

test("Run persists the result and exact writeback before a separate approval; forged commands cannot publish", async () => {
  let calls = 0;
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const records = new Map<string, ContextRecord>();
        const env = yield* fixture({
          records,
          publish: (request, authorization) =>
            Effect.sync(() => {
              calls++;
              const retained = Schema.decodeUnknownSync(RunState)(
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
        const { action: _removed, ...localOnly } = definition;
        const signal = yield* env.system.select("/user/signals/publish").resolve();
        const before = env.registry.get("/signals/publish");
        const unsupported = yield* signal.ask<{ _tag: string }>((replyTo) => ({
          _tag: "Configure",
          definition,
          goal: "review",
          active: true,
          replyTo,
        }));
        assert.equal(unsupported._tag, "Rejected");
        assert.deepEqual(env.registry.get("/signals/publish"), before);
        const configured = yield* signal.ask<{ _tag: string }>((replyTo) => ({
          _tag: "Configure",
          definition: localOnly,
          active: true,
          replyTo,
        }));
        assert.equal(configured._tag, "Accepted");
        assert.equal(
          Schema.decodeUnknownSync(SignalDefinition)(env.registry.get("/signals/publish")!.state)
            .action,
          undefined,
        );
        assert.deepEqual(env.state()!.writeback!.request.action, operation.request.action);
        const id = writebackApprovalId(operation.request);
        const entry = approvalEntries(env.registry).find((entry) => entry.id === id)!;
        assert.match(entry.request.prompt, /oc_test as user/);
        assert.match(entry.request.prompt, /\n\ndone\n\n/);
        const actor = yield* env.system.select(runActorPath(env.record()!.path)).resolve();
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
    let calls = 0;
    for (let restart = 0; restart < 2; restart++) {
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const env = yield* fixture({
              records,
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
            yield* env.signals.ask<void>((replyTo) => ({ _tag: "Ready", replyTo }));
            const signal = yield* env.system.select("/user/signals/publish").resolve();
            yield* signal.tell({ _tag: "Recover" });
            yield* signal.ask<void>((replyTo) => ({ _tag: "Ready", replyTo }));
            const actor = yield* env.system.select(runActorPath(env.record()!.path)).resolve();
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
  const entered = Deferred.makeUnsafe<void>();
  let calls = 0;
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const env = yield* fixture({
          records,
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
          definition: { ...definition, mode: "auto" },
          publish: () => Effect.die("Rejected publication cannot send"),
        });
        yield* env.completed;
        yield* env.waiting;
        yield* env.decide(writebackApprovalId(env.state()!.writeback!.request), "reject");
        yield* env.until(() => env.state()?.writeback?.status === "rejected");
        assert.equal(env.state()!.status, "completed");
        assert.equal(env.state()!.outcomeText, "done");
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
        const actor = yield* env.system.select(runActorPath(env.record()!.path)).resolve();
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
    let calls = 0;
    let lost = false;
    const acknowledgementLost = Deferred.makeUnsafe<void>();
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const env = yield* fixture({
            records,
            saved: (record) => {
              if (
                !lost &&
                record.path.includes("/runs/") &&
                Schema.decodeUnknownSync(RunState)(record.state).writeback?.status === phase
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
            publish: () => Effect.die(new Error("Uncertain submission cannot retry")),
          });
          const status = phase === "sending" ? "unknown" : "published";
          yield* env.until(() => env.state()?.writeback?.status === status);
          yield* env.signals.ask<void>((replyTo) => ({ _tag: "Ready", replyTo }));
          const signal = yield* env.system.select("/user/signals/publish").resolve();
          yield* signal.tell({ _tag: "Recover" });
          yield* signal.ask<void>((replyTo) => ({ _tag: "Ready", replyTo }));
          const actor = yield* env.system.select(runActorPath(env.record()!.path)).resolve();
          yield* actor.tell({ _tag: "Resume", path: env.record()!.path });
          yield* actor.ask<void>((replyTo) => ({ _tag: "Cancel", reason: "barrier", replyTo }));
          assert.equal(calls, phase === "sending" ? 0 : 1);
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
  });
}
