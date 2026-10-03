import type { RecoveryReply } from "@aster/api-contracts";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem } from "@aster/actor";
import { Deferred, Effect, Layer, Schema, Stream } from "effect";
import {
  ContextRegistry,
  GoalSettings,
  contextView,
  defineContext,
  makeContextRegistry,
  type ContextRecord,
} from "../src/index.js";
import { SystemOneActor } from "../src/context/reaction-actor.js";
import { ReactionPolicy, ReactionFailure } from "../src/context/reaction-policy.js";
import {
  ReactionState,
  type ReactionWork,
  type ReactionDeliveryInput,
} from "../src/context/reaction-state.js";

const sourceDefinition = defineContext({
  identity: "Source",
  signalSource: true,
  state: Schema.Struct({ summary: Schema.String }),
  message: Schema.String,
  view: contextView({ state: Schema.Struct({ summary: Schema.String }), message: Schema.String }),
});
const source = {
  path: "/source",
  description: "Source",
  state: { summary: "First evidence" },
  messages: [],
};
const proposed = (work: ReactionWork): Extract<ReactionDeliveryInput, { _tag: "Signal" }> => ({
  _tag: "Signal",
  input: {
    requestId: `deliver-${work.event.requestId}`,
    causationId: work.event.requestId,
    source: "/system-one",
    target: "/signals/review",
    expectedRevision: 1,
    sourceContext: work.event.record,
  },
});
const completed = (record: ContextRecord) =>
  record.path === "/system-one" &&
  Schema.decodeUnknownSync(ReactionState)(record.state).work.length > 0 &&
  Schema.decodeUnknownSync(ReactionState)(record.state).work.every(
    (work) => work.status === "completed",
  );
const storeFor = (records: Map<string, ContextRecord>) => ({
  loadAll: () => [...records.values()],
  save: (record: ContextRecord) => {
    records.set(record.path, structuredClone(record));
  },
});
const layerFor = (
  registry: ContextRegistry["Service"],
  policy: Omit<ReactionPolicy["Service"], "bind">,
) =>
  Layer.mergeAll(
    Layer.succeed(ContextRegistry, registry),
    Layer.succeed(GoalSettings, { definitions: [] }),
    Layer.succeed(ReactionPolicy, { ...policy, bind: () => Effect.void }),
  );

test("System One recovers every retained source version without source Actors or live change replay", async () => {
  const records = new Map<string, ContextRecord>();
  await Effect.runPromise(
    Effect.gen(function* () {
      const registry = yield* makeContextRegistry(storeFor(records));
      yield* registry.register(source.path, sourceDefinition);
      yield* registry.commit(source, { expectedRevision: 0 });
      yield* registry.commit(
        { ...source, state: { summary: "Second evidence" } },
        { expectedRevision: 1 },
      );
    }),
  );
  const seen: string[] = [];
  for (const restart of [false, true]) {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* makeContextRegistry(storeFor(records));
          const changes = yield* registry.subscribe;
          const system = yield* ActorSystem.make().pipe(
            ActorSystem.provide(
              layerFor(registry, {
                plan: (work) =>
                  Effect.sync(() => {
                    assert.equal(work.snapshot[source.path]?.revision, work.event.revision);
                    seen.push((work.event.record.state as { summary: string }).summary);
                    return { screenings: [], commands: [] };
                  }),
                deliver: () => Effect.die(new Error("No decisions to deliver")),
              }),
            ),
          );
          const actor = yield* system.spawn("system-one", SystemOneActor);
          yield* actor.ask<void>((replyTo) => ({ _tag: "Ready", replyTo }));
          if (!restart)
            yield* changes.pipe(
              Stream.filter((change) => completed(change.record)),
              Stream.take(1),
              Stream.runDrain,
            );
          yield* actor.tell({ _tag: "Wake" });
          yield* actor.ask<void>((replyTo) => ({ _tag: "Ready", replyTo }));
          assert.deepEqual(seen, ["First evidence", "Second evidence"]);
          assert.equal(
            Schema.decodeUnknownSync(ReactionState)(registry.get("/system-one")!.state).work.length,
            2,
          );
          assert.equal(registry.get("/system-one")!.reactionEvents, undefined);
          const view = JSON.stringify(registry.project(registry.get("/system-one")!));
          assert.equal(
            view.includes("First evidence"),
            false,
            "Public diagnostics omit frozen evidence/catalogues",
          );
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
  }
});

test("a persisted decision survives lost commit acknowledgement without re-screening or duplicate delivery", async () => {
  const records = new Map<string, ContextRecord>();
  let loseDecisionAck = true;
  let planned = 0;
  let delivered = 0;
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry({
          ...storeFor(records),
          save: (record) => {
            records.set(record.path, structuredClone(record));
            if (
              record.path === "/system-one" &&
              loseDecisionAck &&
              Schema.decodeUnknownSync(ReactionState)(record.state).work[0]?.status === "ready"
            ) {
              loseDecisionAck = false;
              throw new Error("Decision committed; response lost");
            }
          },
        });
        yield* registry.register(source.path, sourceDefinition);
        yield* registry.commit(source, { expectedRevision: 0 });
        const changes = yield* registry.subscribe;
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            layerFor(registry, {
              plan: (work) =>
                Effect.sync(() => {
                  planned++;
                  return { screenings: [], commands: [proposed(work)] };
                }),
              deliver: (command) =>
                Effect.sync(() => {
                  delivered++;
                  const durable = Schema.decodeUnknownSync(ReactionState)(
                    records.get("/system-one")!.state,
                  ).work[0]!;
                  assert.equal(durable.deliveries?.[0]?.status, "sending");
                  assert.equal(durable.deliveries?.[0]?.attempts, 1);
                  assert.deepEqual(durable.deliveries?.[0]?.command, command);
                  return {
                    _tag: "Accepted",
                    receipt: { requestId: command.input.requestId, revision: 2 },
                  };
                }),
            }),
          ),
        );
        yield* system.spawn("system-one", SystemOneActor);
        yield* changes.pipe(
          Stream.filter((change) => completed(change.record)),
          Stream.take(1),
          Stream.runDrain,
        );
        assert.equal(planned, 1);
        assert.equal(delivered, 1);
        assert.equal(loseDecisionAck, false);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("lost receiver acknowledgement replays the frozen decision after restart with one receiver effect", async () => {
  const records = new Map<string, ContextRecord>();
  const receipts = new Map<string, ReactionDeliveryInput>();
  let planned = 0;
  let delivered = 0;
  for (const restart of [false, true]) {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* makeContextRegistry(storeFor(records));
          if (!restart) {
            yield* registry.register(source.path, sourceDefinition);
            yield* registry.commit(source, { expectedRevision: 0 });
          }
          const changes = yield* registry.subscribe;
          const system = yield* ActorSystem.make().pipe(
            ActorSystem.provide(
              layerFor(registry, {
                plan: (work) =>
                  Effect.sync(() => {
                    planned++;
                    return { screenings: [], commands: [proposed(work)] };
                  }),
                deliver: (command) =>
                  Effect.gen(function* () {
                    delivered++;
                    const previous = receipts.get(command.input.requestId);
                    if (previous) assert.deepEqual(command, previous);
                    else receipts.set(command.input.requestId, structuredClone(command));
                    if (!restart)
                      return yield* new ReactionFailure({
                        message: "Receiver persisted; acknowledgement lost",
                      });
                    return {
                      _tag: "Accepted",
                      receipt: { requestId: command.input.requestId, revision: 2 },
                    };
                  }),
              }),
            ),
          );
          yield* system.spawn("system-one", SystemOneActor);
          yield* changes.pipe(
            Stream.filter(
              (change) =>
                change.path === "/system-one" &&
                (restart
                  ? completed(change.record)
                  : Schema.decodeUnknownSync(ReactionState)(change.record.state).work[0]
                      ?.deliveries?.[0]?.status === "unknown"),
            ),
            Stream.take(1),
            Stream.runDrain,
          );
          assert.equal(planned, 1);
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
  }
  assert.equal(delivered, 2);
  assert.equal(receipts.size, 1);
});

test("interrupted planning reuses its admitted source and catalogue despite later source updates", async () => {
  const records = new Map<string, ContextRecord>();
  let original: ReactionWork | undefined;
  for (const restart of [false, true]) {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* makeContextRegistry(storeFor(records));
          yield* registry.register(source.path, sourceDefinition);
          if (!restart) yield* registry.commit(source, { expectedRevision: 0 });
          const entered = yield* Deferred.make<void>();
          const changes = yield* registry.subscribe;
          const system = yield* ActorSystem.make().pipe(
            ActorSystem.provide(
              layerFor(registry, {
                plan: (work) =>
                  Effect.gen(function* () {
                    if (!restart) {
                      original = work;
                      yield* Deferred.succeed(entered, undefined);
                      return yield* Effect.never;
                    }
                    if (work.event.revision === 1) {
                      assert.deepEqual(work.snapshot, original!.snapshot);
                      assert.deepEqual(work.goals, original!.goals);
                      assert.deepEqual(work.event, original!.event);
                      assert.equal(work.attempts, 2);
                    }
                    return { screenings: [], commands: [] };
                  }),
                deliver: () => Effect.die(new Error("No delivery")),
              }),
            ),
          );
          yield* system.spawn("system-one", SystemOneActor);
          if (!restart) {
            yield* Deferred.await(entered);
            yield* registry.commit(
              { ...source, state: { summary: "New evidence" } },
              { expectedRevision: 1 },
            );
          } else {
            yield* changes.pipe(
              Stream.filter((change) => completed(change.record)),
              Stream.take(1),
              Stream.runDrain,
            );
            assert.equal(
              Schema.decodeUnknownSync(ReactionState)(registry.get("/system-one")!.state).work
                .length,
              2,
            );
          }
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
  }
});

test("queued sources freeze target revisions when screening starts after the preceding delivery", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        yield* registry.register(source.path, sourceDefinition);
        yield* registry.register(
          "/signals/review",
          defineContext({
            identity: "Target",
            state: Schema.Struct({ count: Schema.Number }),
            message: Schema.Never,
          }),
        );
        yield* registry.commit(
          { path: "/signals/review", description: "Target", state: { count: 0 }, messages: [] },
          { expectedRevision: 0 },
        );
        yield* registry.commit(source, { expectedRevision: 0 });
        yield* registry.commit(
          { ...source, state: { summary: "Second" } },
          { expectedRevision: 1 },
        );
        const revisions: number[] = [];
        const changes = yield* registry.subscribe;
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            layerFor(registry, {
              plan: (work) =>
                Effect.sync(() => {
                  const command = proposed(work);
                  return {
                    screenings: [],
                    commands: [
                      {
                        ...command,
                        input: {
                          ...command.input,
                          expectedRevision: work.snapshot["/signals/review"]!.revision!,
                        },
                      },
                    ],
                  };
                }),
              deliver: (command) =>
                Effect.gen(function* () {
                  const current = registry.get("/signals/review")!;
                  assert.equal(command.input.expectedRevision, current.revision);
                  revisions.push(command.input.expectedRevision);
                  const next = yield* registry
                    .commit(
                      { ...current, state: { count: current.revision! } },
                      { expectedRevision: command.input.expectedRevision },
                    )
                    .pipe(Effect.orDie);
                  return {
                    _tag: "Accepted",
                    receipt: { requestId: command.input.requestId, revision: next.revision! },
                  };
                }),
            }),
          ),
        );
        yield* system.spawn("system-one", SystemOneActor);
        yield* changes.pipe(
          Stream.filter((change) => completed(change.record)),
          Stream.take(1),
          Stream.runDrain,
        );
        assert.deepEqual(revisions, [1, 2]);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("System One rejects corrupted recovered work before any planning or delivery", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const registry = yield* makeContextRegistry();
      yield* registry.register(source.path, sourceDefinition);
      const committed = yield* registry.commit(source, { expectedRevision: 0 });
      const event = committed.reactionEvents![0]!;
      const work: ReactionWork = {
        event,
        snapshot: { [source.path]: event.record },
        goals: [],
        admittedAt: event.createdAt,
        status: "completed",
        attempts: 1,
        screenings: [],
        deliveries: [],
      };
      for (const invalid of [
        [work, work],
        [{ ...work, event: { ...event, requestId: "forged" } }],
        [{ ...work, snapshot: { [source.path]: { ...event.record, revision: 100 } } }],
        [{ ...work, admittedAt: "invalid" }],
        [
          {
            ...work,
            deliveries: [
              {
                command: proposed(work),
                status: "delivered" as const,
                attempts: 1,
                receipt: { requestId: "other", revision: 2 },
              },
            ],
          },
        ],
      ]) {
        const recovered = yield* makeContextRegistry({
          loadAll: () => [
            {
              path: "/system-one",
              description: "Reactions",
              revision: 1,
              state: { work: invalid },
              messages: [],
            },
          ],
          save: () => assert.fail("Corrupted recovery must never rewrite stored state"),
        });
        const result = yield* recovered
          .register("/system-one", SystemOneActor.context)
          .pipe(Effect.exit);
        assert.equal(result._tag, "Failure");
      }
    }),
  );
});

test("interrupted delivery attempts stay bounded across restarts and explicit retry preserves the command", async () => {
  const records = new Map<string, ContextRecord>();
  let delivered = 0;
  let original: ReactionDeliveryInput | undefined;
  for (let restart = 0; restart < 4; restart++) {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* makeContextRegistry(storeFor(records));
          if (restart === 0) {
            yield* registry.register(source.path, sourceDefinition);
            yield* registry.commit(source, { expectedRevision: 0 });
          }
          const entered = yield* Deferred.make<void>();
          const changes = yield* registry.subscribe;
          const system = yield* ActorSystem.make().pipe(
            ActorSystem.provide(
              layerFor(registry, {
                plan: (work) => Effect.succeed({ screenings: [], commands: [proposed(work)] }),
                deliver: (command) =>
                  Effect.gen(function* () {
                    delivered++;
                    if (original) assert.deepEqual(command, original);
                    original = command;
                    yield* Deferred.succeed(entered, undefined);
                    if (restart < 3) return yield* Effect.never;
                    return {
                      _tag: "Accepted",
                      receipt: { requestId: command.input.requestId, revision: 2 },
                    };
                  }),
              }),
            ),
          );
          const actor = yield* system.spawn("system-one", SystemOneActor);
          yield* actor.ask<void>((replyTo) => ({ _tag: "Ready", replyTo }));
          if (restart < 3) {
            yield* Deferred.await(entered);
            const delivery = Schema.decodeUnknownSync(ReactionState)(
              registry.get("/system-one")!.state,
            ).work[0]!.deliveries![0]!;
            assert.equal(delivery.status, "sending");
            assert.equal(delivery.attempts, restart + 1);
          } else {
            const work = Schema.decodeUnknownSync(ReactionState)(registry.get("/system-one")!.state)
              .work[0]!;
            assert.equal(delivered, 3);
            assert.equal(work.deliveries![0]!.status, "unknown");
            assert.equal(work.deliveries![0]!.attempts, 3);
            const input = {
              _tag: "RetryReactionDelivery" as const,
              requestId: "operator-retry",
              expectedRevision: registry.get("/system-one")!.revision!,
              workId: work.event.requestId,
              deliveryId: work.deliveries![0].command.input.requestId,
            };
            const reply = yield* actor.ask<RecoveryReply>((replyTo) => ({
              _tag: "Recover",
              input,
              replyTo,
            }));
            assert.equal(reply._tag, "Accepted");
            assert.deepEqual(
              yield* actor.ask<RecoveryReply>((replyTo) => ({ _tag: "Recover", input, replyTo })),
              reply,
            );
            const conflict = yield* actor.ask<RecoveryReply>((replyTo) => ({
              _tag: "Recover",
              input: { ...input, expectedRevision: input.expectedRevision + 1 },
              replyTo,
            }));
            assert.equal(conflict._tag, "Rejected");
            yield* changes.pipe(
              Stream.filter((change) => completed(change.record)),
              Stream.take(1),
              Stream.runDrain,
            );
            assert.equal(delivered, 4);
          }
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
  }
});

test("operator retry of failed screening retains frozen evidence and cannot rescreen a completed decision", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        yield* registry.register(source.path, sourceDefinition);
        yield* registry.commit(source, { expectedRevision: 0 });
        const admitted: ReactionWork[] = [];
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            layerFor(registry, {
              plan: (work) =>
                Effect.suspend(() => {
                  admitted.push(work);
                  return admitted.length === 1
                    ? Effect.fail(new ReactionFailure({ message: "Model unavailable" }))
                    : Effect.succeed({ screenings: [], commands: [] });
                }),
              deliver: () => Effect.die(new Error("No delivery proposed")),
            }),
          ),
        );
        const changes = yield* registry.subscribe;
        const actor = yield* system.spawn("system-one", SystemOneActor);
        yield* actor.ask<void>((replyTo) => ({ _tag: "Ready", replyTo }));
        const state = () =>
          Schema.decodeUnknownSync(ReactionState)(registry.get("/system-one")!.state);
        if (state().work[0]?.status !== "failed")
          yield* changes.pipe(
            Stream.filter(() => state().work[0]?.status === "failed"),
            Stream.take(1),
            Stream.runDrain,
          );
        yield* registry.commit(
          { ...source, state: { summary: "New live evidence" } },
          { expectedRevision: 1 },
        );
        const input = {
          _tag: "RetryScreening" as const,
          requestId: "retry-screening",
          workId: state().work[0].event.requestId,
          expectedRevision: registry.get("/system-one")!.revision!,
        };
        const receipt = yield* actor.ask<RecoveryReply>((replyTo) => ({
          _tag: "Recover",
          input,
          replyTo,
        }));
        assert.equal(receipt._tag, "Accepted");
        if (state().work[0].status !== "completed")
          yield* changes.pipe(
            Stream.filter(() => state().work[0].status === "completed"),
            Stream.take(1),
            Stream.runDrain,
          );
        assert.deepEqual(admitted[1].snapshot, admitted[0].snapshot);
        assert.deepEqual(admitted[1].goals, admitted[0].goals);
        assert.equal(admitted[1].attempts, 2);
        assert.equal(state().work[0].error, undefined);
        assert.deepEqual(
          yield* actor.ask<RecoveryReply>((replyTo) => ({ _tag: "Recover", input, replyTo })),
          receipt,
        );
        assert.equal(
          (yield* actor.ask<RecoveryReply>((replyTo) => ({
            _tag: "Recover",
            input: {
              ...input,
              requestId: "new-retry",
              expectedRevision: registry.get("/system-one")!.revision!,
            },
            replyTo,
          })))._tag,
          "Rejected",
        );
        assert.equal(admitted.length, 2);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});
