import { inspectReactions } from "../src/reactions/inspection.js";
import { DurableContext } from "../src/context/store.js";
import type { TestContextRegistry } from "../src/testing/context.js";
import { deliveriesOf, workStatus, targetPath } from "../src/reactions/state.js";
import { ProcessingSnapshot, type RecoveryReply } from "@aster/api-contracts";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem } from "@aster/actor";
import { ConfigProvider, Deferred, Effect, Layer, Schema, Stream } from "effect";
import {
  DecisionError,
  ContextRegistry,
  GoalSettings,
  contextView,
  defineContext,
  type StoredContext,
  type ContextSnapshot,
} from "../src/index.js";
import { makeContextRegistry } from "../src/testing/context.js";
import { SystemOneActor } from "../src/reactions/actor.js";
import { ReactionPolicy, ReactionFailure, makeReactionPolicy } from "../src/reactions/policy.js";
import {
  ReactionSnapshot,
  type ReactionWork,
  type FrozenReaction,
  type ReactionDeliveryInput,
  type ReactionPlan,
} from "../src/reactions/state.js";

const sourceDefinition = defineContext({
  changes: "durable-state",
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
    requestId: `deliver-${work.event.id}`,
    causationId: work.event.id,
    source: "/system-one",
    target: "/signals/review",
    version: 1,
    sourceContext: work.event.record,
  },
});
const matched = (command: ReactionDeliveryInput): ReactionPlan[number] => ({
  target: command.input.target,
  result: {
    _tag: "Matched",
    reason: "Test evidence",
    delivery: { command, status: "pending", attempts: 0 },
  },
});
const unmatched = (work: FrozenReaction): ReactionPlan =>
  work.targets
    .filter(({ result }) => result._tag === "Pending")
    .map(({ input }) => ({
      target: targetPath(input),
      result: { _tag: "NotMatched", reason: "Unrelated test evidence" },
    }));
const completed = (record: ContextSnapshot) =>
  record.path === "/system-one" &&
  Schema.decodeUnknownSync(ReactionSnapshot)(record.state).work.length > 0 &&
  Schema.decodeUnknownSync(ReactionSnapshot)(record.state).work.every(
    (work) => workStatus(work) === "completed",
  );
const storeFor = (records: Map<string, StoredContext>) => ({
  loadAll: () => [...records.values()],
  save: (record: StoredContext) => {
    records.set(record.snapshot.path, structuredClone(record));
  },
});
const candidateDefinition = defineContext({
  state: Schema.ObjectKeyword,
  message: Schema.Never,
  view: contextView({ state: Schema.ObjectKeyword }),
});
const layerFor = (
  registry: TestContextRegistry,
  policy: ReactionPolicy["Service"],
  slugs = ["review"],
) =>
  Layer.mergeAll(
    Layer.succeed(ContextRegistry, registry),
    Layer.succeed(DurableContext, registry.backend),
    Layer.succeed(GoalSettings, { definitions: [] }),
    Layer.succeed(ReactionPolicy, policy),
    Layer.effectDiscard(
      Effect.forEach(slugs, (slug) =>
        Effect.gen(function* () {
          const path = `/signals/${slug}`;
          yield* registry.register(path, candidateDefinition);
          if (!registry.get(path))
            yield* registry.commit(
              {
                path,
                description: slug,
                messages: [],
                state: {
                  status: "active",
                  version: 1,
                  trigger: { _tag: "Context", when: slug },
                  task: { _tag: "Goal", target: "/goals/personal", text: "Review" },
                },
              },
              { expectedRevision: 0 },
            );
        }),
      ),
    ),
  );

test("System One coalesces retained queued versions without source Actors or live change replay", async () => {
  const records = new Map<string, StoredContext>();
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
                    assert.equal(work.event.record.revision, work.event.record.revision);
                    seen.push((work.event.record.state as { summary: string }).summary);
                    return unmatched(work);
                  }),
                deliver: () => Effect.die(new Error("No decisions to deliver")),
              }),
            ),
          );
          const actor = yield* system.spawn("system-one", SystemOneActor);
          yield* actor.awaitStarted;
          if (!restart)
            yield* changes.pipe(
              Stream.filter((change) => completed(change.record)),
              Stream.take(1),
              Stream.runDrain,
            );
          yield* actor.tell({ _tag: "Ingest", events: [] });
          yield* actor.awaitStarted;
          assert.deepEqual(seen, ["Second evidence"]);
          assert.equal(
            Schema.decodeUnknownSync(ReactionSnapshot)(registry.get("/system-one")!.state).work
              .length,
            1,
          );
          assert.equal("events" in registry.get("/system-one")!, false);
          const view = JSON.stringify(registry.views.project(registry.get("/system-one")!));
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
  const records = new Map<string, StoredContext>();
  let loseDecisionAck = true;
  let planned = 0;
  let delivered = 0;
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry({
          ...storeFor(records),
          save: (record) => {
            records.set(record.snapshot.path, structuredClone(record));
            if (
              record.snapshot.path === "/system-one" &&
              loseDecisionAck &&
              workStatus(
                Schema.decodeUnknownSync(ReactionSnapshot)(record.snapshot.state).work[0]!,
              ) === "ready"
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
                  return [matched(proposed(work))];
                }),
              deliver: (command) =>
                Effect.sync(() => {
                  delivered++;
                  const durable = Schema.decodeUnknownSync(ReactionSnapshot)(
                    records.get("/system-one")!.snapshot.state,
                  ).work[0]!;
                  assert.equal(deliveriesOf(durable)[0]?.status, "sending");
                  assert.equal(deliveriesOf(durable)[0]?.attempts, 1);
                  assert.deepEqual(deliveriesOf(durable)[0]?.command, command);
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
  const records = new Map<string, StoredContext>();
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
                    return [matched(proposed(work))];
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
                change.record.path === "/system-one" &&
                (restart
                  ? completed(change.record)
                  : Schema.decodeUnknownSync(ReactionSnapshot)(change.record.state).work.some(
                      (work) => deliveriesOf(work)[0]?.status === "unknown",
                    )),
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
  const records = new Map<string, StoredContext>();
  let original: FrozenReaction | undefined;
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
                    if (work.event.record.revision === 1) {
                      assert.deepEqual(work.targets, original!.targets);
                      assert.deepEqual(work.event, original!.event);
                    }
                    return unmatched(work);
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
              Schema.decodeUnknownSync(ReactionSnapshot)(registry.get("/system-one")!.state).work
                .length,
              2,
            );
          }
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
  }
});

test("System One rejects corrupted recovered work before any planning or delivery", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const registry = yield* makeContextRegistry();
      yield* registry.register(source.path, sourceDefinition);
      yield* registry.commit(source, { expectedRevision: 0 });
      const event = registry.backend.journal()[0]!;
      const work: ReactionWork = { event, status: "frozen", targets: [] };
      const target = {
        input: { _tag: "Signal", slug: "review", when: "Review", version: 1 },
        result: { _tag: "Pending" },
      };
      for (const invalid of [
        [work, work],
        [{ ...work, event: { ...event, id: "forged" } }],
        [{ ...work, targets: [{ ...target, input: { ...target.input, version: 0 } }] }],
        [{ ...work, targets: [{ input: { _tag: "Goal" }, result: { _tag: "Pending" } }] }],
        [{ ...work, targets: [target, target] }],
        [
          {
            ...work,
            targets: [
              {
                ...target,
                result: {
                  _tag: "Matched",
                  reason: "Relevant",
                  delivery: {
                    command: proposed(work),
                    status: "delivered",
                    attempts: 1,
                    receipt: { requestId: "other", revision: 2 },
                  },
                },
              },
            ],
          },
        ],
      ]) {
        const recovered = yield* makeContextRegistry({
          loadAll: () => [
            {
              snapshot: {
                path: "/system-one",
                description: "Reactions",
                revision: 1,
                state: {
                  work: invalid,
                  sourceRevisions: { [source.path]: event.record.revision },
                  recoveryReceipts: [],
                },
                messages: [],
              },
              events: [],
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
  const records = new Map<string, StoredContext>();
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
                plan: (work) => Effect.succeed([matched(proposed(work))]),
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
          yield* actor.awaitStarted;
          if (restart < 3) {
            yield* Deferred.await(entered);
            const delivery = deliveriesOf(
              Schema.decodeUnknownSync(ReactionSnapshot)(registry.get("/system-one")!.state)
                .work[0]!,
            )[0]!;
            assert.equal(delivery.status, "sending");
            assert.equal(delivery.attempts, restart + 1);
          } else {
            const work = Schema.decodeUnknownSync(ReactionSnapshot)(
              registry.get("/system-one")!.state,
            ).work[0]!;
            assert.equal(delivered, 3);
            assert.equal(deliveriesOf(work)[0]!.status, "unknown");
            assert.equal(deliveriesOf(work)[0]!.attempts, 3);
            const input = {
              _tag: "RetryReactionDelivery" as const,
              requestId: "operator-retry",
              expectedRevision: registry.get("/system-one")!.revision!,
              workId: work.event.id,
              deliveryId: deliveriesOf(work)[0].command.input.requestId,
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
        const admitted: FrozenReaction[] = [];
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            layerFor(registry, {
              plan: (work) =>
                Effect.suspend(() => {
                  admitted.push(work);
                  return admitted.length === 1
                    ? Effect.fail(new ReactionFailure({ message: "Model unavailable" }))
                    : Effect.succeed(unmatched(work));
                }),
              deliver: () => Effect.die(new Error("No delivery proposed")),
            }),
          ),
        );
        const changes = yield* registry.subscribe;
        const actor = yield* system.spawn("system-one", SystemOneActor);
        yield* actor.awaitStarted;
        const state = () =>
          Schema.decodeUnknownSync(ReactionSnapshot)(registry.get("/system-one")!.state);
        if (workStatus(state().work[0]!) !== "failed")
          yield* changes.pipe(
            Stream.filter(() => workStatus(state().work[0]!) === "failed"),
            Stream.take(1),
            Stream.runDrain,
          );
        yield* registry.commit(
          { ...source, state: { summary: "New live evidence" } },
          { expectedRevision: 1, mode: "bootstrap" },
        );
        const input = {
          _tag: "RetryScreening" as const,
          requestId: "retry-screening",
          workId: state().work[0].event.id,
          expectedRevision: registry.get("/system-one")!.revision!,
        };
        const receipt = yield* actor.ask<RecoveryReply>((replyTo) => ({
          _tag: "Recover",
          input,
          replyTo,
        }));
        assert.equal(receipt._tag, "Accepted");
        if (workStatus(state().work[0]!) !== "completed")
          yield* changes.pipe(
            Stream.filter(() => workStatus(state().work[0]!) === "completed"),
            Stream.take(1),
            Stream.runDrain,
          );
        assert.deepEqual(admitted[1].targets, admitted[0].targets);
        assert.equal("error" in state().work[0], false);
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

for (const retryMatched of [true, false])
  test(`partial screening survives restart and retries only failed targets: ${retryMatched ? "Matched" : "NotMatched"}`, async () => {
    const records = new Map<string, StoredContext>();
    const calls = { signals: 0, healthy: 0, broken: 0, ignored: 0 };
    const delivered: string[] = [];
    for (const restart of [false, true]) {
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const registry = yield* makeContextRegistry(storeFor(records));
            const definition = defineContext({
              state: Schema.ObjectKeyword,
              message: Schema.Never,
              view: contextView({ state: Schema.ObjectKeyword }),
            });
            if (!restart) {
              for (const slug of ["healthy", "broken", "ignored"]) {
                const path = `/goals/${slug}`;
                yield* registry.register(path, definition);
                yield* registry.commit(
                  { path, description: slug, state: { status: "active" }, messages: [] },
                  { expectedRevision: 0 },
                );
              }
              yield* registry.register("/signals/review", definition);
              yield* registry.commit(
                {
                  path: "/signals/review",
                  description: "Review",
                  messages: [],
                  state: {
                    status: "active",
                    version: 1,
                    trigger: { _tag: "Context", when: "Review" },
                    task: { _tag: "Goal", target: "/goals/healthy", text: "Review" },
                  },
                },
                { expectedRevision: 0 },
              );
              yield* registry.register(source.path, sourceDefinition);
              yield* registry.commit(source, { expectedRevision: 0 });
            }
            for (const path of [
              "/goals/healthy",
              "/goals/broken",
              "/goals/ignored",
              "/signals/review",
            ])
              yield* registry.register(path, definition);
            const policy = yield* makeReactionPolicy({
              client: {
                systemOne: (request) =>
                  Effect.suspend(
                    (): ReturnType<
                      import("../src/services/system-one.js").SystemOneClient["systemOne"]
                    > => {
                      if (request.questions.matches) {
                        calls.signals++;
                        return Effect.succeed({
                          answers: { matches: { type: "choice", choice: "yes" } },
                        });
                      }
                      const broken =
                        request.questions.relevance!.instructions.includes('Goal "broken"');
                      const ignored =
                        request.questions.relevance!.instructions.includes('Goal "ignored"');
                      if (broken) calls.broken++;
                      else if (ignored) calls.ignored++;
                      else calls.healthy++;
                      if (broken && !restart)
                        return Effect.fail(new DecisionError({ message: "Unavailable" }));
                      return Effect.succeed({
                        answers: {
                          relevance: {
                            type: "score",
                            score: ignored || (broken && !retryMatched) ? 0 : 9,
                          },
                        },
                      });
                    },
                  ),
              },
            });
            const system = yield* ActorSystem.make().pipe(
              ActorSystem.provide(
                Layer.succeed(ContextRegistry, registry),
                Layer.succeed(DurableContext, registry.backend),
                Layer.succeed(GoalSettings, {
                  definitions: ["healthy", "broken", "ignored"].map((slug) => ({
                    slug,
                    description: slug,
                  })),
                }),
                Layer.succeed(ReactionPolicy, {
                  ...policy,
                  deliver: (command) =>
                    Effect.sync(() => {
                      delivered.push(command.input.target);
                      return {
                        _tag: "Accepted" as const,
                        receipt: { requestId: command.input.requestId, revision: 2 },
                      };
                    }),
                }),
              ),
            );
            const changes = yield* registry.subscribe;
            const actor = yield* system.spawn("system-one", SystemOneActor);
            yield* actor.awaitStarted;
            if (restart) {
              const state = Schema.decodeUnknownSync(ReactionSnapshot)(
                registry.get("/system-one")!.state,
              );
              assert.equal(workStatus(state.work[0]!), "failed");
              const reply = yield* actor.ask<RecoveryReply>((replyTo) => ({
                _tag: "Recover",
                replyTo,
                input: {
                  _tag: "RetryScreening",
                  requestId: "retry-broken",
                  workId: state.work[0]!.event.id,
                  expectedRevision: registry.get("/system-one")!.revision,
                },
              }));
              assert.equal(reply._tag, "Accepted");
            }
            yield* changes.pipe(
              Stream.filter(({ record }) => {
                if (record.path !== "/system-one") return false;
                const work = Schema.decodeUnknownSync(ReactionSnapshot)(record.state).work[0];
                return (
                  work &&
                  workStatus(work) === (restart ? "completed" : "failed") &&
                  deliveriesOf(work).length === (restart && retryMatched ? 3 : 2) &&
                  deliveriesOf(work).every((entry) => entry.status === "delivered")
                );
              }),
              Stream.take(1),
              Stream.runDrain,
            );
            const inspection = yield* inspectReactions(registry, "system-one");
            assert.ok(Schema.is(ProcessingSnapshot)(inspection));
            const matches = inspection.entries.find(
              (entry) => entry.kind === "screening",
            )!.matches!;
            assert.equal(matches.length, 4);
            assert.equal(
              matches.find((match) => match.target === "/goals/ignored")?._tag,
              "NotMatched",
            );
            const broken = matches.find((match) => match.target === "/goals/broken")!;
            assert.equal(
              broken._tag,
              restart ? (retryMatched ? "Matched" : "NotMatched") : "Failed",
            );
            if (broken._tag === "NotMatched") assert.match(broken.reason, /below threshold/);
            const view = registry.reader.get("/system-one")!.state as {
              work: { matches: unknown }[];
            };
            assert.deepEqual(view.work[0]!.matches, matches);
            assert.ok(!JSON.stringify(inspection).includes("First evidence"));
          }),
        ).pipe(Effect.timeout("5 seconds")),
      );
    }
    assert.deepEqual(calls, { signals: 1, healthy: 1, broken: 2, ignored: 1 });
    assert.deepEqual(delivered.sort(), [
      ...(retryMatched ? ["/goals/broken"] : []),
      "/goals/healthy",
      "/signals/review",
    ]);
  });

test("live coalescing retains in-flight work and the latest queued revision across restart", async () => {
  const records = new Map<string, StoredContext>();
  const seen: number[] = [];
  let scans = 0;
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
                    seen.push(work.event.record.revision);
                    if (!restart) {
                      yield* Deferred.succeed(entered, undefined);
                      return yield* Effect.never;
                    }
                    return unmatched(work);
                  }),
                deliver: () => Effect.die("No delivery expected"),
              }),
              Layer.succeed(DurableContext, {
                ...registry.backend,
                journal: () => {
                  scans++;
                  return registry.backend.journal();
                },
              }),
            ),
          );
          yield* (yield* system.spawn("system-one", SystemOneActor)).awaitStarted;
          if (!restart) {
            yield* Deferred.await(entered);
            for (const revision of [2, 3, 4])
              yield* registry.commit(
                { ...source, state: { summary: `Evidence ${revision}` } },
                { expectedRevision: revision - 1 },
              );
            yield* changes.pipe(
              Stream.filter(
                ({ record }) =>
                  record.path === "/system-one" &&
                  Schema.decodeUnknownSync(ReactionSnapshot)(record.state).sourceRevisions[
                    source.path
                  ] === 4,
              ),
              Stream.take(1),
              Stream.runDrain,
            );
            const work = Schema.decodeUnknownSync(ReactionSnapshot)(
              registry.get("/system-one")!.state,
            ).work;
            assert.deepEqual(
              work.map((item) => [workStatus(item), item.event.record.revision]),
              [
                ["planning", 1],
                ["pending", 4],
              ],
            );
          } else {
            yield* changes.pipe(
              Stream.filter(({ record }) => completed(record)),
              Stream.take(1),
              Stream.runDrain,
            );
          }
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
  }
  assert.deepEqual(seen, [1, 1, 4]);
  assert.equal(scans, 2, "Live commits do not scan the journal, including reaction state commits");
});

test("slow delivery does not block matching or other targets and delivery concurrency is bounded", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        yield* registry.register(source.path, sourceDefinition);
        yield* registry.commit(source, { expectedRevision: 0 });
        const slow = yield* Deferred.make<void>();
        const entered = yield* Deferred.make<void>();
        const matchedNext = yield* Deferred.make<void>();
        const fastFinished = yield* Deferred.make<void>();
        const delivered: string[] = [];
        let active = 0,
          peak = 0;
        const changes = yield* registry.subscribe;
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            layerFor(
              registry,
              {
                plan: (work) =>
                  Effect.gen(function* () {
                    if (work.event.record.revision === 2) {
                      yield* Deferred.succeed(matchedNext, undefined);
                      return unmatched(work);
                    }
                    return ["slow", "a", "b", "c", "d"]
                      .map((slug) => {
                        const command = proposed(work);
                        return {
                          ...command,
                          input: {
                            ...command.input,
                            target: `/signals/${slug}`,
                            requestId: `${slug}-${work.event.id}`,
                          },
                        };
                      })
                      .map(matched);
                  }),
                deliver: (command) =>
                  Effect.gen(function* () {
                    active++;
                    peak = Math.max(peak, active);
                    if (command.input.target === "/signals/slow") {
                      yield* Deferred.succeed(entered, undefined);
                      yield* Deferred.await(slow);
                    }
                    delivered.push(command.input.target);
                    if (delivered.length === 4) yield* Deferred.succeed(fastFinished, undefined);
                    return {
                      _tag: "Accepted" as const,
                      receipt: { requestId: command.input.requestId, revision: 1 },
                    };
                  }).pipe(
                    Effect.ensuring(
                      Effect.sync(() => {
                        active--;
                      }),
                    ),
                  ),
              },
              ["slow", "a", "b", "c", "d"],
            ),
          ),
        );
        yield* (yield* system.spawn("system-one", SystemOneActor)).awaitStarted;
        yield* Deferred.await(entered);
        yield* registry.commit(
          { ...source, state: { summary: "New evidence" } },
          { expectedRevision: 1 },
        );
        yield* Deferred.await(matchedNext);
        yield* Deferred.await(fastFinished);
        assert.equal(delivered.includes("/signals/slow"), false);
        assert.ok(peak <= 2);
        yield* Deferred.succeed(slow, undefined);
        yield* changes.pipe(
          Stream.filter(({ record }) => completed(record)),
          Stream.take(1),
          Stream.runDrain,
        );
        assert.equal(delivered.length, 5);
      }),
    ).pipe(
      Effect.provideService(
        ConfigProvider.ConfigProvider,
        ConfigProvider.fromUnknown({ config: { reactions: { deliveryConcurrency: 2 } } }),
      ),
      Effect.timeout("5 seconds"),
    ),
  );
});

test("completed history is bounded and source watermarks prevent replay after pruning", async () => {
  const records = new Map<string, StoredContext>();
  let planned = 0;
  for (const restart of [false, true]) {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* makeContextRegistry(storeFor(records));
          yield* registry.register(source.path, sourceDefinition);
          const changes = yield* registry.subscribe;
          const system = yield* ActorSystem.make().pipe(
            ActorSystem.provide(
              layerFor(registry, {
                plan: (work) =>
                  Effect.sync(() => {
                    planned++;
                    return unmatched(work);
                  }),
                deliver: () => Effect.die("No delivery expected"),
              }),
            ),
          );
          yield* (yield* system.spawn("system-one", SystemOneActor)).awaitStarted;
          if (!restart)
            for (let revision = 1; revision <= 105; revision++) {
              yield* registry.commit(
                { ...source, state: { summary: String(revision) } },
                { expectedRevision: revision - 1 },
              );
              yield* changes.pipe(
                Stream.filter(
                  ({ record }) =>
                    record.path === "/system-one" &&
                    Schema.decodeUnknownSync(ReactionSnapshot)(record.state).work.some(
                      (item) =>
                        item.event.record.revision === revision && workStatus(item) === "completed",
                    ),
                ),
                Stream.take(1),
                Stream.runDrain,
              );
            }
          const state = Schema.decodeUnknownSync(ReactionSnapshot)(
            registry.get("/system-one")!.state,
          );
          assert.equal(state.work.length, 100);
          assert.equal(state.sourceRevisions[source.path], 105);
          assert.ok(state.work.every((item) => workStatus(item) === "completed"));
          assert.equal(planned, 105);
        }),
      ).pipe(Effect.timeout("10 seconds")),
    );
  }
});

test("delivery receipts remain visible during matching retry and survive interrupted retry recovery", async () => {
  const records = new Map<string, StoredContext>();
  let plans = 0;
  let deliveries = 0;
  for (const restart of [false, true]) {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* makeContextRegistry(storeFor(records));
          yield* registry.register(source.path, sourceDefinition);
          if (!restart) yield* registry.commit(source, { expectedRevision: 0 });
          const delivering = yield* Deferred.make<void>();
          const finishDelivery = yield* Deferred.make<void>();
          const retrying = yield* Deferred.make<void>();
          const changes = yield* registry.subscribe;
          const system = yield* ActorSystem.make().pipe(
            ActorSystem.provide(
              layerFor(
                registry,
                {
                  plan: (work) =>
                    Effect.gen(function* () {
                      plans++;
                      if (plans === 1) {
                        const command = proposed(work);
                        return [
                          matched({
                            ...command,
                            input: { ...command.input, target: "/signals/slow" },
                          }),
                          {
                            target: "/signals/broken",
                            result: { _tag: "Failed" as const, error: "Model unavailable" },
                          },
                        ];
                      }
                      assert.deepEqual(
                        work.targets
                          .filter(({ result }) => result._tag === "Pending")
                          .map(({ input }) => input.slug),
                        ["broken"],
                      );
                      assert.equal(
                        deliveriesOf(work)[0]!.status,
                        restart ? "delivered" : "sending",
                      );
                      if (!restart) {
                        yield* Deferred.succeed(retrying, undefined);
                        return yield* Effect.never;
                      }
                      return unmatched(work);
                    }),
                  deliver: (command) =>
                    Effect.gen(function* () {
                      deliveries++;
                      yield* Deferred.succeed(delivering, undefined);
                      yield* Deferred.await(finishDelivery);
                      return {
                        _tag: "Accepted",
                        receipt: { requestId: command.input.requestId, revision: 2 },
                      };
                    }),
                },
                ["slow", "broken"],
              ),
            ),
          );
          const actor = yield* system.spawn("system-one", SystemOneActor);
          yield* actor.awaitStarted;
          if (!restart) {
            yield* Deferred.await(delivering);
            const current = registry.get("/system-one")!;
            const work = Schema.decodeUnknownSync(ReactionSnapshot)(current.state).work[0]!;
            const reply = yield* actor.ask<RecoveryReply>((replyTo) => ({
              _tag: "Recover",
              replyTo,
              input: {
                _tag: "RetryScreening",
                workId: work.event.id,
                requestId: "retry-while-delivering",
                expectedRevision: current.revision,
              },
            }));
            assert.equal(reply._tag, "Accepted");
            yield* Deferred.await(retrying);
            yield* Deferred.succeed(finishDelivery, undefined);
            yield* changes.pipe(
              Stream.filter(({ record }) => {
                if (record.path !== "/system-one") return false;
                const work = Schema.decodeUnknownSync(ReactionSnapshot)(record.state).work[0];
                return work !== undefined && deliveriesOf(work)[0]?.status === "delivered";
              }),
              Stream.take(1),
              Stream.runDrain,
            );
            const inspection = yield* inspectReactions(registry, "system-one");
            assert.equal(
              inspection.entries.find((entry) => entry.kind === "screening")!.status,
              "planning",
            );
            assert.equal(
              inspection.entries.find((entry) => entry.kind === "screening")!.attempts,
              undefined,
            );
            assert.equal(
              inspection.entries.find((entry) => entry.kind === "reaction-delivery")!.status,
              "delivered",
            );
          } else {
            yield* changes.pipe(
              Stream.filter(({ record }) => completed(record)),
              Stream.take(1),
              Stream.runDrain,
            );
            const state = Schema.decodeUnknownSync(ReactionSnapshot)(
              registry.get("/system-one")!.state,
            );
            assert.equal(deliveriesOf(state.work[0]!)[0]!.status, "delivered");
          }
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
  }
  assert.equal(plans, 3);
  assert.equal(deliveries, 1);
});

test("an event without eligible targets completes and is not reconsidered on restart", async () => {
  const records = new Map<string, StoredContext>();
  for (const restart of [false, true]) {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* makeContextRegistry(storeFor(records));
          yield* registry.register(source.path, sourceDefinition);
          if (!restart) yield* registry.commit(source, { expectedRevision: 0 });
          const policy = yield* makeReactionPolicy({
            client: { systemOne: () => Effect.die(new Error("No candidate may call the model")) },
          });
          const changes = yield* registry.subscribe;
          const system = yield* ActorSystem.make().pipe(
            ActorSystem.provide(layerFor(registry, policy, [])),
          );
          yield* (yield* system.spawn("system-one", SystemOneActor)).awaitStarted;
          if (!completed(registry.get("/system-one")!))
            yield* changes.pipe(
              Stream.filter(({ record }) => completed(record)),
              Stream.take(1),
              Stream.runDrain,
            );
          assert.equal((yield* inspectReactions(registry, "system-one")).entries.length, 1);
          assert.equal(
            Schema.decodeUnknownSync(ReactionSnapshot)(registry.get("/system-one")!.state).work
              .length,
            1,
          );
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
  }
});
