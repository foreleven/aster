import {
  taskExecutionLayer,
  personalReasoningLayer,
  personalDisabled,
} from "./workflow-fixtures.js";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem, ActorTestKit } from "@aster/actor";
import {
  type RecoveryReply,
  BusinessNotification,
  PersonalMessage,
  PersonalState,
} from "@aster/api-contracts";
import { Effect, Layer, Schema, Stream } from "effect";
import {
  ContextRegistry,
  ExternalAgents,
  GoalActor,
  PersonalActions,
  PersonalAgentActor,
  RunRootActor,
  defineContext,
  makeApplicationApi,
  makeMemoryGoalHistory,
  type ContextRecord,
  type PersonalReply,
} from "../src/index.js";
import { makeContextRegistry } from "../src/testing/context.js";
import { NotificationsActor, NotificationState } from "../src/notifications/actor.js";
import { BusinessOutbox } from "../src/notifications/inbox.js";
import { fakeAgent } from "./fixtures.js";
import { goalWorkingState } from "../src/goals/working-state.js";
import { GoalState } from "../src/goals/state.js";

const sourcePath = "/signals/review/runs/one";
const publication = defineContext({
  state: BusinessOutbox,
  message: Schema.Never,
});
const notification = (index: number, remainingAgentTurns = 4): BusinessNotification => ({
  requestId: `notification-${index}`,
  causationId: "user-objective",
  source: sourcePath,
  target: "/personal",
  revision: index,
  createdAt: "2026-10-02T00:00:00.000Z",
  causal: { rootRequestId: "user-objective", remainingAgentTurns },
  kind: "RunResult",
  text: `Result ${index}`,
});
const retainedStore = (records: Map<string, ContextRecord>) => ({
  loadAll: () => [...records.values()],
  save: (record: ContextRecord) => {
    records.set(record.path, structuredClone(record));
  },
});

test("notification recovery rejects malformed deliveries before rewriting durable state", async () => {
  for (const deliveries of [
    [{ input: notification(1), status: "delivered", attempts: 1 }],
    [{ input: notification(1), status: "sending", attempts: 0 }],
    [
      {
        input: notification(1),
        status: "unknown",
        attempts: 1,
        receipt: { requestId: "wrong", revision: 1 },
      },
    ],
    [{ input: { ...notification(1), source: "/credentials" }, status: "pending", attempts: 0 }],
    [{ input: { ...notification(1), createdAt: "invalid" }, status: "pending", attempts: 0 }],
    [1, 1].map(() => ({ input: notification(1), status: "pending", attempts: 0 })),
  ]) {
    await Effect.runPromise(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry({
          loadAll: () => [
            {
              path: "/notifications",
              description: "Notifications",
              revision: 1,
              state: { deliveries },
              messages: [],
            },
          ],
          save: () => assert.fail("Corrupted recovery must not rewrite notification state"),
        });
        const registration = yield* registry
          .register("/notifications", NotificationsActor.context)
          .pipe(Effect.exit);
        assert.equal(registration._tag, "Failure");
      }),
    );
  }
});

test("Goal progress publishes atomically, omits bookkeeping, and reaches Personal after source acknowledgement loss", async () => {
  const records = new Map<string, ContextRecord>();
  const path = "/goals/review";
  let loseAck = true;
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry({
          ...retainedStore(records),
          save: (record) => {
            records.set(record.path, structuredClone(record));
            if (
              record.path === path &&
              Schema.decodeUnknownSync(GoalState)(record.state).status === "completed" &&
              loseAck
            ) {
              loseAck = false;
              throw new Error("Goal and notification committed before acknowledgement loss");
            }
          },
        });
        yield* registry.register(path, GoalActor.context);
        yield* registry.commit(
          {
            path,
            description: "Review",
            messages: [],
            state: {
              slug: "review",
              description: "Review",
              status: "active",
              summary: "",
              progress: "",
              inputs: [],
              historyThrough: 0,
              historyCount: 0,
              causal: { rootRequestId: "review-request", remainingAgentTurns: 1 },
            },
          },
          { expectedRevision: 0 },
        );
        const working = goalWorkingState(
          registry,
          makeMemoryGoalHistory(),
          () => ({ slug: "review", description: "Review" }),
          () => path,
        );
        yield* working.save({ summary: "Compacted context", historyCount: 1 });
        assert.equal(
          Schema.decodeUnknownSync(BusinessOutbox)(registry.get(path)!.state).businessOutbox.length,
          0,
        );
        yield* working.save({ progress: "Reviewed the release evidence." });
        yield* working.save({
          progress: "Reviewed the release evidence.",
          historyCount: 0,
        });
        yield* working.save({ lastError: "One decision needs clarification." });
        yield* working.save({ lastError: "One decision needs clarification." });
        const lost = yield* working
          .save({ status: "completed", lastError: undefined })
          .pipe(Effect.result);
        assert.equal(lost._tag, "Failure");
        assert.equal(loseAck, false);
        const outbox = Schema.decodeUnknownSync(BusinessOutbox)(
          records.get(path)!.state,
        ).businessOutbox;
        assert.deepEqual(
          outbox.map((n) => n.kind),
          ["GoalProgress", "NeedsAttention", "GoalProgress"],
        );
        assert.equal(outbox.at(-1)!.revision, records.get(path)!.revision);
        assert.ok(
          outbox.every(
            (n) =>
              n.causal.rootRequestId === "review-request" && n.causal.remainingAgentTurns === 0,
          ),
        );
      }),
    ),
  );
  for (const restart of [false, true]) {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* makeContextRegistry(retainedStore(records));
          const system = yield* ActorSystem.make().pipe(
            ActorSystem.provide(
              Layer.succeed(ContextRegistry, registry),
              PersonalActions.unavailable,
              personalDisabled,
            ),
          );
          const personal = yield* system.spawn("personal", PersonalAgentActor);
          yield* personal.ask<PersonalReply>((replyTo) => ({ _tag: "Get", replyTo }));
          const changes = yield* registry.subscribe;
          const notifications = yield* system.spawn("notifications", NotificationsActor);
          yield* notifications.ask<void>((replyTo) => ({ _tag: "Ready", replyTo }));
          if (!restart)
            yield* changes.pipe(
              Stream.filter(
                (change) =>
                  change.record.path === "/notifications" &&
                  Schema.decodeUnknownSync(NotificationState)(
                    change.record.state,
                  ).deliveries.filter((d) => d.status === "delivered").length === 3,
              ),
              Stream.take(1),
              Stream.runDrain,
            );
          const current = registry.get("/personal")!;
          assert.equal(current.messages.length, 3);
          assert.deepEqual(
            Schema.decodeUnknownSync(PersonalState)(current.state).pendingRequestIds,
            [],
          );
          assert.ok(
            Schema.decodeUnknownSync(Schema.Array(PersonalMessage))(current.messages).every(
              (message) =>
                message.payload._tag === "ProgressEvent" &&
                message.payload.processing === "display-only",
            ),
          );
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
  }
});

for (const loss of [false, true]) {
  test(`notification inbox and delivery recover with one visible event (receiver acknowledgement loss: ${loss})`, async () => {
    const records = new Map<string, ContextRecord>();
    let loseAck = loss;
    const input = notification(1);
    for (const restart of [false, true]) {
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const registry = yield* makeContextRegistry({
              ...retainedStore(records),
              save: (record) => {
                records.set(record.path, structuredClone(record));
                if (record.path === "/personal" && record.messages.length && loseAck) {
                  loseAck = false;
                  throw new Error("Personal notification committed before acknowledgement loss");
                }
              },
            });
            if (!restart) {
              yield* registry.register(sourcePath, publication);
              yield* registry.commit(
                {
                  path: sourcePath,
                  description: "Source result",
                  state: { businessOutbox: [input] },
                  messages: [],
                },
                { expectedRevision: 0 },
              );
            }
            const system = yield* ActorSystem.make().pipe(
              ActorSystem.provide(
                Layer.succeed(ContextRegistry, registry),
                PersonalActions.unavailable,
                personalDisabled,
              ),
            );
            const personal = yield* system.spawn("personal", PersonalAgentActor);
            yield* personal.ask<PersonalReply>((replyTo) => ({ _tag: "Get", replyTo }));
            const changes = yield* registry.subscribe;
            const dispatcher = yield* system.spawn("notifications", NotificationsActor);
            yield* dispatcher.ask<void>((replyTo) => ({ _tag: "Ready", replyTo }));
            if (!restart || loss)
              yield* changes.pipe(
                Stream.filter((change) =>
                  !restart && loss
                    ? change.record.path === "/personal" && change.record.messages.length === 1
                    : change.record.path === "/notifications" &&
                      Schema.decodeUnknownSync(NotificationState)(change.record.state).deliveries[0]
                        ?.status === "delivered",
                ),
                Stream.take(1),
                Stream.runDrain,
              );
            const replay = yield* personal.ask<PersonalReply>((replyTo) => ({
              _tag: "Notify",
              input,
              replyTo,
            }));
            assert.equal(replay._tag, "Accepted");
            const current = registry.get("/personal")!;
            assert.equal(current.messages.length, 1);
            assert.deepEqual(
              Schema.decodeUnknownSync(PersonalState)(current.state).pendingRequestIds,
              [input.requestId],
            );
            const changed = yield* personal.ask<PersonalReply>((replyTo) => ({
              _tag: "Notify",
              input: { ...input, text: "Altered" },
              replyTo,
            }));
            assert.ok(changed._tag === "Rejected" && changed.error.kind === "conflict");
            const fabricated = yield* personal.ask<PersonalReply>((replyTo) => ({
              _tag: "Notify",
              input: { ...input, requestId: "not-committed" },
              replyTo,
            }));
            assert.ok(fabricated._tag === "Rejected" && fabricated.error.kind === "invalid-input");
            yield* dispatcher.tell({ _tag: "Wake" });
            yield* dispatcher.ask<void>((replyTo) => ({ _tag: "Ready", replyTo }));
            assert.equal(registry.get("/personal")!.messages.length, 1);
          }),
        ).pipe(Effect.timeout("5 seconds")),
      );
    }
  });
}

test("Personal retains over-budget progress while admitting at most eight automatic inputs per cause across restart", async () => {
  const records = new Map<string, ContextRecord>();
  for (const restart of [false, true]) {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* makeContextRegistry(retainedStore(records));
          yield* registry.register(sourcePath, publication);
          const system = yield* ActorSystem.make().pipe(
            ActorSystem.provide(
              Layer.succeed(ContextRegistry, registry),
              PersonalActions.unavailable,
              personalDisabled,
            ),
          );
          const personal = yield* system.spawn("personal", PersonalAgentActor);
          yield* personal.ask<PersonalReply>((replyTo) => ({ _tag: "Get", replyTo }));
          const start = restart ? 6 : 1;
          const end = restart ? 11 : 5;
          for (let index = start; index <= end; index++) {
            const input = notification(index, index === 11 ? 0 : 4);
            const source = registry.get(sourcePath);
            const outbox = source
              ? Schema.decodeUnknownSync(BusinessOutbox)(source.state).businessOutbox
              : [];
            yield* registry.commit(
              {
                path: sourcePath,
                description: "Results",
                state: { businessOutbox: [...outbox, input] },
                messages: [],
              },
              { expectedRevision: source?.revision ?? 0 },
            );
            const accepted = yield* personal.ask<PersonalReply>((replyTo) => ({
              _tag: "Notify",
              input,
              replyTo,
            }));
            assert.equal(accepted._tag, "Accepted");
          }
          if (restart) {
            const current = registry.get("/personal")!;
            const messages = Schema.decodeUnknownSync(Schema.Array(PersonalMessage))(
              current.messages,
            );
            assert.equal(messages.length, 11);
            assert.equal(
              Schema.decodeUnknownSync(PersonalState)(current.state).pendingRequestIds.length,
              8,
            );
            assert.equal(
              messages.filter(
                (m) =>
                  m.payload._tag === "ProgressEvent" && m.payload.processing === "display-only",
              ).length,
              3,
            );
          }
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
  }
});

test("Run outcomes and Personal task proposals form a bounded durable feedback chain without executor submission", async () => {
  const records = new Map<string, ContextRecord>();
  let models = 0;
  let loseOutcomeAck = true;
  let submitted = 0;
  const agents = {
    test: fakeAgent({
      submit: () =>
        Effect.sync(() => {
          submitted++;
          return { sessionId: "unexpected" };
        }),
    }),
  };
  for (const restart of [false, true]) {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* makeContextRegistry({
            ...retainedStore(records),
            save: (record) => {
              records.set(record.path, structuredClone(record));
              const outbox = Schema.decodeUnknownResult(BusinessOutbox)(record.state);
              if (
                record.path.startsWith("/runs/personal--") &&
                loseOutcomeAck &&
                outbox._tag === "Success" &&
                outbox.success.businessOutbox.length
              ) {
                loseOutcomeAck = false;
                throw new Error(
                  "Run outcome and notification committed before acknowledgement loss",
                );
              }
            },
          });
          const actions = yield* PersonalActions;
          const system = yield* ActorSystem.make().pipe(
            ActorSystem.provide(
              Layer.succeed(ContextRegistry, registry),
              Layer.succeed(PersonalActions, actions),
              Layer.succeed(ExternalAgents, agents),
              taskExecutionLayer({
                prepare: () => Effect.die(new Error("Task input is already frozen")),
                ready: () => Effect.succeed(false),
              }),
              personalReasoningLayer({
                enabled: true,
                run: (input) =>
                  Effect.sync(() => {
                    models++;
                    assert.equal(input.payload._tag, "ProgressEvent");
                    return {
                      text: "Queued a follow-up",
                      tasks: [
                        {
                          agent: "test",
                          task: { instructions: "Review updated evidence", input: [] },
                        },
                      ],
                    };
                  }),
              }),
            ),
          );
          const runs = yield* system.spawn("runs", RunRootActor);
          const personal = yield* system.spawn("personal", PersonalAgentActor);
          yield* actions.bind(undefined, undefined, undefined, runs);
          yield* personal.ask<PersonalReply>((replyTo) => ({ _tag: "Get", replyTo }));
          const changes = yield* registry.subscribe;
          const dispatcher = yield* system.spawn("notifications", NotificationsActor);
          const wakes = yield* registry.subscribe;
          yield* Stream.runForEach(wakes, (change) =>
            Schema.is(BusinessOutbox)(change.record.state)
              ? dispatcher.tell({ _tag: "Wake" })
              : Effect.void,
          ).pipe(Effect.forkScoped);
          if (!restart) {
            const result = yield* personal.ask<PersonalReply>((replyTo) => ({
              _tag: "StartTask",
              replyTo,
              input: {
                requestId: "user-task",
                causationId: "user-task",
                expectedRevision: registry.get("/personal")!.revision!,
                agent: "test",
                task: { instructions: "Review evidence", input: [] },
              },
            }));
            assert.equal(result._tag, "Queued");
            yield* changes.pipe(
              Stream.filter((change) => {
                if (change.record.path !== "/notifications") return false;
                const deliveries = Schema.decodeUnknownSync(NotificationState)(
                  change.record.state,
                ).deliveries;
                return deliveries.length === 5 && deliveries.every((d) => d.status === "delivered");
              }),
              Stream.take(1),
              Stream.runDrain,
            );
          }
          yield* dispatcher.ask<void>((replyTo) => ({ _tag: "Ready", replyTo }));
          const messages = Schema.decodeUnknownSync(Schema.Array(PersonalMessage))(
            registry.get("/personal")!.messages,
          );
          const progress = messages.filter((message) => message.payload._tag === "ProgressEvent");
          assert.equal(models, 4);
          assert.equal(loseOutcomeAck, false);
          assert.equal(submitted, 0);
          assert.equal(progress.length, 5);
          assert.deepEqual(
            progress.map((message) => message.causal?.remainingAgentTurns),
            [4, 3, 2, 1, 0],
          );
          assert.ok(progress.every((message) => message.causal?.rootRequestId === "user-task"));
          assert.equal(
            Schema.decodeUnknownSync(PersonalState)(registry.get("/personal")!.state)
              .pendingRequestIds.length,
            0,
          );
          assert.equal(
            Object.keys(registry.snapshot()).filter((path) => path.startsWith("/runs/personal--"))
              .length,
            5,
          );
        }),
      ).pipe(
        Effect.provide(
          PersonalActions.layer.pipe(Layer.provide(Layer.succeed(ExternalAgents, agents))),
        ),
        Effect.timeout("8 seconds"),
      ),
    );
  }
});

test("notification recovery commits authorization before replay and deduplicates its receipt after acknowledgement loss", async () => {
  const records = new Map<string, ContextRecord>();
  const input = notification(1);
  records.set("/notifications", {
    path: "/notifications",
    revision: 10,
    description: "Notifications",
    messages: [],
    state: {
      deliveries: [{ input, status: "unknown", attempts: 3, error: "Lost acknowledgement" }],
    },
  });
  let loseAck = true;
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry({
          ...retainedStore(records),
          save: (record) => {
            records.set(record.path, structuredClone(record));
            if (
              record.path === "/notifications" &&
              loseAck &&
              Schema.decodeUnknownSync(NotificationState)(record.state).recoveryReceipts?.length
            ) {
              loseAck = false;
              throw new Error("Authorization committed before acknowledgement loss");
            }
          },
        });
        yield* registry.register(sourcePath, publication);
        yield* registry.commit(
          {
            path: sourcePath,
            description: "Result",
            state: { businessOutbox: [input] },
            messages: [],
          },
          { expectedRevision: 0 },
        );
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(ContextRegistry, registry),
            PersonalActions.unavailable,
            personalDisabled,
          ),
        );
        const personal = yield* system.spawn("personal", PersonalAgentActor);
        yield* personal.ask<PersonalReply>((replyTo) => ({ _tag: "Get", replyTo }));
        const dispatcher = yield* system.spawn("notifications", NotificationsActor);
        yield* dispatcher.ask<void>((replyTo) => ({ _tag: "Ready", replyTo }));
        const command = {
          _tag: "RetryNotification" as const,
          requestId: "operator-recovery",
          expectedRevision: registry.get("/notifications")!.revision!,
          deliveryId: input.requestId,
        };
        const changes = yield* registry.subscribe;
        const replies = yield* ActorTestKit.probe<RecoveryReply>();
        yield* dispatcher.tell({ _tag: "Recover", input: command, replyTo: replies.ref });
        const state = () =>
          Schema.decodeUnknownSync(NotificationState)(registry.get("/notifications")!.state);
        if (state().deliveries[0].status !== "delivered")
          yield* changes.pipe(
            Stream.filter(() => state().deliveries[0].status === "delivered"),
            Stream.take(1),
            Stream.runDrain,
          );
        const receipt = yield* dispatcher.ask<RecoveryReply>((replyTo) => ({
          _tag: "Recover",
          input: command,
          replyTo,
        }));
        assert.equal(receipt._tag, "Accepted");
        assert.deepEqual(
          yield* dispatcher.ask<RecoveryReply>((replyTo) => ({
            _tag: "Recover",
            input: command,
            replyTo,
          })),
          receipt,
        );
        assert.equal(
          (yield* dispatcher.ask<RecoveryReply>((replyTo) => ({
            _tag: "Recover",
            input: { ...command, deliveryId: "another" },
            replyTo,
          })))._tag,
          "Rejected",
        );
        assert.equal(
          (yield* dispatcher.ask<RecoveryReply>((replyTo) => ({
            _tag: "Recover",
            input: { ...command, requestId: "stale" },
            replyTo,
          })))._tag,
          "Rejected",
        );
        assert.equal(state().recoveryReceipts?.length, 1);
        assert.equal(state().deliveries[0].attempts, 4);
        assert.equal(state().deliveries[0].error, undefined);
        assert.equal(registry.get("/personal")!.messages.length, 1);
        assert.deepEqual(state().deliveries[0].input, input);
        const view = yield* makeApplicationApi({
          registry,
          inspect: Effect.succeed(null),
        }).inspectProcessing("notifications");
        assert.equal(view.entries[0].status, "delivered");
        assert.equal(JSON.stringify(view).includes("Result 1"), false);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});
