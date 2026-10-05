import { goalIntentRecords } from "./goal-fixtures.js";
import { goalWorkflowLayer } from "./workflow-fixtures.js";
import { goalInputId } from "../src/goals/inputs.js";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem } from "@aster/actor";
import { Deferred, Effect, Layer, Schema, Stream } from "effect";
import {
  ContextRegistry,
  ExternalAgents,
  GoalState,
  GoalsRootActor,
  makeContextRegistry,
  makeMemoryGoalHistory,
  type ContextRecord,
  type GoalDeliveryReply,
} from "../src/index.js";
import { GoalHistoryError } from "../src/goals/history.js";
import { type GoalIntentInput } from "../src/goals/intent.js";
import type { GoalCommandReply } from "../src/goals/actors.js";
import { preparationLayer } from "./fixtures.js";

const input: GoalIntentInput = {
  requestId: "source-revision-one-to-project",
  causationId: "source-revision-one",
  source: "/system-one",
  target: "/goals/project",
  expectedRevision: 1,
  intent: {
    intentId: "intent-one",
    goalSlug: "project",
    source: {
      contextPath: "/lark/im/chats/release",
      actorPath: "/lark/im/chats/release",
      name: "Release team",
      kind: "context",
    },
    content: {
      summary: "Release delayed pending API review",
      summaryRevision: "1",
      summaryFingerprint: "fingerprint",
    },
    relevance: {
      score: 0.9,
      rationale: "The release deadline affects this Goal",
      screeningRecordId: "screen-one",
      threshold: 0.7,
      policyVersion: "v1",
    },
    createdAt: "2026-10-02T00:00:00.000Z",
  },
};

for (const fault of ["accept-ack", "history-ack", "projection-ack"] as const) {
  test(`Goal Intent recovers ${fault} loss with one durable input, receipt and history entry`, async () => {
    const records = new Map<string, ContextRecord>();
    const history = makeMemoryGoalHistory();
    let fail = true;
    let firstInput = input;
    let receipt: { requestId: string; revision: number } | undefined;
    for (const restart of [false, true]) {
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const registry = yield* makeContextRegistry({
              loadAll: () => [...records.values()],
              save: (record) => {
                records.set(record.path, structuredClone(record));
                const item = goalIntentRecords(
                  Schema.decodeUnknownSync(GoalState)(record.state),
                )[0];
                if (
                  fail &&
                  item &&
                  ((fault === "accept-ack" && item.historySequence === undefined) ||
                    (fault === "projection-ack" && item.historySequence !== undefined))
                ) {
                  fail = false;
                  throw new Error("Persisted before acknowledgement loss");
                }
              },
            });
            const entered = yield* Deferred.make<void>();
            const system = yield* ActorSystem.make().pipe(
              ActorSystem.provide(
                Layer.succeed(ContextRegistry, registry),
                preparationLayer,
                Layer.succeed(ExternalAgents, {}),
                goalWorkflowLayer({
                  definitions: [{ slug: "project", description: "Monitor release" }],
                  history: {
                    ...history,
                    append: (slug, message, requestId) =>
                      Effect.gen(function* () {
                        if (
                          requestId === goalInputId("project", "GoalIntent", input.intent.intentId)
                        ) {
                          assert.equal(
                            goalIntentRecords(
                              Schema.decodeUnknownSync(GoalState)(
                                records.get("/goals/project")!.state,
                              ),
                            ).length,
                            1,
                            "The inbox must commit before the history projection",
                          );
                        }
                        const entry = yield* history.append(slug, message, requestId);
                        if (
                          fail &&
                          fault === "history-ack" &&
                          requestId === goalInputId("project", "GoalIntent", input.intent.intentId)
                        ) {
                          fail = false;
                          return yield* new GoalHistoryError({
                            cause: new Error("History acknowledgement lost"),
                          });
                        }
                        return entry;
                      }),
                  },
                  reasoner: {
                    plan: () =>
                      Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
                  },
                  signals: () => [],
                  reconcile: () => Effect.succeed([]),
                  deactivate: () => Effect.void,
                }),
              ),
            );
            const root = yield* system.spawn("goals", GoalsRootActor);
            yield* root.ask<import("../src/goals/protocol.js").GoalReadyReply>((replyTo) => ({
              _tag: "AwaitReady",
              stage: "restored",
              replyTo,
            }));
            const send = (value: GoalIntentInput) =>
              root.ask<GoalDeliveryReply>((replyTo) => ({
                _tag: "Route",
                slug: "project",
                command: {
                  _tag: "SubmitInput",
                  requestId: value.requestId,
                  input: { _tag: "GoalIntent", delivery: value },
                  replyTo: replyTo,
                },
              }));
            if (!restart) {
              firstInput = {
                ...input,
                expectedRevision: registry.get("/goals/project")!.revision!,
              };
              const invalid = yield* send({
                ...firstInput,
                intent: {
                  ...firstInput.intent,
                  relevance: { ...firstInput.intent.relevance, score: 0.1 },
                },
              });
              assert.ok(invalid._tag === "Rejected" && invalid.error.kind === "invalid-input");
              assert.equal(
                goalIntentRecords(
                  Schema.decodeUnknownSync(GoalState)(registry.get("/goals/project")!.state),
                ).length,
                0,
              );
              const changes = yield* registry.subscribe;
              const sending = yield* send(firstInput).pipe(Effect.forkScoped);
              yield* changes.pipe(
                Stream.filter(
                  (change) =>
                    change.path === "/goals/project" &&
                    goalIntentRecords(Schema.decodeUnknownSync(GoalState)(change.record.state))[0]
                      ?.historySequence !== undefined,
                ),
                Stream.take(1),
                Stream.runDrain,
              );
              assert.equal(
                sending.pollUnsafe(),
                undefined,
                "A failed acceptance never returns a success receipt",
              );
              yield* root.tell({ _tag: "Initialize" });
              yield* Deferred.await(entered);
            }
            const accepted = yield* send(firstInput);
            assert.equal(accepted._tag, "Accepted");
            if (accepted._tag !== "Accepted") return;
            if (receipt) assert.deepEqual(accepted.receipt, receipt);
            receipt = accepted.receipt;
            const entries = (yield* history.read("project", { limit: 100 })).filter(
              (entry) =>
                entry.requestId === goalInputId("project", "GoalIntent", input.intent.intentId),
            );
            assert.equal(entries.length, 1);
            assert.match(JSON.stringify(entries[0]!.message), /Release team/);
            assert.match(
              JSON.stringify(entries[0]!.message),
              /The release deadline affects this Goal/,
            );
            const stored = Schema.decodeUnknownSync(GoalState)(
              registry.get("/goals/project")!.state,
            );
            assert.equal(goalIntentRecords(stored).length, 1);
            assert.deepEqual(goalIntentRecords(stored)[0]?.input, firstInput);
            assert.equal(fail, false);
            const conflict = yield* send({
              ...firstInput,
              intent: {
                ...firstInput.intent,
                content: { ...firstInput.intent.content, summary: "Changed evidence" },
              },
            });
            assert.ok(conflict._tag === "Rejected" && conflict.error.kind === "conflict");
            const stale = yield* send({ ...firstInput, requestId: "stale" });
            assert.ok(stale._tag === "Rejected" && stale.error.kind === "conflict");
            if (restart) {
              yield* root.ask<GoalCommandReply>((replyTo) => ({
                _tag: "Route",
                slug: "project",
                command: { _tag: "End", requestId: "test-8862", replyTo: replyTo },
              }));
              assert.deepEqual(yield* send(firstInput), accepted);
              const closed = yield* send({
                ...firstInput,
                requestId: "closed",
                expectedRevision: registry.get("/goals/project")!.revision!,
              });
              assert.ok(closed._tag === "Rejected" && closed.error.kind === "conflict");
              const unavailable = yield* root.ask<GoalDeliveryReply>((replyTo) => ({
                _tag: "Route",
                slug: "missing",
                command: {
                  _tag: "SubmitInput",
                  requestId: firstInput.requestId,
                  input: { _tag: "GoalIntent", delivery: firstInput },
                  replyTo: replyTo,
                },
              }));
              assert.ok(unavailable._tag === "Rejected" && unavailable.error.kind === "not-found");
            }
          }),
        ).pipe(Effect.timeout("5 seconds")),
      );
    }
  });
}
