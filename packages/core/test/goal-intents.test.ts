import { testConversations } from "./conversation-fixtures.js";
import { goalIntentRecords } from "./goal-fixtures.js";
import { goalWorkflowLayer } from "./workflow-fixtures.js";
import { goalInputId } from "../src/goals/state/inputs.js";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem } from "@aster/actor";
import { Deferred, Effect, Layer, Schema } from "effect";
import {
  ContextRegistry,
  ExternalAgents,
  GoalSnapshot,
  GoalsRootActor,
  type ContextRecord,
  type GoalCommandReply,
} from "../src/index.js";
import { makeContextRegistry } from "../src/testing/context.js";
import { ConversationError } from "@aster/agent";
import { type GoalIntentInput } from "../src/goals/screening/intent.js";

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

for (const fault of ["pi-ack", "actor-ack"] as const) {
  test(`Goal recovers ${fault} loss with one Pi input and its original receipt`, async () => {
    const records = new Map<string, ContextRecord>();
    const history = testConversations();
    let fail = true;
    let firstInput = input;
    for (const restart of [false, true]) {
      await Effect.runPromise(
        Effect.scoped(
          Effect.gen(function* () {
            const cut = yield* Deferred.make<void>();
            const registry = yield* makeContextRegistry({
              loadAll: () => [...records.values()],
              save: (record) => {
                records.set(record.path, structuredClone(record));
                if (
                  fail &&
                  fault === "actor-ack" &&
                  record.path === "/goals/project" &&
                  goalIntentRecords(Schema.decodeUnknownSync(GoalSnapshot)(record.state)).length
                ) {
                  fail = false;
                  Effect.runSync(Deferred.succeed(cut, undefined));
                  throw new Error("Actor acknowledgement lost after commit");
                }
              },
            });
            const system = yield* ActorSystem.make().pipe(
              ActorSystem.provide(
                Layer.succeed(ContextRegistry, registry),
                Layer.succeed(ExternalAgents, {}),
                goalWorkflowLayer({
                  definitions: [{ slug: "project", description: "Monitor release" }],
                  history: {
                    ...history,
                    append: (owner, requestId, kind, data) =>
                      Effect.gen(function* () {
                        const entry = yield* history.append(owner, requestId, kind, data);
                        if (
                          fail &&
                          fault === "pi-ack" &&
                          requestId === goalInputId("project", "GoalIntent", input.intent.intentId)
                        ) {
                          assert.equal(
                            goalIntentRecords(
                              Schema.decodeUnknownSync(GoalSnapshot)(records.get(owner)!.state),
                            ).length,
                            0,
                          );
                          fail = false;
                          yield* Deferred.succeed(cut, undefined);
                          return yield* new ConversationError({
                            kind: "unavailable",
                            message: "Pi acknowledgement lost after commit",
                          });
                        }
                        return entry;
                      }),
                  },
                  reasoner: { plan: () => Effect.never },
                  deactivate: () => Effect.void,
                }),
              ),
            );
            const goalActivation = yield* Deferred.make<void>();
            const root = yield* system.spawn("goals", GoalsRootActor, {
              metadata: { goalActivation },
            });
            yield* root.awaitStarted;
            yield* (yield* system.select("/user/goals/project").resolve()).awaitStarted;
            const send = (value: GoalIntentInput) =>
              root.ask<GoalCommandReply>((replyTo) => ({
                _tag: "Route",
                slug: "project",
                command: {
                  _tag: "SubmitInput",
                  requestId: value.requestId,
                  input: { _tag: "GoalIntent", delivery: value },
                  replyTo,
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
              const sending = yield* send(firstInput).pipe(Effect.forkScoped);
              yield* Deferred.await(cut);
              assert.equal(sending.pollUnsafe(), undefined);
              return;
            }
            const accepted = yield* send(firstInput);
            assert.equal(accepted._tag, "Accepted");
            if (accepted._tag !== "Accepted") return;
            const stored = Schema.decodeUnknownSync(GoalSnapshot)(
              registry.get("/goals/project")!.state,
            );
            assert.equal(goalIntentRecords(stored).length, 1);
            const entries = (yield* history.read("/goals/project")).filter(
              (entry) =>
                entry.requestId === goalInputId("project", "GoalIntent", input.intent.intentId),
            );
            assert.equal(entries.length, 1);
            assert.match(JSON.stringify(entries[0]!.data), /Release team/);
            assert.doesNotMatch(JSON.stringify(stored), /Release delayed|fingerprint.*Release/);
            assert.deepEqual(
              stored.receipts.find((item) => item.requestId === input.requestId)?.receipt,
              accepted.receipt,
            );
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
            yield* root.ask<GoalCommandReply>((replyTo) => ({
              _tag: "Route",
              slug: "project",
              command: { _tag: "End", requestId: "end", replyTo },
            }));
            assert.deepEqual(yield* send(firstInput), accepted);
          }),
        ).pipe(Effect.timeout("5 seconds")),
      );
    }
  });
}
