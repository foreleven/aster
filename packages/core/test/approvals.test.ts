import { DurableContext } from "@aster/core";
import { Actor, ActorSystem } from "@aster/actor";
import { Effect, Layer, Match } from "effect";
import assert from "node:assert/strict";
import { test } from "node:test";
import type { ApprovalReply } from "../src/approvals/actor.js";
import {
  ApprovalQueueActor,
  ApprovalResolved,
  ContextRegistry,
  approvalEntries,
  sendApproval,
  type ApprovalEntry,
  type ApprovalResponse,
  type InputRequest,
  type StoredContext,
} from "../src/index.js";
import { makeContextRegistry, type ContextStore } from "../src/testing/context.js";

const until = (condition: () => boolean) =>
  Effect.gen(function* () {
    while (!condition()) yield* Effect.sleep(5);
  }).pipe(Effect.timeout("5 seconds"));

test("approval validation rejects invalid answers without saving and accepts complete responses", async () => {
  const questions = [
    { id: "environment", prompt: "Which environment?" },
    { id: "version", prompt: "Which version?" },
  ];
  const cases: readonly {
    name: string;
    missing?: boolean;
    kind?: ApprovalEntry["kind"];
    status?: ApprovalEntry["status"];
    questions?: InputRequest["questions"];
    response: ApprovalResponse;
    accepted?: ApprovalResponse;
    error?: string;
  }[] = [
    {
      name: "invalid offered option",
      questions: [{ id: "choice", prompt: "Pick", options: ["A"] }],
      response: { answers: { choice: ["B"] } },
      error: "Answer must match an offered option",
    },
    {
      name: "single choice rejects multiple",
      questions: [{ id: "choice", prompt: "Pick", options: ["A", "B"], multiple: false }],
      response: { answers: { choice: ["A", "B"] } },
      error: "Single-choice question requires one answer",
    },
    {
      name: "custom input is provider controlled",
      questions: [{ id: "choice", prompt: "Pick", options: ["A"], allowOther: true }],
      response: { answers: { choice: ["B"] } },
    },
    { name: "missing entry", missing: true, response: {}, error: "Approval not found" },
    ...(["resolved", "acknowledged", "revoked"] as const).map((status) => ({
      name: `${status} entry`,
      status,
      response: {},
      error: "Approval already resolved",
    })),
    {
      name: "confirmation needs a decision",
      kind: "confirmation",
      response: { text: "Yes" },
      error: "Approval decision is required",
    },
    {
      name: "approval needs a decision",
      kind: "approval",
      response: {},
      error: "Approval decision is required",
    },
    { name: "explicit approval", kind: "confirmation", response: { decision: "approve" } },
    { name: "explicit rejection", kind: "approval", response: { decision: "reject" } },
    { name: "empty input", response: { decision: "approve" }, error: "Input is required" },
    {
      name: "blank text and answers",
      questions,
      response: { text: "  ", answers: { environment: ["", "\t"] } },
      error: "Input is required",
    },
    { name: "free-form input", response: { text: "Use staging" } },
    {
      name: "single question accepts text",
      questions: questions.slice(0, 1),
      response: { text: "Use staging" },
      accepted: { text: "Use staging", answers: { environment: ["Use staging"] } },
    },
    {
      name: "multiple questions cannot share free text",
      questions,
      response: { text: "Use staging" },
      error: "Every question requires an answer",
    },
    {
      name: "partial answers",
      questions,
      response: { answers: { environment: ["staging"] } },
      error: "Every question requires an answer",
    },
    {
      name: "unrelated answers",
      questions: questions.slice(0, 1),
      response: { answers: { other: ["staging"] } },
      error: "Every question requires an answer",
    },
    {
      name: "complete keyed answers",
      questions,
      response: { answers: { environment: ["", "staging"], version: ["v2"] } },
    },
  ];

  for (const scenario of cases) {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const kind = scenario.kind ?? "input";
          const entry: ApprovalEntry = {
            id: "validation",
            target: "/user/receiver",
            contextPath: "/delegations/validation",
            kind,
            status: scenario.status ?? "pending",
            request: {
              id: "external",
              kind: kind === "input" ? "input" : "approval",
              prompt: "Confirm or provide input",
              questions: scenario.questions,
            },
          };
          const initial = {
            path: "/approvals",
            description: "Approval queue",
            state: { entries: scenario.missing ? [] : [entry], revokedIds: [] },
            messages: [],
          };
          let saves = 0;
          const registry = yield* makeContextRegistry({
            loadAll: () => [{ snapshot: { ...initial, revision: 0 }, events: [] }],
            save: () => {
              saves++;
            },
          });
          const system = yield* ActorSystem.make().pipe(
            ActorSystem.provide(
              Layer.merge(
                Layer.succeed(ContextRegistry, registry),
                Layer.succeed(DurableContext, registry.backend),
              ),
            ),
          );
          const queue = yield* system.spawn("approvals", ApprovalQueueActor);
          const reply = yield* queue.ask<ApprovalReply>((replyTo) => ({
            _tag: "Resolve",
            id: entry.id,
            response: scenario.response,
            replyTo,
          }));
          if (scenario.error) {
            assert.equal(reply._tag, "Rejected");
            if (reply._tag === "Rejected") assert.equal(reply.error.message, scenario.error);
            assert.equal(saves, 0, scenario.name);
            assert.deepEqual(
              registry.get("/approvals"),
              { ...initial, revision: 0 },
              scenario.name,
            );
          } else {
            assert.deepEqual(reply, { _tag: "Accepted" }, scenario.name);
            assert.equal(saves, 1, scenario.name);
            assert.deepEqual(
              approvalEntries(registry),
              [{ ...entry, status: "resolved", response: scenario.accepted ?? scenario.response }],
              scenario.name,
            );
          }
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
  }
});

test("persisted approval answers reach a recreated Actor only after it exists, with acknowledgement", async () => {
  const records = new Map<string, StoredContext>();
  const store: ContextStore = {
    loadAll: () => [...records.values()].map((r) => structuredClone(r)),
    save: (record) => {
      records.set(record.snapshot.path, structuredClone(record));
    },
  };
  let received = 0;
  const Receiver = Actor.define("test/approvalReceiver", {
    commands: [ApprovalResolved],
  })(
    Effect.succeed({
      receive: (command, context) =>
        Match.value(command).pipe(
          Match.tag("ApprovalResolved", ({ requestId, response }) =>
            Effect.gen(function* () {
              assert.equal(response.decision, "approve");
              received++;
              yield* sendApproval(context, {
                _tag: "Acknowledge",
                id: requestId,
                target: context.path,
              });
            }),
          ),
          Match.exhaustive,
        ),
    }),
  );
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry(store);
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.merge(
              Layer.succeed(ContextRegistry, registry),
              Layer.succeed(DurableContext, registry.backend),
            ),
          ),
        );
        const queue = yield* system.spawn("approvals", ApprovalQueueActor);
        yield* queue.tell({
          _tag: "Enqueue",
          entry: {
            id: "approval-1",
            target: "/user/receiver",
            contextPath: "/delegations/one",
            kind: "approval",
            request: { id: "external-1", kind: "approval", prompt: "Allow?" },
            status: "pending",
          },
        });
        const reply = yield* queue.ask<ApprovalReply>((replyTo) => ({
          _tag: "Resolve",
          id: "approval-1",
          response: { decision: "approve" },
          replyTo,
        }));
        assert.deepEqual(reply, { _tag: "Accepted" });
        assert.equal(approvalEntries(registry)[0]!.status, "resolved");
      }),
    ),
  );
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry(store);
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.merge(
              Layer.succeed(ContextRegistry, registry),
              Layer.succeed(DurableContext, registry.backend),
            ),
          ),
        );
        yield* system.spawn("approvals", ApprovalQueueActor);
        yield* Effect.sleep(30);
        assert.equal(received, 0);
        yield* system.spawn("receiver", Receiver);
        yield* until(() => approvalEntries(registry)[0]?.status === "acknowledged");
        yield* Effect.sleep(1100);
        assert.equal(received, 1);
        assert.equal(
          registry.backend.journal().some((event) => event.record.path === "/approvals"),
          false,
        );
      }),
    ),
  );
});
