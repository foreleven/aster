import { personalDisabled } from "./workflow-fixtures.js";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem } from "@aster/actor";
import {
  ApplicationError,
  PersonalResult,
  PersonalState,
  type ApprovalDeliveryInput,
  type ApprovalEntry,
  type PersonalApprovalResponseInput,
} from "@aster/api-contracts";
import { Effect, Layer, Schema, Stream } from "effect";
import {
  ApprovalQueueActor,
  ContextRegistry,
  ExternalAgents,
  PersonalActions,
  PersonalAgentActor,
  approvalEntries,
  makeApplicationApi,
  type ApprovalCommandReply,
  type ContextRecord,
} from "../src/index.js";
import { makeContextRegistry } from "../src/testing/context.js";

const entry: ApprovalEntry = {
  id: "confirm-1",
  target: "/user/receiver",
  contextPath: "/signals/release/runs/one",
  kind: "confirmation",
  request: { id: "confirm-1", kind: "approval", prompt: "Inspect release?" },
  status: "pending",
};
const input: PersonalApprovalResponseInput = {
  requestId: "decision-1",
  causationId: "user-decision",
  approvalId: entry.id,
  expectedRevision: 1,
  approvalsRevision: 2,
  response: { decision: "approve" },
};
const fixture = (records = new Map<string, ContextRecord>(), loseAck = false) =>
  Effect.gen(function* () {
    const registry = yield* makeContextRegistry({
      loadAll: () => [...records.values()],
      save: (record) => {
        records.set(record.path, structuredClone(record));
      },
    });
    const actions = yield* PersonalActions.pipe(
      Effect.provide(PersonalActions.layer.pipe(Layer.provide(Layer.succeed(ExternalAgents, {})))),
    );
    const system = yield* ActorSystem.make().pipe(
      ActorSystem.provide(
        Layer.succeed(ContextRegistry, registry),
        personalDisabled,
        Layer.succeed(PersonalActions, {
          ...actions,
          respondApproval: (command) =>
            Effect.gen(function* () {
              const source = Schema.decodeUnknownSync(PersonalState)(
                records.get("/personal")!.state,
              ).outbox!.find((item) => item.input.requestId === command.requestId)!;
              assert.ok(source.attempts! > 0, "attempt is persisted before submission");
              const receipt = yield* actions.respondApproval(command);
              const target = records.get("/approvals")!;
              assert.ok(
                (target.state as { commandReceipts: { receipt: unknown }[] }).commandReceipts.some(
                  (item) => JSON.stringify(item.receipt) === JSON.stringify(receipt),
                ),
                "receipt persisted before acknowledgement",
              );
              if (loseAck) {
                loseAck = false;
                return yield* new ApplicationError({
                  kind: "unavailable",
                  message: "Injected lost acknowledgement",
                });
              }
              return receipt;
            }),
        }),
      ),
    );
    const approvals = yield* system.spawn("approvals", ApprovalQueueActor);
    const personal = yield* system.spawn("personal", PersonalAgentActor);
    yield* actions.bind(undefined, undefined, approvals);
    const api = makeApplicationApi({
      registry,
      personal,
      approvals,
      inspect: Effect.succeed(null),
    });
    yield* api.personal.get;
    const until = (predicate: () => boolean) =>
      Effect.gen(function* () {
        const changes = yield* registry.subscribe;
        if (!predicate())
          yield* changes.pipe(Stream.filter(predicate), Stream.take(1), Stream.runDrain);
      });
    const status = () =>
      Schema.decodeUnknownSync(PersonalState)(registry.get("/personal")!.state).outbox?.[0];
    const respond = (command: ApprovalDeliveryInput) =>
      approvals.ask<ApprovalCommandReply>((replyTo) => ({
        _tag: "RespondPersonal",
        input: command,
        replyTo,
      }));
    return { registry, api, approvals, until, status, respond };
  });

test("Personal approval response recovers a lost acknowledgement and replays after receiver acknowledgement", async () => {
  const records = new Map<string, ContextRecord>();
  let accepted: unknown;
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const env = yield* fixture(records, true);
        yield* env.approvals.tell({ _tag: "Enqueue", entry });
        yield* env.until(() => approvalEntries(env.registry).length === 1);
        accepted = yield* env.api.personal.respondApproval(input);
        yield* env.until(() => env.status()?.status === "unknown");
        assert.equal(approvalEntries(env.registry)[0].status, "resolved");
        yield* env.approvals.tell({ _tag: "Acknowledge", id: entry.id, target: entry.target });
        yield* env.until(() => approvalEntries(env.registry)[0].status === "acknowledged");
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
  const target = structuredClone(records.get("/approvals")!);
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const env = yield* fixture(records);
        yield* env.until(() => env.status()?.status === "delivered");
        assert.deepEqual(env.registry.get("/approvals"), target);
        assert.equal(env.status()?.attempts, 2);
        assert.equal(env.status()?.error, undefined);
        assert.deepEqual(yield* env.api.personal.respondApproval(input), accepted);
        assert.equal(env.status()?.receipt?.revision, 3);
        assert.equal(
          env.registry
            .get("/approvals")!
            .messages.filter((message) => (message as { type: string }).type === "Resolved").length,
          1,
        );
        const command = env.status()!.input;
        assert.ok("approvalId" in command);
        if (command.operation !== "respondApproval") return;
        const conflict = yield* env.respond({ ...command, response: { decision: "reject" } });
        assert.equal(conflict._tag, "Rejected");
        assert.deepEqual(env.registry.get("/approvals"), target);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("versioned approval responses reject stale revisions, malformed answers and revoked requests without a receipt", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const env = yield* fixture();
        yield* env.approvals.tell({
          _tag: "Enqueue",
          entry: {
            ...entry,
            kind: "input",
            request: {
              id: entry.id,
              kind: "input",
              prompt: "Pick environment",
              questions: [
                { id: "env", prompt: "Which?", options: ["test", "prod"], multiple: false },
              ],
            },
          },
        });
        yield* env.until(() => approvalEntries(env.registry).length === 1);
        const command: ApprovalDeliveryInput = {
          operation: "respondApproval",
          requestId: "response",
          causationId: "user",
          source: "/personal",
          target: "/approvals",
          expectedRevision: 2,
          createdAt: "2026-10-02T00:00:00Z",
          approvalId: entry.id,
          response: { text: "test" },
        };
        const before = env.registry.get("/approvals");
        for (const invalid of [
          { ...command, expectedRevision: 1 },
          { ...command, response: { text: "unknown" } },
          { ...command, approvalId: "missing" },
        ]) {
          assert.equal((yield* env.respond(invalid))._tag, "Rejected");
          assert.deepEqual(env.registry.get("/approvals"), before);
        }
        assert.equal((yield* env.respond(command))._tag, "Accepted");
        assert.deepEqual(approvalEntries(env.registry)[0].response?.answers, { env: ["test"] });
        yield* env.approvals.tell({ _tag: "Enqueue", entry: { ...entry, id: "revoked" } });
        yield* env.approvals.tell({ _tag: "Revoke", id: "revoked" });
        yield* env.until(() =>
          approvalEntries(env.registry).some(
            (item) => item.id === "revoked" && item.status === "revoked",
          ),
        );
        const revoked = env.registry.get("/approvals")!;
        assert.equal(
          (yield* env.respond({
            ...command,
            requestId: "revoked-response",
            approvalId: "revoked",
            expectedRevision: revoked.revision!,
          }))._tag,
          "Rejected",
        );
        assert.deepEqual(env.registry.get("/approvals"), revoked);
        assert.equal(
          (yield* env.respond(command))._tag,
          "Accepted",
          "later queue mutations preserve the original receipt",
        );
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("Personal model result schema cannot authorize approval decisions", () => {
  const result = Schema.decodeUnknownSync(PersonalResult)({
    text: "Ready",
    approvalResponses: [{ approvalId: entry.id, response: { decision: "approve" } }],
  });
  assert.deepEqual(result, { text: "Ready" });
});
