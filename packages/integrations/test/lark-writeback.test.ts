import assert from "node:assert/strict";
import { test } from "node:test";
import { Effect } from "effect";
import { makeContextRegistry } from "@aster/core/testing";
import { type WritebackOperation } from "@aster/core";
import { makeLarkChannelWrites, isLarkWritebackEcho } from "@aster/integrations";

const source = "/signals/report/runs/one";
const target = "/lark/im/chats/oc_test";
const at = "2026-10-03T00:00:00.000Z";
const operation: WritebackOperation = {
  request: {
    requestId: "a".repeat(48),
    source,
    taskSource: "/signals/report",
    causationId: "input-1",
    createdAt: at,
    content: "@./private-file\nExact approved text",
    action: { _tag: "PublishResult", channelPath: target, identity: "user" },
    causal: { rootRequestId: "input-1", remainingAgentTurns: 0 },
  },
  status: "sending",
  submittedAt: at,
  authorization: {
    approvalId: `${source}:writeback:${"a".repeat(48)}`,
    approvalsRevision: 2,
    approvedAt: at,
  },
};
const registryFor = (writeback = operation) =>
  makeContextRegistry({
    loadAll: () => [
      { path: source, description: "Run", revision: 3, messages: [], state: { writeback } },
      {
        path: target,
        description: "Known chat",
        revision: 1,
        messages: [],
        state: { chat: { id: "oc_test" } },
      },
    ],
    save: () => assert.fail("A Channel adapter does not mutate Run state"),
  });

test("Lark publication binds exact approved content, identity and idempotency key without file expansion", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const registry = yield* registryFor();
      let calls = 0;
      let argv: readonly string[] = [];
      const adapter = makeLarkChannelWrites(registry, (args) =>
        Effect.sync(() => {
          calls++;
          argv = args;
          return JSON.stringify({
            ok: true,
            identity: "user",
            data: { chat_id: "oc_test", message_id: "om_one" },
          });
        }),
      );
      assert.deepEqual(yield* adapter.publish(operation.request, operation.authorization!), {
        externalId: "om_one",
      });
      assert.deepEqual(argv, [
        "im",
        "+messages-send",
        "--as",
        "user",
        "--chat-id",
        "oc_test",
        "--msg-type",
        "text",
        "--content",
        JSON.stringify({ text: operation.request.content }),
        "--idempotency-key",
        operation.request.requestId,
        "--format",
        "json",
      ]);
      assert.equal(calls, 1);
      for (const altered of [
        { ...operation.request, content: "Different message" },
        { ...operation.request, action: { ...operation.request.action, identity: "bot" as const } },
        {
          ...operation.request,
          action: { ...operation.request.action, channelPath: "/lark/im/chats/oc_other" },
        },
      ]) {
        const error = yield* adapter.publish(altered, operation.authorization!).pipe(Effect.flip);
        assert.equal(error.outcome, "rejected");
        assert.equal(calls, 1, "Rejected payloads never reach the transport");
      }
    }),
  );
});

test("unverifiable successful process output remains unknown and is never retried", async () => {
  for (const stdout of [
    "not-json",
    JSON.stringify({ code: 0 }),
    JSON.stringify({
      ok: true,
      identity: "bot",
      data: { chat_id: "oc_test", message_id: "om_one" },
    }),
  ]) {
    let attempts = 0;
    await Effect.runPromise(
      Effect.gen(function* () {
        const registry = yield* registryFor();
        const adapter = makeLarkChannelWrites(registry, () =>
          Effect.sync(() => {
            attempts++;
            return stdout;
          }),
        );
        const error = yield* adapter
          .publish(operation.request, operation.authorization!)
          .pipe(Effect.flip);
        assert.equal(error.outcome, "unknown");
        assert.equal(attempts, 1);
      }),
    );
  }
});

test("Channel echo guard uses retained receipts and conservatively handles unknown acknowledgements", async () => {
  for (const status of ["published", "sending", "unknown", "rejected"] as const) {
    await Effect.runPromise(
      Effect.gen(function* () {
        const registry = yield* registryFor({
          ...operation,
          status,
          ...(status === "published" ? { externalId: "om_one" } : {}),
        });
        const incoming = {
          id: "om_one",
          at,
          content: JSON.stringify({ text: operation.request.content }),
        };
        assert.equal(isLarkWritebackEcho(registry, target, incoming), status !== "rejected");
        assert.equal(isLarkWritebackEcho(registry, "/lark/im/chats/oc_other", incoming), false);
        assert.equal(
          isLarkWritebackEcho(registry, target, {
            ...incoming,
            id: "different",
            content: "Human update",
          }),
          false,
        );
        if (status !== "published")
          assert.equal(
            isLarkWritebackEcho(registry, target, { ...incoming, at: "2026-01-01T00:00:00Z" }),
            false,
          );
      }),
    );
  }
});
