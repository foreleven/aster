import assert from "node:assert/strict";
import { test } from "node:test";
import { Effect } from "effect";
import { makeTestContext } from "@aster/core/testing";
import { makeImSummaryGate } from "@aster/integrations";

const secret = "PRIVATE_LARK_SENTINEL";
const message = {
  id: "message",
  at: "2026-10-02",
  content: "Release ready",
  url: "https://example.com/message",
  deleted: false,
  sender: { id: "user", name: "Alice", token: secret },
};
const chat = { id: "chat", name: "Project", mode: "group", description: "Project group" };

test("dormant Lark Contexts read committed text without loading integration definitions", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const records = [
          {
            text: JSON.stringify({ chat, summary: { text: "Ready", references: [] } }),
            path: "/lark/im/chats/chat",
            revision: 6,
            description: "Project group",
            state: { chat, summary: { text: "Ready", references: [] }, provider: secret },
            messages: [message],
          },
          {
            text: "Email from Alice: Release\nReady",
            path: "/lark/mail/me/email",
            revision: 8,
            description: "Email",
            state: {
              messageId: "email",
              mailbox: "me",
              from: "Alice",
              subject: "Release",
              bodyPlainText: "Ready",
              attachments: [],
              credential: secret,
            },
            messages: [],
          },
        ];
        const registry = yield* makeTestContext({
          loadAll: () => records.map((snapshot) => ({ snapshot, events: [] })),
          save: () => {},
        });
        const texts = Object.values(registry.backend.snapshot());
        assert.equal(JSON.stringify(texts).includes(secret), false);
        assert.ok(texts.every((record) => !("state" in record) && !("messages" in record)));
        assert.equal(texts[0]!.text, records[0]!.text);
        assert.equal(texts[1]!.text, records[1]!.text);
      }),
    ),
  );
});

test("summary admission sends readable sender identity without opaque provider fields", async () => {
  let input = "";
  const gate = makeImSummaryGate({
    systemOne: (request) =>
      Effect.sync(() => {
        input = typeof request.state === "string" ? request.state : JSON.stringify(request.state);
        return { answers: { summarize: { type: "choice", choice: "yes" } } };
      }),
  });
  assert.equal(
    await Effect.runPromise(
      Effect.scoped(gate.needed({ path: "/lark/im/chats/chat", chat, messages: [message] })),
    ),
    true,
  );
  assert.equal(input.includes(secret), false);
  assert.ok(input.includes("Alice") && input.includes("Release ready"));
});
