import assert from "node:assert/strict";
import { test } from "node:test";
import { Effect } from "effect";
import { makeApplicationApi, makeContextRegistry } from "@aster/core";
import { larkContextViews, makeImSummaryGate } from "@aster/integrations";

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

test("archived Lark contexts remain readable through integration family policies without live children", async () => {
  await Effect.runPromise(
    Effect.gen(function* () {
      const records = [
        {
          path: "/lark/im/chats/chat",
          revision: 6,
          description: "Project group",
          state: { chat, summary: { text: "Ready", references: [] }, provider: secret },
          messages: [message],
        },
        {
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
      const registry = yield* makeContextRegistry({ loadAll: () => records, save: () => {} });
      yield* registry.registerViews(larkContextViews);
      const api = makeApplicationApi({ registry, inspect: Effect.succeed(null) });
      const views = yield* api.contexts;
      assert.equal(JSON.stringify(views).includes(secret), false);
      assert.ok(views.every((record) => record.projection?.visibility === "public"));
      assert.deepEqual(views[0]!.messages, [{ ...message, sender: { id: "user", name: "Alice" } }]);
      assert.deepEqual(registry.get(records[0]!.path), records[0]);
    }),
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
      gate.needed({ path: "/lark/im/chats/chat", chat, messages: [message] }),
    ),
    true,
  );
  assert.equal(input.includes(secret), false);
  assert.ok(input.includes("Alice") && input.includes("Release ready"));
});
