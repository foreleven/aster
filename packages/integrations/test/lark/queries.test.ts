import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorNotFound } from "@aster/actor";
import { ContextQueryError } from "@aster/core";
import { makeTestContext } from "@aster/core/testing";
import { Effect, Schema } from "effect";
import { ChatState, serializeChat } from "../../src/lark/im/chat/state.js";
import { ChatMessage } from "../../src/lark/im/model.js";
import { EmailData } from "../../src/lark/mail/model.js";
import { LarkChatQueryError } from "../../src/lark/im/client.js";
import { installLark } from "./fixtures.js";
const chat = { id: "team", name: "Release", mode: "group", description: "Release planning" };
const email = {
  messageId: "retained",
  mailbox: "me",
  from: "Alice",
  subject: "Review",
  bodyPlainText: "Review",
  attachments: [],
};

test("queries read dormant committed records without activating writers or running summary work", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeTestContext();
        yield* Effect.scoped(
          registry.backend.open({
            path: "/lark/im/chats/team",
            state: ChatState,
            message: ChatMessage,
            keyOf: (m) => m.id,
            serialize: serializeChat,
            initial: { state: { chat }, description: "Release" },
          }),
        );
        yield* Effect.scoped(
          registry.backend.open({
            path: "/lark/mail/me/retained",
            state: EmailData,
            message: Schema.Never,
            serialize: JSON.stringify,
            initial: { state: email, description: "Email" },
          }),
        );
        let remoteReads = 0;
        const { queries, system } = yield* installLark({
          registry,
          mail: {
            getMailboxProfile: () => Effect.never,
            listIds: () => Effect.succeed([]),
            listMessages: () => Effect.die("Unexpected list"),
            getMessages: (_mailbox, ids) =>
              Effect.sync(() => {
                remoteReads++;
                return [{ ...email, messageId: ids[0]! }];
              }),
          },
        });
        const before = registry.backend.snapshot();
        assert.deepEqual(
          yield* queries.query({
            path: "/lark/im",
            command: "list_chats",
            args: { query: "release" },
          }),
          { items: [{ path: "/lark/im/chats/team", ...chat }], total: 1 },
        );
        assert.deepEqual(
          yield* queries.query({ path: "/lark/im", command: "summary", args: { chatId: "team" } }),
          { chat, summary: null },
        );
        assert.deepEqual(
          yield* queries.query({
            path: "/lark/mail",
            command: "read",
            args: { messageId: "retained" },
          }),
          email,
        );
        assert.equal(remoteReads, 0);
        assert.ok(
          (yield* system.select("/user/lark/im/chats/team").resolve().pipe(Effect.flip)) instanceof
            ActorNotFound,
        );
        assert.deepEqual(
          yield* queries.query({
            path: "/lark/mail",
            command: "read",
            args: { messageId: "remote" },
          }),
          { ...email, messageId: "remote" },
        );
        assert.equal(remoteReads, 1);
        assert.equal(registry.backend.get("/lark/mail/me/remote"), undefined);
        for (const path of ["/lark/im/chats/team", "/lark/mail/me/retained"])
          assert.deepEqual(registry.backend.get(path), before[path]);
        const unknown = yield* queries
          .query({ path: "/lark/im", command: "summary", args: { chatId: "unknown" } })
          .pipe(Effect.flip);
        assert.ok(unknown instanceof ContextQueryError);
        assert.equal(unknown._tag, "ContextQueryError");
        assert.equal(unknown.kind, "unavailable");
      }),
    ),
  );
});

test("invalid retained email data fails locally, and provider errors use the public query contract", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeTestContext();
        yield* Effect.scoped(
          registry.backend.open({
            path: "/lark/mail/me/bad",
            state: Schema.Struct({ invalid: Schema.Boolean }),
            message: Schema.Never,
            serialize: JSON.stringify,
            initial: { state: { invalid: true }, description: "Invalid email" },
          }),
        );
        const { queries } = yield* installLark({
          registry,
          mail: {
            getMailboxProfile: () => Effect.never,
            listIds: () => Effect.succeed([]),
            listMessages: () => Effect.die("Unexpected list"),
            getMessages: () => Effect.die("Invalid local data fell back to provider"),
          },
          im: {
            searchMessages: () => Effect.succeed([]),
            getChatSettings: () => Effect.succeed([]),
            listMessages: () =>
              Effect.fail(
                new LarkChatQueryError({ kind: "invalid-input", message: "Invalid history range" }),
              ),
          },
        });
        const local = yield* queries
          .query({ path: "/lark/mail", command: "read", args: { messageId: "bad" } })
          .pipe(Effect.flip);
        assert.ok(local instanceof ContextQueryError);
        assert.equal(local._tag, "ContextQueryError");
        assert.equal(local.kind, "failed");
        const remote = yield* queries
          .query({ path: "/lark/im", command: "messages", args: { chatId: "team" } })
          .pipe(Effect.flip);
        assert.ok(remote instanceof ContextQueryError);
        assert.equal(remote._tag, "ContextQueryError");
        assert.equal(remote.kind, "invalid-input");
      }),
    ),
  );
});

import { queryError, LarkCliError } from "../../src/lark/errors.js";
import { LarkResponseError } from "../../src/lark/mail/client.js";
test("public query errors retain stable kinds and keep provider/decoder causes private", () => {
  for (const kind of ["timeout", "unavailable", "failed"] as const) {
    const result = queryError(
      new LarkCliError({
        kind,
        message: "PRIVATE_PROVIDER_DIAGNOSTIC",
        cause: new Error("PRIVATE_CAUSE"),
      }),
    );
    assert.equal(result.kind, kind);
    assert.ok(!JSON.stringify(result).includes("PRIVATE"));
  }
  const result = queryError(
    new LarkResponseError({ cause: new Error("PRIVATE_PROVIDER_DIAGNOSTIC") }),
  );
  assert.equal(result.kind, "failed");
  assert.equal(result.message, "Unable to read Lark data");
});
