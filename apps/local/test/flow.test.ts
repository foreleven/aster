import { gateStub, summaryStub } from "./summary-fixtures.js";

import { Models } from "@aster/agent";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem } from "@aster/actor";
import { ContextRegistry, ContextQueries, contextSpawnOptions } from "@aster/core";
import { makeContextRegistry } from "@aster/core/testing";
import {
  ChatSummarizer,
  ImAgentQueue,
  ChatSummaryGate,
  LarkChatService,
  LarkConfig,
  LarkIntegration,
  LarkAccountCli,
  LarkMailCli,
  LarkRootActor,
  LarkEmailChannelActor,
  type EmailData,
} from "@aster/integrations";
import { Deferred, Effect, Fiber, Layer, Option, Stream } from "effect";
import { SystemOneClient } from "@aster/core";

const email: EmailData = {
  messageId: "new-id",
  mailbox: "me",
  from: "Alice alice@example.com",
  subject: "Please review the draft",
  bodyPlainText: "Could you review the draft today?",
  attachments: [],
};

test("a code-registered Lark root starts without YAML entries and creates its own mail child", async () => {
  const snapshot = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(ContextRegistry, registry),
            ContextQueries.layer,
            Layer.succeed(ImAgentQueue, { run: (_id, execute) => execute }),
            Layer.succeed(ChatSummaryGate, { needed: gateStub(async () => true) }),
            Layer.succeed(LarkChatService, {
              searchMessages: () => Effect.succeed([]),
              getChatSettings: () => Effect.succeed([]),
            }),
            Layer.succeed(ChatSummarizer, {
              summarize: summaryStub(async () => {
                throw new Error("No chats in mail test");
              }),
            }),
            Layer.empty,
            Models.layer([]),
            Layer.succeed(SystemOneClient, {
              systemOne: () =>
                Effect.sync(() => {
                  throw new Error("Mail-only flow must not call System One");
                }),
            }),
            LarkIntegration.services,
            Layer.succeed(LarkAccountCli, {
              getAccount: () =>
                Effect.succeed({
                  openId: "ou_test",
                  name: "Test",
                  email: "test@example.com",
                  enterpriseEmail: "",
                }),
            }),
            Layer.succeed(LarkMailCli, {
              getMailboxProfile: () =>
                Effect.succeed({ address: "test@example.com", name: "Mail" }),
              listIds: () => Effect.succeed([]),
              getMessages: () => Effect.succeed([]),
            }),
          ),
        );
        const ready = yield* Stream.runHead(
          Stream.filter(
            registry.changes,
            (change) => change.record.path === "/lark/mail" && "profile" in change.record.state,
          ),
        ).pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        yield* system.spawn("lark", LarkRootActor);
        yield* Fiber.join(ready).pipe(Effect.timeout("2 seconds"));
        return registry.snapshot();
      }),
    ),
  );
  assert.equal(snapshot["/lark"]?.description, "My Lark account");
  assert.deepEqual(snapshot["/lark/mail"]?.state, {
    mailbox: "me",
    profile: { address: "test@example.com", name: "Mail" },
  });
  assert.equal("type" in snapshot["/lark/mail"]!, false);
});

test("Lark channel publishes today’s startup mail as an email Context", async () => {
  const result = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(ContextRegistry, registry),
            ContextQueries.layer,
            Layer.succeed(ImAgentQueue, { run: (_id, execute) => execute }),
            Layer.succeed(ChatSummaryGate, { needed: gateStub(async () => true) }),
            Layer.succeed(LarkChatService, {
              searchMessages: () => Effect.succeed([]),
              getChatSettings: () => Effect.succeed([]),
            }),
            Layer.succeed(ChatSummarizer, {
              summarize: summaryStub(async () => {
                throw new Error("No chats in mail test");
              }),
            }),
            Layer.succeed(LarkConfig, {
              profile: "test",
              description: "Work account",
              mail: { mailbox: "me", description: "Work mailbox", pollIntervalMs: 10 },
            }),
            Layer.empty,
            Layer.succeed(LarkAccountCli, {
              getAccount: () =>
                Effect.succeed({
                  openId: "ou_test",
                  name: "Test",
                  email: "test@example.com",
                  enterpriseEmail: "",
                }),
            }),
            Layer.succeed(LarkMailCli, {
              getMailboxProfile: () =>
                Effect.succeed({ address: "test@example.com", name: "Work mailbox" }),
              listIds: () => Effect.succeed(["new-id"]),
              getMessages: (_mailbox, ids) => Effect.succeed(ids.includes("new-id") ? [email] : []),
            }),
          ),
        );
        const change = yield* Stream.runHead(
          Stream.filter(registry.changes, (item) => item.record.path === "/lark/mail/me/new-id"),
        ).pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        yield* system.spawn("lark", LarkRootActor);
        const event = yield* Fiber.join(change).pipe(Effect.timeout("2 seconds"));
        const paths = Object.keys(registry.snapshot());
        return { event, paths };
      }),
    ),
  );
  assert.equal(Option.isSome(result.event), true);
  if (Option.isSome(result.event)) {
    assert.equal(result.event.value.record.path, "/lark/mail/me/new-id");
  }
  assert.equal(result.paths.includes("/lark/mail/me/old-id"), false);
  assert.equal(result.paths.includes("/lark/mail/me/new-id"), true);
});

for (const mailbox of ["me", "other"])
  test(`mailbox restart preserves only the matching profile before remote refresh: ${mailbox}`, async () => {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const saved = {
            path: "/lark/mail",
            description: "Mailbox",
            revision: 7,
            state: { mailbox: "me", profile: { address: "saved@example.com", name: "Saved" } },
            messages: [],
          };
          const registry = yield* makeContextRegistry({
            loadAll: () => [{ snapshot: saved, events: [] }],
            save: () => {},
          });
          const entered = yield* Deferred.make<void>();
          const system = yield* ActorSystem.make().pipe(
            ActorSystem.provide(
              Layer.succeed(ContextRegistry, registry),
              ContextQueries.layer,
              Layer.succeed(LarkConfig, {
                description: "Account",
                mail: { mailbox, description: "Mailbox", pollIntervalMs: 60_000 },
              }),
              Layer.succeed(LarkMailCli, {
                getMailboxProfile: () =>
                  Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
                listIds: () => Effect.succeed([]),
                getMessages: () => Effect.succeed([]),
              }),
            ),
          );
          yield* system.spawn("mail", LarkEmailChannelActor, contextSpawnOptions("/lark/mail"));
          yield* Deferred.await(entered);
          const restored = registry.get("/lark/mail")!;
          if (mailbox === "me") assert.deepEqual(restored, saved);
          else {
            assert.deepEqual(restored.state, { mailbox: "other" });
            assert.equal(restored.revision, 8);
          }
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
  });
