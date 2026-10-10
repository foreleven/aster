import { serializeFixtureState } from "@aster/core/testing";
import { DurableContext } from "@aster/core";
import { Schema } from "effect";
import { TaskSnapshot } from "@aster/core";
import { AccountProfile } from "@aster/integrations/lark/testing";
import { MailboxProfile } from "@aster/integrations/lark/testing";
import { EmailData } from "@aster/integrations/lark/testing";
import {
  ApprovalQueueActor,
  ContextCaptures,
  ExternalAgents,
  MemoryActor,
  MemoryBackend,
  type ContextInput,
  type ContextCapture as MemoryCapture,
} from "@aster/core";
import { makeTestContext, taskCapture } from "@aster/core/testing";
import { larkCaptures } from "@aster/integrations/lark/testing";
import { testConversations } from "./conversation-fixtures.js";

import { ActorSystem } from "@aster/actor";
import assert from "node:assert/strict";
import { test } from "node:test";

import { Effect, Layer } from "effect";

const source = (subject: string): ContextInput & { text: string } => ({
  text: serializeFixtureState({
    messageId: "test",
    mailbox: "me",
    from: "Alice",
    subject,
    bodyPlainText: "Please review",
    attachments: [],
  }),
  path: "/lark/mail/me/test",
  description: "An email in Lark mailbox me",
  state: {
    messageId: "test",
    mailbox: "me",
    from: "Alice",
    subject,
    bodyPlainText: "Please review",
    attachments: [],
  },
  messages: [],
});

const waitFor = (predicate: () => boolean, label = "processing") =>
  Effect.gen(function* () {
    for (let i = 0; i < 200; i++) {
      if (predicate()) return;
      yield* Effect.sleep(5);
    }
    return yield* Effect.die(new Error(`Expected ${label} did not finish`));
  });

test("admitted Task Runs capture activity, using the evaluated source snapshot", async () => {
  const result = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeTestContext();
        const capturesPolicy = yield* ContextCaptures;
        const conversations = testConversations();
        yield* capturesPolicy.register([...larkCaptures(true), taskCapture(conversations)]);

        yield* registry.register("/lark", {
          state: Schema.Struct({ account: Schema.optional(AccountProfile) }),
          serialize: (state) => {
            const account = Schema.decodeUnknownSync(
              Schema.Struct({ account: Schema.optional(AccountProfile) }),
            )(state).account;
            return account ? JSON.stringify(account) : "";
          },
          message: Schema.Never,
        });
        yield* registry.register("/lark/mail", {
          state: Schema.Struct({
            mailbox: Schema.String,
            profile: Schema.optional(MailboxProfile),
          }),
          serialize: (state) => {
            const profile = Schema.decodeUnknownSync(
              Schema.Struct({ profile: Schema.optional(MailboxProfile) }),
            )(state).profile;
            return profile ? JSON.stringify(profile) : "";
          },
          message: Schema.Never,
        });
        yield* registry.register("/lark/mail/me/test", {
          state: EmailData,
          message: Schema.Never,
          events: "state",
          serialize: (state) => JSON.stringify(state),
        });
        const captures: MemoryCapture[] = [];
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.mergeAll(Layer.succeed(DurableContext, registry.backend)),
            Layer.succeed(ContextCaptures, capturesPolicy),
            Layer.succeed(ExternalAgents, {}),
            Layer.succeed(MemoryBackend, {
              description: "Memory",
              retrieval: "bm25",
              capture: (input) =>
                Effect.sync(() => {
                  captures.push(structuredClone(input));
                }),
              recall: { search: () => Effect.succeed([]), expand: () => Effect.succeed([]) },
              drain: Effect.void,
            }),
          ),
        );
        const memory = yield* system.spawn("memory", MemoryActor);
        yield* memory.awaitStarted;
        yield* system.spawn("approvals", ApprovalQueueActor);
        yield* registry.commit(
          {
            text: serializeFixtureState({ mailbox: "me" }),
            path: "/lark/mail",
            description: "Work mailbox",
            state: { mailbox: "me" },
            messages: [],
          },
          { expectedRevision: registry.get("/lark/mail")?.revision ?? 0 },
        );
        yield* registry.commit(source("Unconfirmed candidate"), {
          expectedRevision: registry.get("/lark/mail/me/test")?.revision ?? 0,
        });
        assert.equal(captures.length, 0, "Context candidate must not write memory");
        yield* registry.commit(source("Confirmed request"), {
          expectedRevision: registry.get("/lark/mail/me/test")?.revision ?? 0,
        });
        const confirmed = registry.backend.get("/lark/mail/me/test")!;
        const taskPath = "/tasks/" + "a".repeat(64);
        const input = {
          requestId: "task",
          source: "/signals/review",
          target: taskPath,
          createdAt: "2026-10-01T00:00:00Z",
          agent: "test",
          task: { instructions: "Review", input: [] },
          replyTo: "/goals/personal",
          evidence: confirmed,
          remainingAgentTurns: 3,
        };
        const entry = yield* conversations.append(taskPath, "task", "task.admission", input);
        yield* registry.register(taskPath, { state: TaskSnapshot, message: Schema.Never });
        yield* registry.commit(
          {
            text: serializeFixtureState({
              status: "ready",
              inputs: [
                {
                  requestId: "task",
                  entryId: entry.id,
                  receipt: { requestId: "task", revision: 1 },
                  status: "pending",
                },
              ],
              admission: {
                source: "/signals/review",
                agent: "test",
                replyTo: "/goals/personal",
                remainingAgentTurns: 3,
              },
            }),
            path: taskPath,
            description: "Review",
            messages: [],
            state: {
              status: "ready",
              inputs: [
                {
                  requestId: "task",
                  entryId: entry.id,
                  receipt: { requestId: "task", revision: 1 },
                  status: "pending",
                },
              ],
              admission: {
                source: "/signals/review",
                agent: "test",
                replyTo: "/goals/personal",
                remainingAgentTurns: 3,
              },
            },
          },
          { expectedRevision: 0 },
        );
        // A newer source value exists before memory finishes processing the Run notification.
        yield* registry.commit(
          {
            ...source("Later source update"),
            description: "An email in Lark mailbox me",
          },
          { expectedRevision: registry.get("/lark/mail/me/test")?.revision ?? 0 },
        );
        yield* waitFor(() => captures.length >= 1, "Memory capture");
        return { captures, snapshot: registry.snapshot() };
      }),
    ).pipe(Effect.provide(ContextCaptures.layer)),
  );
  assert.equal(result.captures.length, 1);
  const capture = result.captures[0]!;
  assert.match(capture.sessionId, /^\/tasks\/[a-f0-9]{64}:text:[a-f0-9]{64}$/);
  assert.equal(JSON.parse(capture.records[1]!.text).subject, "Confirmed request");
  assert.equal("type" in result.snapshot[capture.records[0]!.path]!, false);
});

test("discovered account and mailbox identities use separate sessions; no capture of empty initialization", async () => {
  const result = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeTestContext();
        const capturesPolicy = yield* ContextCaptures;
        const conversations = testConversations();
        yield* capturesPolicy.register([...larkCaptures(true), taskCapture(conversations)]);

        yield* registry.register("/lark", {
          state: Schema.Struct({ account: Schema.optional(AccountProfile) }),
          serialize: (state) => {
            const account = Schema.decodeUnknownSync(
              Schema.Struct({ account: Schema.optional(AccountProfile) }),
            )(state).account;
            return account ? JSON.stringify(account) : "";
          },
          message: Schema.Never,
        });
        yield* registry.register("/lark/mail", {
          state: Schema.Struct({
            mailbox: Schema.String,
            profile: Schema.optional(MailboxProfile),
          }),
          serialize: (state) => {
            const profile = Schema.decodeUnknownSync(
              Schema.Struct({ profile: Schema.optional(MailboxProfile) }),
            )(state).profile;
            return profile ? JSON.stringify(profile) : "";
          },
          message: Schema.Never,
        });
        yield* registry.register("/lark/mail/me/test", {
          state: EmailData,
          message: Schema.Never,
          events: "state",
          serialize: (state) => JSON.stringify(state),
        });
        const captures: MemoryCapture[] = [];
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.mergeAll(Layer.succeed(DurableContext, registry.backend)),
            Layer.succeed(ContextCaptures, capturesPolicy),
            Layer.succeed(MemoryBackend, {
              description: "Memory",
              retrieval: "bm25",
              capture: (input) =>
                Effect.sync(() => {
                  captures.push(structuredClone(input));
                }),
              recall: { search: () => Effect.succeed([]), expand: () => Effect.succeed([]) },
              drain: Effect.void,
            }),
          ),
        );
        const memory = yield* system.spawn("memory", MemoryActor);
        yield* memory.awaitStarted;
        yield* registry.commit(
          {
            text: serializeFixtureState({}),
            path: "/lark",
            description: "Work account",
            state: {},
            messages: [],
          },
          { expectedRevision: registry.get("/lark")?.revision ?? 0 },
        );
        yield* registry.commit(
          {
            text: serializeFixtureState({ mailbox: "me" }),
            path: "/lark/mail",
            description: "Work mailbox",
            state: { mailbox: "me" },
            messages: [],
          },
          { expectedRevision: registry.get("/lark/mail")?.revision ?? 0 },
        );
        yield* Effect.sleep(10);
        assert.equal(captures.length, 0);
        yield* registry.commit(
          {
            text: serializeFixtureState({
              mailbox: "me",
              profile: { address: "test@example.com", name: "Work mailbox" },
            }),
            path: "/lark/mail",
            description: "Work mailbox",
            state: {
              mailbox: "me",
              profile: { address: "test@example.com", name: "Work mailbox" },
            },
            messages: [],
          },
          { expectedRevision: registry.get("/lark/mail")?.revision ?? 0 },
        );
        yield* waitFor(() => captures.length === 1);
        yield* registry.commit(
          {
            text: serializeFixtureState({
              account: {
                openId: "ou_test",
                name: "Test",
                email: "test@example.com",
                enterpriseEmail: "",
              },
            }),
            path: "/lark",
            description: "Work account",
            state: {
              account: {
                openId: "ou_test",
                name: "Test",
                email: "test@example.com",
                enterpriseEmail: "",
              },
            },
            messages: [],
          },
          { expectedRevision: registry.get("/lark")?.revision ?? 0 },
        );
        yield* waitFor(() => captures.length === 2);
        return captures;
      }),
    ).pipe(Effect.provide(ContextCaptures.layer)),
  );
  assert.deepEqual(
    result.map((input) => input.records.map((r) => r.path)),
    [["/lark/mail"], ["/lark"]],
  );
  assert.notEqual(result[0]!.sessionId, result[1]!.sessionId);
});
