import { ContextCaptures } from "@aster/core";
import { testConversations } from "./conversation-fixtures.js";
import { taskCapture } from "@aster/core/testing";
import { larkCaptures, larkContextViews } from "@aster/integrations";
import { TaskActor } from "@aster/core";
import { ApprovalQueueActor, ExternalAgents } from "@aster/core";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem } from "@aster/actor";
import { MemoryActor } from "@aster/core";
import { MemoryBackend, type ContextCapture as MemoryCapture } from "@aster/core";
import { ContextRegistry, type ContextInput } from "@aster/core";
import { makeContextRegistry } from "@aster/core/testing";
import { LarkRootActor, LarkEmailChannelActor, LarkMailMessageActor } from "@aster/integrations";
import { Effect, Layer } from "effect";

const source = (subject: string): ContextInput => ({
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
        const registry = yield* makeContextRegistry();
        yield* registry.views.register(larkContextViews);
        const capturesPolicy = yield* ContextCaptures;
        const conversations = testConversations();
        yield* capturesPolicy.register([...larkCaptures, taskCapture(conversations)]);

        yield* registry.register("/lark", LarkRootActor.context);
        yield* registry.register("/lark/mail", LarkEmailChannelActor.context);
        yield* registry.register("/lark/mail/me/test", LarkMailMessageActor.context);
        const captures: MemoryCapture[] = [];
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(ContextRegistry, registry),
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
        const confirmed = registry.reader.get("/lark/mail/me/test")!;
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
          causal: { rootRequestId: "task", remainingAgentTurns: 3 },
        };
        const entry = yield* conversations.append(taskPath, "task", "task.admission", input);
        yield* registry.register(taskPath, TaskActor.context);
        yield* registry.commit(
          {
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
                causal: { rootRequestId: "task", remainingAgentTurns: 3 },
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
  assert.match(capture.sessionId, /^\/tasks\/[a-f0-9]{64}:trigger$/);
  assert.equal((capture.records[1]!.state as { subject: string }).subject, "Confirmed request");
  assert.equal("type" in result.snapshot[capture.records[0]!.path]!, false);
});

test("discovered account and mailbox identities use separate sessions; no capture of empty initialization", async () => {
  const result = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        yield* registry.views.register(larkContextViews);
        const capturesPolicy = yield* ContextCaptures;
        const conversations = testConversations();
        yield* capturesPolicy.register([...larkCaptures, taskCapture(conversations)]);

        yield* registry.register("/lark", LarkRootActor.context);
        yield* registry.register("/lark/mail", LarkEmailChannelActor.context);
        yield* registry.register("/lark/mail/me/test", LarkMailMessageActor.context);
        const captures: MemoryCapture[] = [];
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(ContextRegistry, registry),
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
            path: "/lark",
            description: "Work account",
            state: {},
            messages: [],
          },
          { expectedRevision: registry.get("/lark")?.revision ?? 0 },
        );
        yield* registry.commit(
          {
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
