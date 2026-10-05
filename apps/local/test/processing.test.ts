import { ContextCaptures, ContextDescriptions, makeContextMaintenance } from "@aster/core";
import { testConversations } from "./conversation-fixtures.js";
import { taskCapture } from "@aster/core/testing";
import { larkCaptures, larkDescriptions, larkContextViews } from "@aster/integrations";
import { TaskActor, DEFAULT_EXECUTOR_PROMPT } from "@aster/core";
import { ContextDescriptionError } from "@aster/core";
import { ApprovalQueueActor, ExternalAgents } from "@aster/core";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem } from "@aster/actor";
import { MemoryActor } from "@aster/core";
import { MemoryBackend, type ContextCapture as MemoryCapture } from "@aster/core";
import { ContextRegistry, type ContextRecord } from "@aster/core";
import { makeContextRegistry } from "@aster/core/testing";
import { LarkRootActor, LarkEmailChannelActor, LarkMailMessageActor } from "@aster/integrations";
import { Deferred, Effect, Fiber, Layer, Stream } from "effect";

const source = (subject: string): ContextRecord => ({
  path: "/lark/mail/me/test",
  description: "",
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
        const descriptions = yield* ContextDescriptions;
        yield* descriptions.register(larkDescriptions);

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
        yield* system.spawn("approvals", ApprovalQueueActor);
        const newerSourceWritten = yield* Deferred.make<void>();
        const descriptionInputs: unknown[] = [];
        const processor = makeContextMaintenance({
          registry,
          capture: (input) =>
            Deferred.await(newerSourceWritten).pipe(
              Effect.andThen(
                memory
                  .ask<void>((replyTo) => ({ _tag: "Capture", input, replyTo }))
                  .pipe(Effect.orDie),
              ),
            ),
          captures: capturesPolicy,
          descriptions,
          describe: (identity) =>
            Effect.sync(() => {
              descriptionInputs.push(identity);
              return `Fixed ${identity.identity}`;
            }),
        });
        const listener = yield* Stream.runForEach(registry.changes, processor).pipe(
          Effect.forkScoped,
        );
        yield* Effect.yieldNow;
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
        yield* waitFor(
          () => !!registry.get("/lark/mail/me/test")?.description,
          "description initialization",
        );
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
              status: "awaiting-confirmation",
              inputs: [
                {
                  requestId: "task",
                  entryId: entry.id,
                  receipt: { requestId: "task", revision: 1 },
                  status: "pending",
                },
              ],
              executorPrompt: DEFAULT_EXECUTOR_PROMPT,
              admission: {
                entryId: entry.id,
                receipt: { requestId: "task", revision: 1 },
                input: {
                  requestId: "task",
                  source: "/signals/review",
                  target: taskPath,
                  createdAt: "2026-10-01T00:00:00Z",
                  agent: "test",
                  task: { instructions: "Review", input: [] },
                  replyTo: "/goals/personal",
                  evidence: confirmed,
                  causal: { rootRequestId: "task", remainingAgentTurns: 3 },
                },
              },
            },
          },
          { expectedRevision: 0 },
        );
        // A newer source value exists before memory finishes processing the Run notification.
        yield* registry.commit(
          {
            ...source("Later source update"),
            description: "Fixed An email in a Lark mailbox",
          },
          { expectedRevision: registry.get("/lark/mail/me/test")?.revision ?? 0 },
        );
        yield* Deferred.succeed(newerSourceWritten, undefined);
        yield* waitFor(() => captures.length >= 1, "Memory capture");
        yield* Fiber.interrupt(listener);
        return { captures, descriptionInputs, snapshot: registry.snapshot() };
      }),
    ).pipe(Effect.provide(Layer.mergeAll(ContextCaptures.layer, ContextDescriptions.layer))),
  );
  assert.equal(result.captures.length, 1);
  const capture = result.captures[0]!;
  assert.match(capture.sessionId, /^\/tasks\/[a-f0-9]{64}:trigger$/);
  assert.equal((capture.records[1]!.state as { subject: string }).subject, "Confirmed request");
  assert.equal("type" in result.snapshot[capture.records[0]!.path]!, false);
  assert.deepEqual(result.descriptionInputs[0], {
    path: "/lark/mail/me/test",
    identity: "An email in a Lark mailbox",
    parentDescription: "Work mailbox",
  });
  assert.equal(
    result.descriptionInputs.some((input) => JSON.stringify(input).includes("Please review")),
    false,
  );
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
        const descriptions = yield* ContextDescriptions;
        yield* descriptions.register(larkDescriptions);

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
        const processChange = makeContextMaintenance({
          registry,
          capture: (input) =>
            memory.ask<void>((replyTo) => ({ _tag: "Capture", input, replyTo })).pipe(Effect.orDie),
          captures: capturesPolicy,
          descriptions,
          describe: () => Effect.die(new Error("Descriptions are provided by this fixture")),
        });
        const listener = yield* Stream.runForEach(registry.changes, processChange).pipe(
          Effect.forkScoped,
        );
        yield* Effect.yieldNow;
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
        yield* Fiber.interrupt(listener);
        return captures;
      }),
    ).pipe(Effect.provide(Layer.mergeAll(ContextCaptures.layer, ContextDescriptions.layer))),
  );
  assert.deepEqual(
    result.map((input) => input.records.map((r) => r.path)),
    [["/lark/mail"], ["/lark"]],
  );
  assert.notEqual(result[0]!.sessionId, result[1]!.sessionId);
});

test("a failed Context item does not terminate processing of later changes", async () => {
  const { isolateContextChange } = await import("@aster/core");
  const handled: string[] = [];
  await Effect.runPromise(
    Stream.runForEach(
      Stream.make(
        {
          path: "/failed",
          created: true,
          stateChanged: true,
          record: { revision: 0, path: "/failed", description: "", state: {}, messages: [] },
        },
        {
          path: "/next",
          created: true,
          stateChanged: true,
          record: { revision: 0, path: "/next", description: "", state: {}, messages: [] },
        },
      ),
      isolateContextChange((change) =>
        change.record.path === "/failed"
          ? Effect.fail(
              new ContextDescriptionError({
                path: change.record.path,
                message: "Internal Agent returned no structured result",
                cause: undefined,
              }),
            )
          : Effect.sync(() => {
              handled.push(change.record.path);
            }),
      ),
    ),
  );
  assert.deepEqual(handled, ["/next"]);
});
