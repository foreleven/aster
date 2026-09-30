import { ContextDescriptionError } from "@aster/core";
import { ApprovalQueueActor, ExternalAgents, TaskPreparation } from "@aster/core";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem } from "@aster/actor";
import { MemoryActor, MemoryRuntime, type MemoryCapture } from "@aster/integrations";
import { ContextRegistry, makeContextRegistry, type ContextRecord } from "@aster/core";
import { LarkRootActor, LarkEmailChannelActor, LarkMailMessageActor } from "@aster/integrations";
import { Deferred, Effect, Fiber, Layer, Stream } from "effect";
import { detectSignals } from "@aster/core";
import { makeContextProcessor } from "@aster/core";
import { SignalDefinitions, SignalRootActor } from "@aster/core";

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

const waitFor = (predicate: () => boolean) =>
  Effect.gen(function* () {
    for (let i = 0; i < 200; i++) {
      if (predicate()) return;
      yield* Effect.sleep(5);
    }
    return yield* Effect.die(new Error("Expected processing did not finish"));
  });

test("only confirmed Signal Runs capture activity, using the evaluated source snapshot", async () => {
  const result = await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        yield* registry.register("/lark", LarkRootActor.context);
        yield* registry.register("/lark/mail", LarkEmailChannelActor.context);
        yield* registry.register("/lark/mail/me/test", LarkMailMessageActor.context);
        const captures: MemoryCapture[] = [];
        const definitions = [
          {
            slug: "review",
            when: "review request",
            task: "Review",
            agent: "test",
            mode: "confirm" as const,
          },
        ];
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(ContextRegistry, registry),
            Layer.succeed(ExternalAgents, {}),
            Layer.succeed(TaskPreparation, {
              prepare: (definition) =>
                Effect.sync(() => ({ instructions: definition.task, input: [] })),
              ready: () => Effect.sync(() => true),
            }),
            Layer.succeed(SignalDefinitions, definitions),
            Layer.succeed(MemoryRuntime, {
              config: { description: "Memory", dataDir: "/tmp", port: 3111, autoCompress: false },
              client: {
                capture: async (input) => {
                  captures.push(structuredClone(input));
                },
                search: async () => ({ mode: "compact", results: [] }),
                expand: async () => ({ mode: "expanded", results: [], truncated: false }),
                drain: async () => {},
                close: () => {},
              },
            }),
          ),
        );
        const memory = yield* system.spawn("memory", MemoryActor);
        yield* system.spawn("approvals", ApprovalQueueActor);
        const signals = yield* system.spawn("signals", SignalRootActor);
        const newerSourceWritten = yield* Deferred.make<void>();
        let evaluated = 0;
        const descriptionInputs: unknown[] = [];
        const processor = makeContextProcessor(
          registry,
          (input) =>
            Deferred.await(newerSourceWritten).pipe(
              Effect.andThen(memory.tell({ _tag: "Capture", input })),
            ),
          (record, snapshot) =>
            detectSignals(
              record.path,
              snapshot,
              definitions,
              () => Effect.succeed(definitions),
              signals,
              (path, _candidates, snapshot) =>
                Effect.sync(() => {
                  evaluated++;
                  return (snapshot[path]!.state as { subject: string }).subject ===
                    "Confirmed request"
                    ? ["review"]
                    : [];
                }),
            ),
          (identity) =>
            Effect.sync(() => {
              descriptionInputs.push(identity);
              return `Fixed ${identity.identity}`;
            }),
        );
        const listener = yield* Stream.runForEach(registry.changes, processor).pipe(
          Effect.forkScoped,
        );
        yield* Effect.yieldNow;
        yield* registry.set({
          path: "/lark/mail",
          description: "Work mailbox",
          state: { mailbox: "me" },
          messages: [],
        });
        yield* registry.set(source("Unconfirmed candidate"));
        yield* waitFor(() => evaluated === 1);
        assert.equal(captures.length, 0, "Jev candidate must not write memory");
        yield* registry.set(source("Confirmed request"));
        yield* waitFor(() => Object.keys(registry.snapshot()).some((p) => p.includes("/runs/")));
        // A newer source value exists before memory finishes processing the Run notification.
        yield* registry.set({
          ...source("Later source update"),
          description: "Fixed An email in a Lark mailbox",
        });
        yield* Deferred.succeed(newerSourceWritten, undefined);
        yield* waitFor(() => captures.length >= 1);
        yield* Fiber.interrupt(listener);
        return { captures, descriptionInputs, snapshot: registry.snapshot() };
      }),
    ),
  );
  assert.equal(result.captures.length, 1);
  const capture = result.captures[0]!;
  assert.match(capture.sessionId, /^\/signals\/review\/runs\//);
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
        yield* registry.register("/lark", LarkRootActor.context);
        yield* registry.register("/lark/mail", LarkEmailChannelActor.context);
        yield* registry.register("/lark/mail/me/test", LarkMailMessageActor.context);
        const captures: MemoryCapture[] = [];
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(ContextRegistry, registry),
            Layer.succeed(MemoryRuntime, {
              config: { description: "Memory", dataDir: "/tmp", port: 3111, autoCompress: false },
              client: {
                capture: async (input) => {
                  captures.push(structuredClone(input));
                },
                search: async () => ({ mode: "compact", results: [] }),
                expand: async () => ({ mode: "expanded", results: [], truncated: false }),
                drain: async () => {},
                close: () => {},
              },
            }),
          ),
        );
        const memory = yield* system.spawn("memory", MemoryActor);
        const processChange = makeContextProcessor(
          registry,
          (input) => memory.tell({ _tag: "Capture", input }),
          () => Effect.void,
          () => Effect.die(new Error("Descriptions are provided by this fixture")),
        );
        const listener = yield* Stream.runForEach(registry.changes, processChange).pipe(
          Effect.forkScoped,
        );
        yield* Effect.yieldNow;
        yield* registry.set({
          path: "/lark",
          description: "Work account",
          state: {},
          messages: [],
        });
        yield* registry.set({
          path: "/lark/mail",
          description: "Work mailbox",
          state: { mailbox: "me" },
          messages: [],
        });
        yield* Effect.sleep(10);
        assert.equal(captures.length, 0);
        yield* registry.set({
          path: "/lark/mail",
          description: "Work mailbox",
          state: { mailbox: "me", profile: { address: "test@example.com", name: "Work mailbox" } },
          messages: [],
        });
        yield* waitFor(() => captures.length === 1);
        yield* registry.set({
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
        });
        yield* waitFor(() => captures.length === 2);
        yield* Fiber.interrupt(listener);
        return captures;
      }),
    ),
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
          record: { path: "/failed", description: "", state: {}, messages: [] },
        },
        {
          path: "/next",
          created: true,
          stateChanged: true,
          record: { path: "/next", description: "", state: {}, messages: [] },
        },
      ),
      isolateContextChange((change) =>
        change.path === "/failed"
          ? Effect.fail(
              new ContextDescriptionError({
                path: change.path,
                message: "Internal Agent returned no structured result",
                cause: undefined,
              }),
            )
          : Effect.sync(() => {
              handled.push(change.path);
            }),
      ),
    ),
  );
  assert.deepEqual(handled, ["/next"]);
});
