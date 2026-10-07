import type { ApprovalReply } from "../src/approvals/actor.js";
import { AgentConversations } from "@aster/agent";
import { testConversations } from "./conversation-fixtures.js";
import { goalWorkflowLayer } from "./workflow-fixtures.js";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem } from "@aster/actor";
import { Clock, Deferred, Effect, Fiber, Layer, Stream } from "effect";
import { TestClock } from "effect/testing";
import {
  ApprovalQueueActor,
  approvalEntries,
  ApplicationError,
  ContextRegistry,
  ExternalAgents,
  GoalsRootActor,
  SignalActor,
  SignalDefinitions,
  makeApplicationApi,
} from "../src/index.js";
import { makeContextRegistry } from "../src/testing/context.js";

const definition = {
  slug: "review",
  trigger: { _tag: "Context", when: "now" },
  task: { _tag: "Goal", target: "/goals/personal", text: "Task" },
} as const;
test("Goal API waits for durable input and history; stopped roots fail instead of accepting", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const underlying = testConversations();
        const history = {
          ...underlying,
          append: (goal: string, requestId: string, kind: string, data: unknown) =>
            Effect.gen(function* () {
              if (JSON.stringify(data).includes("Durable input")) {
                yield* Deferred.succeed(entered, undefined);
                yield* Deferred.await(release);
              }
              return yield* underlying.append(goal, requestId, kind, data);
            }),
        };
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(ContextRegistry, registry),

            Layer.succeed(ExternalAgents, {}),
            goalWorkflowLayer({
              definitions: [{ slug: "project", description: "Project" }],
              history,
              reasoner: { plan: () => Effect.never },
            }),
          ),
        );
        const goalActivation = yield* Deferred.make<void>();
        const root = yield* system.spawn("goals", GoalsRootActor, { metadata: { goalActivation } });
        yield* root.awaitStarted;
        const api = makeApplicationApi({
          registry,
          goals: root,
          conversations: history,
          inspect: system.inspect(),
        });
        const sending = yield* api.goals
          .sendMessage("project", "Durable input")
          .pipe(Effect.forkScoped);
        yield* Deferred.await(entered);
        assert.equal(sending.pollUnsafe(), undefined);
        assert.equal(
          (yield* history.read("/goals/project")).some((entry) =>
            JSON.stringify(entry.data).includes("Durable input"),
          ),
          false,
        );
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(sending);
        assert.equal(
          (registry.get("/goals/project")!.state as { inputs: { status: string }[] }).inputs.some(
            (input) => input.status === "pending",
          ),
          true,
        );
        assert.equal(
          (yield* history.read("/goals/project")).some((entry) =>
            JSON.stringify(entry.data).includes("Durable input"),
          ),
          true,
        );
        yield* api.goals.end("project");
        assert.equal(
          (registry.get("/goals/project")!.state as { status: string }).status,
          "completed",
        );
        yield* system.stop(root);
        const clock = yield* TestClock.make();
        const missing = yield* api.goals
          .sendMessage("project", "Lost")
          .pipe(Effect.provideService(Clock.Clock, clock), Effect.result, Effect.forkScoped);
        yield* clock.adjust("31 seconds");
        const result = yield* Fiber.join(missing);
        assert.equal(result._tag, "Failure");
        if (result._tag === "Failure") {
          assert.ok(result.failure instanceof ApplicationError);
          assert.equal(result.failure.kind, "unavailable");
        }
      }),
    ),
  );
});

test("invalid option stays pending and can be corrected without losing the original request", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(Layer.succeed(ContextRegistry, registry)),
        );
        const queue = yield* system.spawn("approvals", ApprovalQueueActor);
        yield* queue.tell({
          _tag: "Enqueue",
          entry: {
            id: "choice",
            target: "/user/recipient",
            contextPath: "/delegations/one",
            status: "pending",
            kind: "input",
            request: {
              id: "choice",
              kind: "input",
              prompt: "Pick",
              questions: [{ id: "q", prompt: "Pick one", options: ["A", "B"], multiple: false }],
            },
          },
        });
        const invalid = yield* queue.ask<ApprovalReply>((replyTo) => ({
          _tag: "Resolve",
          id: "choice",
          response: { answers: { q: ["invalid"] } },
          replyTo,
        }));
        assert.equal(invalid._tag, "Rejected");
        if (invalid._tag === "Rejected") assert.match(invalid.error.message, /offered option/);
        assert.equal(approvalEntries(registry)[0]!.status, "pending");
        assert.deepEqual(
          yield* queue.ask((replyTo) => ({
            _tag: "Resolve",
            id: "choice",
            response: { text: "A", answers: {} },
            replyTo,
          })),
          { _tag: "Accepted" },
        );
        assert.deepEqual(approvalEntries(registry)[0]!.response?.answers, { q: ["A"] });
      }),
    ),
  );
});

test("malformed Signal delivery state stops before recovery writes or execution", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        let saves = 0;
        const malformed = {
          path: "/signals/review",
          description: "Signal",
          state: {
            ...definition,
            status: "active",
            version: 1,
            nextDue: 123,
          },
          messages: [],
        };
        const registry = yield* makeContextRegistry({
          loadAll: () => [{ snapshot: { ...malformed, revision: 0 }, events: [] }],
          save: () => {
            saves++;
          },
        });
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(ContextRegistry, registry),

            Layer.succeed(ExternalAgents, {}),
            Layer.succeed(SignalDefinitions, [definition]),
            Layer.succeed(AgentConversations, testConversations()),
          ),
        );
        const stopped = yield* Stream.runHead(
          system.events.pipe(Stream.filter((event) => event._tag === "ActorStopped")),
        ).pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        yield* system.spawn("review", SignalActor, {
          supervision: () => "stop",
          metadata: { contextPath: "/signals/review" },
        });
        const event = yield* Fiber.join(stopped).pipe(Effect.timeout("2 seconds"));
        assert.equal(event._tag, "Some");
        if (event._tag === "Some" && event.value._tag === "ActorStopped")
          assert.ok(event.value.cause);
        assert.equal(saves, 0);
        assert.deepEqual(registry.get(malformed.path), { ...malformed, revision: 0 });
      }),
    ),
  );
});
