import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem, ActorTestKit } from "@aster/actor";
import { Clock, Deferred, Effect, Fiber, Layer, Stream } from "effect";
import { TestClock } from "effect/testing";
import {
  ApprovalQueueActor,
  approvalEntries,
  ApplicationError,
  ContextRegistry,
  ExternalAgents,
  GoalRuntime,
  GoalsRootActor,
  SignalRunActor,
  SignalActor,
  SignalDefinitions,
  contextSpawnOptions,
  makeApplicationApi,
  makeContextRegistry,
  makeMemoryGoalHistory,
  decideTaskOperation,
  type GoalCommand,
  type RunState,
} from "../src/index.js";
import { fakeAgent, preparationLayer } from "./fixtures.js";

const definition = {
  slug: "review",
  when: "now",
  task: "Task",
  agent: "test",
  mode: "confirm",
} as const;
const source = { path: "/source", description: "Source", state: {}, messages: [] };
const base = {
  signalSlug: "review",
  sourcePath: source.path,
  definition,
  source,
  task: { instructions: "Task", input: [] },
};

test("Goal API waits for durable history and evaluation intent; stopped roots fail instead of accepting", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const initialized = yield* Deferred.make<void>();
        const underlying = makeMemoryGoalHistory();
        const history = {
          ...underlying,
          append: (goal: string, message: Parameters<typeof underlying.append>[1]) =>
            Effect.gen(function* () {
              if (message.role === "user" && message.content === "Durable input") {
                yield* Deferred.succeed(entered, undefined);
                yield* Deferred.await(release);
              }
              return yield* underlying.append(goal, message);
            }),
        };
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(ContextRegistry, registry),
            preparationLayer,
            Layer.succeed(ExternalAgents, {}),
            Layer.succeed(GoalRuntime, {
              definitions: [{ slug: "project", description: "Project" }],
              history,
              reasoner: { plan: () => Effect.never },
              signals: () => [],
              reconcile: () => Deferred.succeed(initialized, undefined).pipe(Effect.as([])),
              deactivate: () => Effect.void,
            }),
          ),
        );
        const root = yield* system.spawn("goals", GoalsRootActor);
        yield* Deferred.await(initialized);
        const api = makeApplicationApi({
          registry,
          goals: root,
          history,
          inspect: system.inspect(),
        });
        const sending = yield* api.goals
          .sendMessage("project", "Durable input")
          .pipe(Effect.forkScoped);
        yield* Deferred.await(entered);
        assert.equal(sending.pollUnsafe(), undefined);
        assert.equal(
          (yield* history.read("project")).some(
            (entry) => entry.message.role === "user" && entry.message.content === "Durable input",
          ),
          false,
        );
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(sending);
        assert.equal(
          (registry.get("/goals/project")!.state as { pendingEvaluation: boolean })
            .pendingEvaluation,
          true,
        );
        assert.equal(
          (yield* history.read("project")).some(
            (entry) => entry.message.role === "user" && entry.message.content === "Durable input",
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

test("every persisted Run terminal outcome is replayed on restart and parent reattachment", async () => {
  const states = [
    "completed",
    "failed",
    "cancelled",
    "blocked",
    "rejected",
    "preparation-failed",
  ] as const;
  for (const status of states) {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          let saves = 0;
          const registry = yield* makeContextRegistry({
            loadAll: () => [
              {
                path: "/runs/review",
                description: "Run",
                state: { ...base, status, outcomeText: `Original ${status}` },
                messages: [],
              },
            ],
            save: () => {
              saves++;
            },
          });
          const system = yield* ActorSystem.make().pipe(
            ActorSystem.provide(
              Layer.succeed(ContextRegistry, registry),
              preparationLayer,
              Layer.succeed(ExternalAgents, {}),
            ),
          );
          const actor = yield* system.spawn(
            "run",
            SignalRunActor,
            contextSpawnOptions("/runs/review"),
          );
          for (let attachment = 0; attachment < 2; attachment++) {
            const parent = yield* ActorTestKit.probe<GoalCommand>();
            yield* actor.tell({ _tag: "Resume", path: "/runs/review", subscriber: parent.ref });
            const update = yield* parent.take().pipe(Effect.timeout("2 seconds"));
            assert.equal(update._tag, "SubmitInput");
            if (update._tag === "SubmitInput" && update.input._tag === "ExecutionFeedback") {
              yield* update.replyTo.tell({
                _tag: "Accepted",
                receipt: { requestId: update.requestId, revision: 1 },
              });
              assert.equal(update.input.status, status);
              assert.equal(update.input.text, `Original ${status}`);
              assert.equal(update.input.terminal, true);
            }
          }
          assert.equal(saves, 0);
        }),
      ),
    );
  }
});

test("Run keeps authoritative failure/cancellation distinct from uncertain external outcomes", async () => {
  for (const status of ["failed", "cancelled", "unknown"] as const) {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const registry = yield* makeContextRegistry({
            loadAll: () => [
              {
                path: "/runs/review",
                description: "Run",
                state: { ...base, status: "ready" },
                messages: [],
              },
            ],
            save: () => {},
          });
          const system = yield* ActorSystem.make().pipe(
            ActorSystem.provide(
              Layer.succeed(ContextRegistry, registry),
              preparationLayer,
              Layer.succeed(ExternalAgents, {
                test: fakeAgent({
                  status: () => Effect.succeed({ state: status, error: "Executor outcome" }),
                }),
              }),
            ),
          );
          const finished = yield* Stream.runHead(
            system.events.pipe(
              Stream.filter(
                (event) =>
                  event._tag === "CommandProcessed" &&
                  event.path === "/user/run" &&
                  event.commandTag === "Finished",
              ),
            ),
          ).pipe(Effect.forkScoped);
          yield* Effect.yieldNow;
          yield* system.spawn("run", SignalRunActor, contextSpawnOptions("/runs/review"));
          yield* Fiber.join(finished).pipe(Effect.timeout("2 seconds"));
          const saved = registry.get("/runs/review")!.state as RunState;
          assert.equal(saved.status, status === "unknown" ? "uncertain" : status);
          const decision = decideTaskOperation(
            [
              {
                id: "task",
                title: "Task",
                instructions: "Task",
                status: "open",
                revision: 1,
                evidence: [],
                createdAt: "now",
                updatedAt: "now",
                execution: { runPath: "/runs/review", status: saved.status, revision: 1 },
              },
            ],
            { operation: "task_execute", id: "task", revision: 1 },
            { at: "now", runPath: "/runs/new", execution: { status: saved.status, revision: 1 } },
          );
          assert.equal(decision._tag, "Success");
          if (decision._tag === "Success")
            assert.equal(decision.success._tag, status === "unknown" ? "Read" : "Execute");
        }),
      ),
    );
  }
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
        const invalid = yield* queue.ask<{ error?: string }>((replyTo) => ({
          _tag: "Resolve",
          id: "choice",
          response: { answers: { q: ["invalid"] } },
          replyTo,
        }));
        assert.match(invalid.error!, /offered option/);
        assert.equal(approvalEntries(registry)[0]!.status, "pending");
        assert.deepEqual(
          yield* queue.ask((replyTo) => ({
            _tag: "Resolve",
            id: "choice",
            response: { text: "A", answers: {} },
            replyTo,
          })),
          {},
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
            occurrences: [{ id: "one", text: "Pending", delivered: "false", source }],
          },
          messages: [],
        };
        const registry = yield* makeContextRegistry({
          loadAll: () => [malformed],
          save: () => {
            saves++;
          },
        });
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(ContextRegistry, registry),
            preparationLayer,
            Layer.succeed(ExternalAgents, {}),
            Layer.succeed(SignalDefinitions, [definition]),
          ),
        );
        const stopped = yield* Stream.runHead(
          system.events.pipe(Stream.filter((event) => event._tag === "ActorStopped")),
        ).pipe(Effect.forkScoped);
        yield* Effect.yieldNow;
        yield* system.spawn("review", SignalActor, { supervision: () => "stop" });
        const event = yield* Fiber.join(stopped).pipe(Effect.timeout("2 seconds"));
        assert.equal(event._tag, "Some");
        if (event._tag === "Some" && event.value._tag === "ActorStopped")
          assert.ok(event.value.cause);
        assert.equal(saves, 0);
        assert.deepEqual(registry.get(malformed.path), malformed);
      }),
    ),
  );
});
