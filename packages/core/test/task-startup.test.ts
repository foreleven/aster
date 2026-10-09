import { ActorSystem } from "@aster/actor";
import { AgentConversations, DurableHarness } from "@aster/agent/harness";
import { Clock, Deferred, Effect, Fiber, Layer, Logger, Schema, Stream } from "effect";
import { TestClock } from "effect/testing";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ContextQueries } from "../src/context/queries/routes.js";
import {
  ContextRegistry,
  ExternalAgents,
  GoalSettings,
  TasksRootActor,
  type TaskAdmissionReply,
} from "../src/index.js";
import { makeContextRegistry } from "../src/testing/context.js";
import { testConversations } from "./conversation-fixtures.js";
import { makeHarness } from "./harness-fixtures.js";
import { retainedTask, taskInput } from "./task-fixtures.js";

const TerminationLog = Schema.Struct({
  event: Schema.Literal("task.actor.terminated"),
  actorPath: Schema.String,
  cause: Schema.Struct({ message: Schema.String }),
});

const taskSystem = (
  registry: ContextRegistry["Service"],
  history: AgentConversations["Service"],
  clock: Clock.Clock,
) =>
  ActorSystem.make().pipe(
    ActorSystem.provide(
      ContextQueries.layer,
      Layer.succeed(ContextRegistry, registry),
      Layer.succeed(AgentConversations, history),
      Layer.succeed(ExternalAgents, {}),
      Layer.succeed(GoalSettings, { definitions: [], reasoning: { model: "test" } }),
      Layer.succeed(
        DurableHarness,
        makeHarness(() => Effect.die("Completed Task must not execute again")),
      ),
      Layer.succeed(Clock.Clock, clock),
    ),
  );

for (const fails of [false, true]) {
  test(`Task ${fails ? "startup failure is supervised and watched" : "slow recovery queues its own inputs"} without blocking root admission`, async () => {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const entered = yield* Deferred.make<void>();
          const release = yield* Deferred.make<void>();
          const noticed = yield* Deferred.make<typeof TerminationLog.Type>();
          const clock = yield* TestClock.make();
          const history = testConversations();
          const retained = yield* retainedTask(history, "completed");
          const otherTask = yield* retainedTask(history, "completed", taskInput("other"));
          const registry = yield* makeContextRegistry({
            loadAll: () => [retained, otherTask],
            save: () => {},
          });
          const system = yield* taskSystem(
            registry,
            {
              ...history,
              read: (path) =>
                Effect.gen(function* () {
                  if (path === retained.snapshot.path) {
                    yield* Deferred.succeed(entered, undefined);
                    yield* Deferred.await(release);
                    if (fails) return yield* Effect.die(new Error("Task recovery failed"));
                  }
                  return yield* history.read(path);
                }),
            },
            clock,
          ).pipe(
            ActorSystem.provide(
              ContextQueries.layer,
              Logger.layer([
                Logger.make<unknown, void>(({ message }) => {
                  if (!Array.isArray(message)) return;
                  for (const entry of message)
                    if (Schema.is(TerminationLog)(entry))
                      Deferred.doneUnsafe(noticed, Effect.succeed(entry));
                }),
              ]),
            ),
          );
          const root = yield* system.spawn("tasks", TasksRootActor);
          yield* Deferred.await(entered);
          yield* root.awaitStarted;
          const queued = yield* root
            .ask<TaskAdmissionReply>((replyTo) => ({
              _tag: "StartTask",
              input: taskInput(),
              replyTo,
            }))
            .pipe(Effect.forkScoped);
          const sendOther = () =>
            root.ask<TaskAdmissionReply>((replyTo) => ({
              _tag: "StartTask",
              input: taskInput("other"),
              replyTo,
            }));
          // A different child can validate and reply while the retained child is restoring.
          const other = yield* sendOther();
          assert.equal(other._tag, "Accepted");
          assert.equal(queued.pollUnsafe(), undefined);
          assert.equal(yield* Deferred.isDone(noticed), false);
          yield* Deferred.succeed(release, undefined);
          if (fails) {
            yield* clock.adjust("10 seconds");
            const log = yield* Deferred.await(noticed);
            assert.equal(log.actorPath, `/user${retained.snapshot.path}`);
            assert.match(log.cause.message, /Task recovery failed/);
            yield* Fiber.interrupt(queued);
          } else {
            assert.equal((yield* Fiber.join(queued))._tag, "Accepted");
          }
          assert.equal((yield* sendOther())._tag, "Accepted");
        }),
      ).pipe(Effect.timeout("5 seconds")),
    );
  });
}

test("Task root restart retains existing child ownership", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const history = testConversations();
        const retained = yield* retainedTask(history, "completed");
        const registry = yield* makeContextRegistry({
          loadAll: () => [retained],
          save: () => {},
        });
        let failNextRead = false;
        const clock = yield* TestClock.make();
        const system = yield* taskSystem(
          {
            ...registry,
            get: (path) => {
              if (failNextRead && path === retained.snapshot.path) {
                failNextRead = false;
                throw new Error("Injected root routing failure");
              }
              return registry.get(path);
            },
          },
          history,
          clock,
        );
        const root = yield* system.spawn("tasks", TasksRootActor);
        yield* root.awaitStarted;
        const child = yield* system.select(`/user${retained.snapshot.path}`).resolve();
        yield* child.awaitStarted;
        const restarting = yield* Stream.runHead(
          Stream.filter(
            system.events,
            (event) => event._tag === "ActorRestarting" && event.path === root.path,
          ),
        ).pipe(Effect.forkScoped({ startImmediately: true }));
        failNextRead = true;
        const failed = yield* root
          .ask((replyTo) => ({
            _tag: "CheckTask",
            input: { requestId: "restart", target: retained.snapshot.path, expectedRevision: 2 },
            replyTo,
          }))
          .pipe(Effect.forkScoped);
        yield* Fiber.join(restarting);
        yield* Fiber.interrupt(failed);
        yield* clock.adjust("1 second");
        const accepted = yield* root.ask<TaskAdmissionReply>((replyTo) => ({
          _tag: "StartTask",
          input: taskInput(),
          replyTo,
        }));
        assert.equal(accepted._tag, "Accepted");
        assert.equal(yield* system.select(child.path).resolve(), child);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});
