import { Schema } from "effect";
import { ContextQueryError } from "../src/context/contracts.js";
import { DurableContext } from "@aster/core";
import { testConversations } from "./conversation-fixtures.js";
import { ActorSystem, Command } from "@aster/actor";
import { Deferred, Effect, Fiber, Layer } from "effect";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ListGoals, ReadGoal } from "../src/goals/queries.js";
import { ListTasks, ReadTask } from "../src/tasks/queries.js";
import { ReadSignal } from "../src/signals/queries.js";
import { ContextQueries } from "../src/context/queries/routes.js";
import {
  ContextRegistry,
  ExternalAgents,
  GoalsRootActor,
  SignalRootActor,
  SignalDefinitions,
  type GoalCommandReply,
  type StoredContext,
} from "../src/index.js";
import { makeContextRegistry } from "../src/testing/context.js";
import { goalWorkflowLayer } from "./workflow-fixtures.js";
import { retainedTask, taskFixture, taskInput } from "./task-fixtures.js";
import type { TaskAdmissionReply } from "../src/tasks/protocol.js";
import type { SignalCommandReply } from "../src/signals/protocol.js";
import type { QueryReply } from "../src/services/actors.js";

test("Goal list/read query live children and mailbox state rather than the durable directory", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const orphan: StoredContext = {
          snapshot: {
            path: "/goals/orphan",
            revision: 1,
            description: "Orphan",
            state: {
              definition: { slug: "orphan", description: "Orphan" },
              status: "active",
              summary: "Not live",
              tasks: [],
              inputs: [],
              receipts: [],
            },
            messages: [],
          },
          events: [],
        };
        const registry = yield* makeContextRegistry({ loadAll: () => [orphan], save: () => {} });
        const queries = yield* ContextQueries.pipe(Effect.provide(ContextQueries.layer));
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.merge(
              Layer.succeed(ContextRegistry, registry),
              Layer.succeed(DurableContext, registry.backend),
            ),
            Layer.succeed(ContextQueries, queries),
            Layer.succeed(ExternalAgents, {}),
            Layer.succeed(SignalDefinitions, []),
            goalWorkflowLayer({
              definitions: [{ slug: "personal", description: "Assistant" }],
              reasoner: { plan: () => Effect.never },
            }),
          ),
        );
        yield* (yield* system.spawn("signals", SignalRootActor)).awaitStarted;
        const root = yield* system.spawn("goals", GoalsRootActor);
        yield* root.awaitStarted;
        const listed = yield* queries.query({ path: "/goals", command: "list", args: {} });
        assert.deepEqual(listed, {
          items: [
            {
              path: "/goals/personal",
              title: "Assistant",
              description: "Assistant",
              status: "active",
              summary: "Ready to begin",
            },
          ],
          total: 1,
        });
        const ended = yield* root.ask<GoalCommandReply>((replyTo) => ({
          _tag: "Route",
          slug: "personal",
          command: { _tag: "End", requestId: "end", replyTo },
        }));
        assert.equal(ended._tag, "Accepted");
        const read = yield* queries.query({
          path: "/goals",
          command: "read",
          args: { path: "/goals/personal" },
        });
        assert.match(JSON.stringify(read), /"status":"completed"/);
        assert.doesNotMatch(JSON.stringify(read), /receipts|inputs|definition/);
        assert.equal(
          Schema.decodeUnknownSync(ContextQueryError)(
            yield* queries
              .query({ path: "/goals", command: "read", args: { path: "/goals/orphan" } })
              .pipe(Effect.flip),
          ).kind,
          "unavailable",
        );
        assert.ok(registry.get("/goals/personal"));
      }),
    ),
  );
});

test("Task details are answered by their Actor and expose admission evidence without transcripts", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const env = yield* taskFixture();
        const input = taskInput("query");
        const reply = yield* env.tasks.ask<TaskAdmissionReply>((replyTo) => ({
          _tag: "StartTask",
          input,
          replyTo,
        }));
        assert.equal(reply._tag, "Accepted");
        const read = yield* env.tasks.ask<Command.Reply<typeof ReadTask>>(
          (replyTo) => new ReadTask({ path: input.target, replyTo }),
        );
        assert.equal(read._tag, "Success");
        if (read._tag === "Success") {
          assert.match(JSON.stringify(read.value), /Read evidence/);
          assert.doesNotMatch(
            JSON.stringify(read.value),
            /outcomeEntryId|inputs|checkpoint|requestId/,
          );
        }
        const list = yield* env.tasks.ask<Command.Reply<typeof ListTasks>>(
          (replyTo) => new ListTasks({ replyTo }),
        );
        assert.equal(list._tag, "Success");
        if (list._tag === "Success")
          assert.match(JSON.stringify(list.value), new RegExp(input.target));
      }),
    ),
  );
});

test("Signal owner queries and pause coordination read each child through its mailbox", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const env = yield* taskFixture();
        const root = yield* env.system.spawn("signals", SignalRootActor);
        yield* root.awaitStarted;
        const created = yield* root.ask<SignalCommandReply>((replyTo) => ({
          _tag: "Change",
          replyTo,
          input: {
            requestId: "create",
            source: "/goals/personal",
            target: "/signals/personal--query",
            remainingAgentTurns: 3,
            change: {
              operation: "create",
              definition: {
                trigger: { _tag: "Context", when: "Query" },
                task: { _tag: "Goal", target: "/goals/personal", text: "Notify" },
              },
            },
          },
        }));
        assert.equal(created._tag, "Accepted");
        const list = yield* root.ask<QueryReply>((replyTo) => ({
          _tag: "ListByOwner",
          owner: "/goals/personal",
          replyTo,
        }));
        assert.equal(list._tag, "Success");
        if (list._tag === "Success") {
          assert.match(JSON.stringify(list.value), /personal--query/);
          assert.doesNotMatch(JSON.stringify(list.value), /receipts|deliveries/);
        }
        yield* root.ask<void>((replyTo) => ({
          _tag: "PauseByOwner",
          owner: "/goals/personal",
          replyTo,
        }));
        const read = yield* root.ask<Command.Reply<typeof ReadSignal>>(
          (replyTo) => new ReadSignal({ path: "/signals/personal--query", replyTo }),
        );
        assert.equal(read._tag, "Success");
        if (read._tag === "Success") assert.match(JSON.stringify(read.value), /"status":"paused"/);
      }),
    ),
  );
});

test("Cancelling a root Task read releases the child's Pi query and leaves its mailbox responsive", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const conversations = testConversations();
        const input = taskInput("cancel-query");
        const retained = yield* retainedTask(conversations, "completed", input);
        const started = yield* Deferred.make<void>(),
          released = yield* Deferred.make<void>();
        const env = yield* taskFixture({
          records: new Map([[input.target, retained]]),
          conversations: {
            ...conversations,
            get: (path, id) =>
              Effect.gen(function* () {
                const entry = yield* conversations.get(path, id);
                if (entry.kind === "task.admission") {
                  return yield* Deferred.succeed(started, undefined).pipe(
                    Effect.andThen(Effect.never),
                    Effect.ensuring(Deferred.succeed(released, undefined)),
                  );
                }
                return entry;
              }),
          },
        });
        const reading = yield* env.tasks
          .ask<Command.Reply<typeof ReadGoal>>(
            (replyTo) => new ReadGoal({ path: input.target, replyTo }),
          )
          .pipe(Effect.forkScoped);
        yield* Deferred.await(started);
        yield* Fiber.interrupt(reading);
        yield* Deferred.await(released);
        const listed = yield* env.tasks.ask<Command.Reply<typeof ListGoals>>(
          (replyTo) => new ListGoals({ replyTo }),
        );
        assert.equal(listed._tag, "Success");
      }),
    ),
  );
});
