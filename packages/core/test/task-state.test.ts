import assert from "node:assert/strict";
import { test } from "node:test";
import { AgentConversations } from "@aster/agent";
import { Context, Deferred, Effect, Exit, Fiber, Layer, Schema } from "effect";
import {
  ContextRegistry,
  ExternalAgents,
  TaskActor,
  TaskSnapshot,
  TaskState,
} from "../src/index.js";
import { makeContextRegistry } from "../src/testing/context.js";
import { testConversations } from "./conversation-fixtures.js";
import { retainedTask, taskInput } from "./task-fixtures.js";

const openTask = (registry: ContextRegistry["Service"], history: AgentConversations["Service"]) =>
  Layer.build(TaskState.layer(taskInput().target)).pipe(
    Effect.map((services) => Context.get(services, TaskState)),
    Effect.provideService(ContextRegistry, registry),
    Effect.provideService(AgentConversations, history),
    Effect.provideService(ExternalAgents, {}),
  );

test("TaskState drains committed storage into its Ref despite interruption", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const history = testConversations();
        const retained = yield* retainedTask(history, "running");
        const registry = yield* makeContextRegistry({ loadAll: () => [retained], save: () => {} });
        yield* registry.register(retained.path, TaskActor.context);
        const stored = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const task = yield* openTask(
          {
            ...registry,
            commit: (record, options) =>
              registry
                .commit(record, options)
                .pipe(
                  Effect.tap(() =>
                    Deferred.succeed(stored, undefined).pipe(
                      Effect.andThen(Deferred.await(release)),
                    ),
                  ),
                ),
          },
          history,
        );
        const saving = yield* task
          .settle({ roundId: "task", status: "waiting_input", text: "Need input", covered: [] })
          .pipe(Effect.forkScoped);
        yield* Deferred.await(stored);
        assert.equal(
          Schema.decodeUnknownSync(TaskSnapshot)(registry.get(retained.path)!.state).status,
          "waiting_input",
        );
        assert.equal((yield* task.snapshot).status, "running");
        const interrupting = yield* Fiber.interrupt(saving).pipe(Effect.forkScoped);
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(interrupting);
        assert.equal((yield* task.snapshot).status, "waiting_input");
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("failed Task persistence leaves the committed Ref unchanged", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const history = testConversations();
        const retained = yield* retainedTask(history, "running");
        const registry = yield* makeContextRegistry({
          loadAll: () => [retained],
          save: () => {
            throw new Error("Injected storage failure");
          },
        });
        yield* registry.register(retained.path, TaskActor.context);
        const task = yield* openTask(registry, history);
        const result = yield* task
          .settle({ roundId: "task", status: "waiting_input", text: "Need input", covered: [] })
          .pipe(Effect.exit);
        assert.ok(Exit.hasDies(result));
        assert.equal((yield* task.snapshot).status, "running");
        assert.equal(
          Schema.decodeUnknownSync(TaskSnapshot)(registry.get(retained.path)!.state).status,
          "running",
        );
      }),
    ),
  );
});

test("settlement covers only the executed inputs and leaves later input ready", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const history = testConversations();
        const retained = yield* retainedTask(history, "running");
        const registry = yield* makeContextRegistry({ loadAll: () => [retained], save: () => {} });
        yield* registry.register(retained.path, TaskActor.context);
        const task = yield* openTask(registry, history);
        const state = yield* task.snapshot;
        // A persisted input not covered by this execution survives its result.
        const ref = state.inputs[0]!;
        yield* task.settle({
          roundId: "task",
          status: "completed",
          text: "Previous round",
          covered: [],
        });
        assert.equal((yield* task.snapshot).status, "ready");
        assert.equal((yield* task.snapshot).inputs[0]!.status, "pending");
        yield* task.settle({
          roundId: "task",
          status: "completed",
          text: "Done",
          covered: [ref.requestId],
        });
        assert.equal((yield* task.snapshot).status, "completed");
        assert.equal((yield* task.snapshot).inputs[0]!.status, "completed");
        assert.deepEqual(
          yield* task.snapshot,
          Schema.decodeUnknownSync(TaskSnapshot)(registry.get(retained.path)!.state),
        );
      }),
    ),
  );
});
