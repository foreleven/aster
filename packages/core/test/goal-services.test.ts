import { makeHarness } from "./harness-fixtures.js";
import { DurableHarness, AgentConversations } from "@aster/agent/harness";
import { CurrentActors } from "../src/services/actors.js";
import assert from "node:assert/strict";
import { test } from "node:test";
import { AgentError, type AssistantMessage } from "@aster/agent";
import { AgentRunner } from "@aster/agent/agent";
import { Context, Deferred, Effect, Exit, Fiber, Layer, Result, Schema, Scope } from "effect";
import {
  GoalActor,
  GoalAgent,
  GoalSettings,
  ContextRegistry,
  MemoryRecall,
  ExternalAgents,
  defineContext,
  TaskSnapshot,
} from "../src/index.js";
import { taskPathFor } from "../src/tasks/state/admission.js";
import { GoalState } from "../src/goals/state/model.js";
import { GoalSnapshot } from "../src/goals/state/snapshot.js";
import { makeContextRegistry } from "../src/testing/context.js";
import { testConversations } from "./conversation-fixtures.js";

const definition = { slug: "personal", description: "Assist the user" };
const path = "/goals/personal";

const openGoal = (registry: ContextRegistry["Service"], messages: AgentConversations["Service"]) =>
  Layer.build(GoalState.layer(path, definition)).pipe(
    Effect.map((services) => Context.get(services, GoalState)),
    Effect.provideService(ContextRegistry, registry),
    Effect.provideService(AgentConversations, messages),
  );

test("GoalState serializes local summary updates with concurrent durable admissions", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        yield* registry.register(path, GoalActor.context);
        const messages = testConversations();
        const entered = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const waiting = yield* Deferred.make<void>();
        const goal = yield* openGoal(registry, {
          ...messages,
          append: (...args) =>
            messages.append(...args).pipe(
              Effect.tap(() =>
                args[2] === "goal.input" &&
                Schema.decodeUnknownSync(
                  Schema.Struct({
                    payload: Schema.Struct({ text: Schema.String }),
                  }),
                )(args[3]).payload.text === "first"
                  ? Deferred.succeed(entered, undefined).pipe(
                      Effect.andThen(Deferred.await(release)),
                    )
                  : Effect.void,
              ),
            ),
        });
        const request = (id: string) => ({
          _tag: "SubmitInput" as const,
          requestId: id,
          input: { _tag: "UserInput" as const, text: id },
        });
        const first = yield* goal.accept(request("first"), false).pipe(Effect.forkScoped);
        yield* Deferred.await(entered);
        const concurrent = yield* Deferred.succeed(waiting, undefined).pipe(
          Effect.andThen(
            Effect.all([goal.updateSummary("New finding"), goal.accept(request("second"), false)], {
              concurrency: "unbounded",
            }),
          ),
          Effect.forkScoped,
        );
        yield* Deferred.await(waiting);
        assert.equal((yield* goal.read).summary, "Ready to begin");
        assert.equal((yield* goal.read).inputs.length, 0);
        yield* Deferred.succeed(release, undefined);
        const accepted = yield* Fiber.join(first);
        yield* Fiber.join(concurrent);
        const state = yield* goal.read;
        assert.equal(state.summary, "New finding");
        assert.equal(state.inputs.length, 2);
        assert.deepEqual(
          state.receipts.map((receipt) => receipt.requestId),
          ["first", "second"],
        );
        assert.deepEqual(state, Schema.decodeUnknownSync(GoalSnapshot)(registry.get(path)!.state));
        const replayed = yield* goal.accept(request("first"), false);
        assert.equal(replayed.replayed, true);
        assert.deepEqual(replayed.receipt, accepted.receipt);
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("GoalState drains an admitted commit into its Ref despite interruption and revokes retired instances", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        yield* registry.register(path, GoalActor.context);
        const stored = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const messages = testConversations();
        const lifetime = yield* Scope.make();
        const goal = yield* openGoal(
          {
            ...registry,
            commit: (record, options) =>
              registry
                .commit(record, options)
                .pipe(
                  Effect.tap(() =>
                    Schema.decodeUnknownSync(GoalSnapshot)(record.state).summary ===
                    "Committed finding"
                      ? Deferred.succeed(stored, undefined).pipe(
                          Effect.andThen(Deferred.await(release)),
                        )
                      : Effect.void,
                  ),
                ),
          },
          messages,
        ).pipe(Effect.provideService(Scope.Scope, lifetime));
        const updating = yield* goal.updateSummary("Committed finding").pipe(Effect.forkScoped);
        yield* Deferred.await(stored);
        assert.equal((yield* goal.read).summary, "Ready to begin");
        const interrupting = yield* Fiber.interrupt(updating).pipe(Effect.forkScoped);
        yield* Deferred.succeed(release, undefined);
        yield* Fiber.join(interrupting);
        assert.equal((yield* goal.read).summary, "Committed finding");
        yield* Scope.close(lifetime, Exit.void);
        const reopened = yield* openGoal(registry, messages);
        yield* reopened.updateSummary("Current finding");
        const late = yield* goal.updateSummary("Retired finding").pipe(Effect.result);
        assert.ok(Result.isFailure(late));
        assert.equal(late.failure.kind, "conflict");
        assert.equal((yield* reopened.read).summary, "Current finding");
        assert.equal(
          Schema.decodeUnknownSync(GoalSnapshot)(registry.get(path)!.state).summary,
          "Current finding",
        );
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("failed Goal persistence leaves the committed Ref unchanged and remains a defect", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry({
          loadAll: () => [],
          save: (record) => {
            if (
              Schema.decodeUnknownSync(GoalSnapshot)(record.snapshot.state).summary ===
              "Uncommitted finding"
            )
              throw new Error("Injected storage failure");
          },
        });
        yield* registry.register(path, GoalActor.context);
        const goal = yield* openGoal(registry, testConversations());
        const result = yield* goal.updateSummary("Uncommitted finding").pipe(Effect.exit);
        assert.ok(Exit.hasDies(result));
        assert.equal((yield* goal.read).summary, "Ready to begin");
        assert.equal(
          Schema.decodeUnknownSync(GoalSnapshot)(registry.get(path)!.state).summary,
          "Ready to begin",
        );
      }),
    ),
  );
});

test("Goal task references recover interrupted attachment and reject unrelated Tasks", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        yield* registry.register(path, GoalActor.context);
        const messages = testConversations();
        const lifetime = yield* Scope.make();
        const goal = yield* openGoal(registry, messages).pipe(
          Effect.provideService(Scope.Scope, lifetime),
        );
        const create = Effect.fnUntraced(function* (id: string, source: string, replyTo: string) {
          const taskPath = taskPathFor(source, id);
          yield* registry.register(
            taskPath,
            defineContext({ state: TaskSnapshot, message: Schema.Never }),
          );
          yield* registry.commit(
            {
              path: taskPath,
              description: id,
              messages: [],
              state: {
                admission: {
                  source,
                  replyTo,
                  agent: "test",
                  remainingAgentTurns: 3,
                },
                status: "ready",
                inputs: [
                  {
                    requestId: id,
                    entryId: 1,
                    receipt: { requestId: id, revision: 1 },
                    status: "pending",
                  },
                ],
              },
            },
            { expectedRevision: 0 },
          );
          return taskPath;
        });
        const direct = yield* create("direct", path, path);
        const scheduled = yield* create("scheduled", "/signals/reminder", path);
        const forwarded = yield* create("forwarded", path, "/goals/other");
        const unrelated = yield* create("unrelated", "/goals/other", "/goals/other");
        assert.deepEqual((yield* goal.read).tasks, []);
        // Task admission survived, but no Goal attachment command committed before the restart.
        yield* Scope.close(lifetime, Exit.void);
        const restored = yield* openGoal(registry, messages);
        assert.deepEqual((yield* restored.read).tasks, [direct, scheduled, forwarded]);
        const revision = registry.get(path)!.revision;
        yield* restored.attachTask(direct);
        assert.equal(registry.get(path)!.revision, revision);
        assert.equal((yield* restored.attachTask(unrelated).pipe(Effect.result))._tag, "Failure");
        assert.deepEqual(
          Schema.decodeUnknownSync(Schema.Struct({ tasks: Schema.Array(Schema.String) }))(
            registry.reader.get(path)!.state,
          ).tasks,
          [direct, scheduled, forwarded],
        );
      }),
    ),
  );
});

test("GoalState persists replies before state settlement and resumes the handoff without model execution", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        yield* registry.register(path, GoalActor.context);
        const messages = testConversations();
        const appended = yield* Deferred.make<void>();
        const release = yield* Deferred.make<void>();
        const retained = {
          ...messages,
          append: (...args: Parameters<typeof messages.append>) =>
            messages
              .append(...args)
              .pipe(
                Effect.tap(() =>
                  args[2] === "goal.reply"
                    ? Deferred.succeed(appended, undefined).pipe(
                        Effect.andThen(Deferred.await(release)),
                      )
                    : Effect.void,
                ),
              ),
        };
        const lifetime = yield* Scope.make();
        const store = yield* openGoal(registry, retained).pipe(
          Effect.provideService(Scope.Scope, lifetime),
        );
        const request = {
          _tag: "SubmitInput" as const,
          requestId: "question",
          input: { _tag: "UserInput" as const, text: "Hello" },
        };
        const first = yield* store.accept(request, false);
        const input = (yield* store.read).inputs[0]!;
        assert.equal((yield* store.resolve(input)).payload._tag, "UserInput");
        yield* store.start(input.inputId);
        const settling = yield* store
          .settle(input.inputId, Result.succeed("Hello back"))
          .pipe(Effect.forkScoped);
        yield* Deferred.await(appended);
        assert.equal((yield* store.read).inputs[0]!.status, "running");
        assert.equal(
          (yield* messages.read(path)).filter((entry) => entry.kind === "goal.reply").length,
          1,
        );
        yield* Fiber.interrupt(settling);
        yield* Deferred.succeed(release, undefined);
        yield* Scope.close(lifetime, Exit.void);
        // Rebind as after a restart: storage reconstructs from Pi and committed GoalSnapshot, with no AgentRunner.
        const reopened = yield* openGoal(registry, retained);
        const replayed = yield* reopened.accept(request, false);
        assert.equal(replayed.replayed, true);
        assert.deepEqual(replayed.receipt, first.receipt);
        yield* reopened.settle(input.inputId, Result.succeed("Hello back"));
        assert.equal((yield* reopened.read).inputs[0]!.status, "completed");
        assert.equal(
          (yield* messages.read(path)).filter((entry) => entry.kind === "goal.reply").length,
          1,
        );
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});

test("GoalAgent resolves local tools from injected GoalState without Actor messages", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        yield* registry.register(path, GoalActor.context);
        const goal = yield* openGoal(registry, testConversations());
        const message: AssistantMessage = {
          role: "assistant",
          api: "openai-completions",
          provider: "test",
          model: "test",
          stopReason: "stop",
          timestamp: 0,
          content: [{ type: "text", text: "A useful reply" }],
          usage: {
            input: 0,
            output: 0,
            cacheRead: 0,
            cacheWrite: 0,
            totalTokens: 0,
            cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
          },
        };
        const agent = yield* GoalAgent.pipe(
          Effect.provide(GoalAgent.layer),
          Effect.provideService(
            AgentRunner,
            AgentRunner.make(() => Effect.die("Unexpected screening")),
          ),
          Effect.provideService(ContextRegistry, registry),
          Effect.provideService(GoalSettings, {
            definitions: [definition],
            reasoning: { model: "test" },
          }),
          Effect.provideService(MemoryRecall, {
            search: () => Effect.succeed([]),
            expand: () => Effect.succeed([]),
          }),
          Effect.provideService(ExternalAgents, {}),
          Effect.provideService(
            DurableHarness,
            makeHarness((invocation) =>
              Effect.gen(function* () {
                const update = invocation.tools!.find((tool) => tool.name === "update_summary")!;
                yield* Effect.tryPromise({
                  try: (signal) => update.execute("update", { summary: "New finding" }, signal),
                  catch: (cause) => new AgentError("Fake tool failed", [], { cause }),
                });
                for (const name of ["goal_current", "task_list"]) {
                  const read = invocation.tools!.find((tool) => tool.name === name)!;
                  const result = yield* Effect.tryPromise({
                    try: (signal) => read.execute(name, {}, signal),
                    catch: (cause) => new AgentError("Local read failed", [], { cause }),
                  });
                  assert.equal(result.isError, undefined);
                }
                return { messages: [message] };
              }),
            ),
          ),
        );
        const reply = yield* agent
          .converse({
            goal: definition,
            input: {
              kind: "UserInput",
              inputId: "one",
              entryId: 1,
              status: "pending",
              receivedAt: "2026-10-01T00:00:00Z",
              payload: { _tag: "UserInput", text: "Hello" },
              remainingAgentTurns: 4,
            },
          })
          .pipe(
            Effect.provideService(GoalState, goal),
            Effect.provideService(CurrentActors, {
              select: () => {
                throw new Error("Local Goal tools must not send Actor messages");
              },
            }),
          );
        assert.equal(reply, "A useful reply");
        assert.equal((yield* goal.read).summary, "New finding");
      }),
    ).pipe(Effect.timeout("5 seconds")),
  );
});
