import { goalWorkflowLayer } from "./workflow-fixtures.js";
import { goalTestReply } from "./goal-fixtures.js";
import { preparationLayer, fakeAgent } from "./fixtures.js";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem } from "@aster/actor";
import { ContextRegistry, makeContextRegistry } from "../src/index.js";
import { Effect, Layer } from "effect";
import { GoalsRootActor } from "../src/index.js";
import type { StoredGoalPlan as GoalPlan } from "../src/goals/plan.js";
import { ExternalAgents, DelegationActor } from "../src/index.js";
import { SignalActor, SignalDefinitions, SignalRootActor, SignalRunActor } from "../src/index.js";

const until = (condition: () => boolean) =>
  Effect.gen(function* () {
    while (!condition()) yield* Effect.sleep(1);
  }).pipe(Effect.timeout("2 seconds"));

test("Signal configuration omits an unassigned goal from public state", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        const definition = {
          slug: "review",
          when: "A review is needed",
          task: "Review the change",
          agent: "doubao-delegate",
          mode: "auto" as const,
        };
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(ContextRegistry, registry),
            Layer.succeed(SignalDefinitions, [definition]),
            preparationLayer,
            Layer.succeed(ExternalAgents, {}),
          ),
        );
        yield* system.spawn("signals", SignalRootActor);
        yield* until(() => registry.get("/signals/review") !== undefined);
        const state = registry.get("/signals/review")!.state;
        assert.equal(Object.hasOwn(state, "goal"), false);
        assert.doesNotThrow(() => JSON.stringify(state));
      }),
    ),
  );
});

test("a user message during planning is retained for a second evaluation without steer", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        const calls: unknown[] = [];
        let finish: ((plan: GoalPlan) => void) | undefined;
        const plan = { progress: "Monitoring", completed: true, evidence: [], signals: [] };
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(ContextRegistry, registry),
            preparationLayer,
            Layer.succeed(ExternalAgents, {}),
            goalWorkflowLayer({
              definitions: [{ slug: "project", description: "Monitor project progress over time" }],
              reasoner: {
                plan: (input) =>
                  Effect.suspend(() => {
                    calls.push(input);
                    if (calls.length === 1)
                      return Effect.callback<GoalPlan>((resume) => {
                        finish = (value) => resume(Effect.succeed(value));
                      });
                    return Effect.succeed(plan);
                  }),
              },
              signals: () => [],
              reconcile: () => Effect.succeed([]),
              deactivate: () => Effect.void,
            }),
          ),
        );
        const goals = yield* system.spawn("goals", GoalsRootActor);
        yield* goals.tell({ _tag: "Initialize" });
        yield* until(() => !!finish);
        yield* goals.tell({
          _tag: "Route",
          slug: "project",
          command: {
            _tag: "SubmitInput",
            requestId: "test-3475",
            input: { _tag: "UserInput", text: "Prioritize frontend dependencies" },
            replyTo: goalTestReply,
          },
        });
        yield* until(() =>
          JSON.stringify(registry.get("/goals/project")!.messages).includes(
            "Prioritize frontend dependencies",
          ),
        );
        finish!(plan);
        yield* until(() => calls.length === 2);
        yield* until(
          () =>
            (registry.get("/goals/project")!.state as { summary: string }).summary === "Monitoring",
        );
        const record = registry.get("/goals/project")!;
        assert.equal((record.state as { status: string }).status, "active");
        assert.ok(
          record.messages.some(
            (message) =>
              (message as { content?: string }).content === "Prioritize frontend dependencies",
          ),
        );
        assert.match(JSON.stringify(calls[1]), /Prioritize frontend dependencies/);
      }),
    ),
  );
});

test("restart resumes a persisted delegation session without another submission", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        const definition = {
          slug: "review",
          when: "Change",
          task: "Analysis",
          agent: "doubao-delegate",
          mode: "auto" as const,
        };
        const source = { path: "/source", description: "Source", state: {}, messages: [] };
        yield* registry.register("/signals/review", SignalActor.context);
        yield* registry.commit(
          {
            path: "/signals/review",
            description: "Analysis",
            state: definition,
            messages: [],
          },
          { expectedRevision: registry.get("/signals/review")?.revision ?? 0 },
        );
        yield* registry.register("/signals/review/runs/saved", SignalRunActor.context);
        yield* registry.commit(
          {
            path: "/signals/review/runs/saved",
            description: "Run",
            state: {
              signalSlug: "review",
              sourcePath: "/source",
              status: "running",
              definition,
              source,
              task: { instructions: "Analysis", input: [] },
            },
            messages: [
              {
                type: "Triggered",
                at: new Date().toISOString(),
                sourcePath: "/source",
                task: "Analysis",
                agent: "doubao-delegate",
                mode: "auto",
                sourceContext: source,
              },
            ],
          },
          { expectedRevision: registry.get("/signals/review/runs/saved")?.revision ?? 0 },
        );
        yield* registry.register("/delegations/saved", DelegationActor.context);
        yield* registry.commit(
          {
            path: "/delegations/saved",
            description: "Execution",
            state: {
              status: "running",
              request: {
                runPath: "/signals/review/runs/saved",
                agent: "doubao-delegate",
                task: { instructions: "Analysis", input: [] },
              },
              session: { sessionId: "saved-session" },
              requests: {},
              responses: {},
            },
            messages: [],
          },
          { expectedRevision: registry.get("/delegations/saved")?.revision ?? 0 },
        );
        let submitted = 0;
        let waited = 0;
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(ContextRegistry, registry),
            Layer.succeed(SignalDefinitions, [definition]),
            preparationLayer,
            Layer.succeed(ExternalAgents, {
              "doubao-delegate": fakeAgent({
                submit: () =>
                  Effect.sync(() => {
                    submitted++;
                    return { sessionId: "duplicate" };
                  }),
                wait: (session) =>
                  Effect.sync(() => {
                    assert.equal(session.sessionId, "saved-session");
                    waited++;
                    return { state: "completed", result: { text: "Recovered result" } };
                  }),
              }),
            }),
          ),
        );
        yield* system.spawn("signals", SignalRootActor);
        yield* until(
          () =>
            (registry.get("/signals/review/runs/saved")!.state as { status: string }).status ===
            "completed",
        );
        assert.equal(submitted, 0);
        assert.equal(waited, 1);
        assert.equal(
          (registry.get("/delegations/saved")!.state as { result: string }).result,
          "Recovered result",
        );
      }),
    ),
  );
});

test("auto Signal submits once and records actual adapter result", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* makeContextRegistry();
        let submitted = 0;
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(
            Layer.succeed(ContextRegistry, registry),
            Layer.succeed(SignalDefinitions, [
              {
                slug: "review",
                when: "A change occurred",
                task: "Analyze impact",
                agent: "doubao-delegate",
                mode: "auto",
              },
            ]),
            preparationLayer,
            Layer.succeed(ExternalAgents, {
              "doubao-delegate": fakeAgent({
                submit: () =>
                  Effect.sync(() => {
                    submitted++;
                    return { sessionId: "session-1" };
                  }),
                wait: () =>
                  Effect.sync(() => ({
                    state: "completed",
                    result: { text: "Project impact analyzed; dependencies need confirmation." },
                  })),
              }),
            }),
          ),
        );
        const root = yield* system.spawn("signals", SignalRootActor);
        const trigger = {
          _tag: "Trigger" as const,
          slug: "review",
          sourceContext: {
            path: "/lark/im/chats/oc_test",
            description: "Project chat",
            state: {},
            messages: ["The API changed"],
          },
        };
        yield* root.tell(trigger);
        yield* root.tell(trigger);
        yield* until(() =>
          Object.values(registry.snapshot()).some(
            (record) =>
              record.path.includes("/runs/") &&
              (record.state as { status?: string }).status === "completed",
          ),
        );
        const runs = Object.values(registry.snapshot()).filter((record) =>
          record.path.includes("/runs/"),
        );
        assert.equal(submitted, 1);
        assert.equal(runs.length, 1);
        assert.equal((runs[0]!.state as { sessionId: string }).sessionId, "session-1");
        assert.match(JSON.stringify(runs[0]!.messages), /Project impact analyzed/);
      }),
    ),
  );
});
