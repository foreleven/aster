import { preparationLayer, fakeAgent } from "./fixtures.js";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem } from "@aster/actor";
import { ContextRegistry } from "../src/index.js";
import { makeContextRegistry } from "../src/testing/context.js";
import { Effect, Layer } from "effect";
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
