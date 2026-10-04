import { goalWorkflowLayer } from "./workflow-fixtures.js";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem } from "@aster/actor";
import { ConfigProvider, Deferred, Effect, Layer, Schema } from "effect";
import {
  ContextRegistry,
  ExternalAgents,
  GoalSettings,
  GoalState,
  GoalsRootActor,
  makeApplicationApi,
  makeContextRegistry,
  makeMemoryGoalHistory,
  parseConfig,
  type ContextRecord,
} from "../src/index.js";
import { preparationLayer } from "./fixtures.js";

const configFor = (goal: object) => ({
  config: {
    models: [
      { name: "test", provider: "openai", model: "test", url: "http://localhost", apiKey: "test" },
    ],
    goals: { model: "test" },
  },
  goals: { project: goal },
});
const settingsFor = (config: unknown) =>
  GoalSettings.pipe(
    Effect.provide(
      GoalSettings.layer.pipe(
        Layer.provide(ConfigProvider.layer(ConfigProvider.fromUnknown(config))),
      ),
    ),
  );

test("Goal config readers retain optional titles and reject blank or non-string titles", async () => {
  for (const goal of [
    { description: "Detailed responsibility" },
    { title: "Project watch", description: "Detailed responsibility" },
  ]) {
    const config = configFor(goal);
    const expected = [{ slug: "project", ...goal }];
    assert.deepEqual(parseConfig(config, "/tmp").goals, expected);
    assert.deepEqual((await Effect.runPromise(settingsFor(config))).definitions, expected);
  }
  for (const title of ["", " \n\t", 42, null]) {
    const config = configFor({ title, description: "Detailed responsibility" });
    assert.throws(() => parseConfig(config, "/tmp"));
  }
  const whitespaceConfig = configFor({ title: " \n\t", description: "Detailed responsibility" });
  assert.equal(
    (await Effect.runPromise(Effect.result(settingsFor(whitespaceConfig))))._tag,
    "Failure",
  );
});

test("Goal startup persists titles and refreshes restored titles without losing work", async () => {
  const history = makeMemoryGoalHistory();
  let saved: ContextRecord | undefined;
  const description = "Detailed responsibility";
  for (const title of [undefined, "Project watch", "Renamed project", undefined]) {
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const previous = saved;
          const registry = yield* makeContextRegistry({
            loadAll: () => (saved ? [saved] : []),
            save: (record) => {
              saved = record;
            },
          });
          const ready = yield* Deferred.make<void>();
          const system = yield* ActorSystem.make().pipe(
            ActorSystem.provide(
              Layer.succeed(ContextRegistry, registry),
              preparationLayer,
              Layer.succeed(ExternalAgents, {}),
              goalWorkflowLayer({
                definitions: [
                  { slug: "project", description, ...(title === undefined ? {} : { title }) },
                ],
                history,
                reasoner: { plan: () => Effect.die("No evaluation expected") },
                signals: () => [],
                reconcile: () => Deferred.succeed(ready, undefined).pipe(Effect.as([])),
                deactivate: () => Effect.void,
              }),
            ),
          );
          yield* system.spawn("goals", GoalsRootActor);
          yield* Deferred.await(ready).pipe(Effect.timeout("2 seconds"));
          const api = makeApplicationApi({ registry, inspect: Effect.succeed(null) });
          const record = yield* api.context("/goals/project");
          const canonical = registry.get(record.path)!;
          const state = Schema.decodeUnknownSync(GoalState)(canonical.state);
          assert.equal(state.title, title ?? description);
          assert.equal(record.description, description);
          assert.deepEqual(saved, canonical);
          assert.deepEqual(yield* api.goals.list, [record]);
          if (previous) {
            assert.deepEqual(canonical, {
              ...previous,
              revision: (previous.revision ?? 0) + 1,
              state: { ...previous.state, title: title ?? description },
            });
          } else {
            // Emulate a pre-title persisted Goal with progress, a task and native history.
            const message = { role: "user" as const, content: "Keep existing work", timestamp: 1 };
            yield* history.append("project", message);
            const { title: _title, ...legacyState } = state;
            yield* registry.commit(
              {
                ...canonical,
                state: {
                  ...legacyState,
                  status: "completed",
                  summary: "Existing conclusions",
                  progress: "Finished",
                  historyCount: 1,
                  tasks: [
                    {
                      id: "task",
                      title: "Saved task",
                      instructions: "Retain",
                      status: "completed",
                      revision: 1,
                      evidence: [],
                      createdAt: "2026-10-01",
                      updatedAt: "2026-10-01",
                    },
                  ],
                },
                messages: [message],
              },
              { expectedRevision: registry.get(record.path)?.revision ?? 0 },
            );
          }
        }),
      ),
    );
  }
});
