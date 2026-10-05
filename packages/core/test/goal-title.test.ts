import { testConversations } from "./conversation-fixtures.js";
import { goalWorkflowLayer } from "./workflow-fixtures.js";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem } from "@aster/actor";
import { ConfigProvider, Effect, Layer, Schema } from "effect";
import {
  ContextRegistry,
  ExternalAgents,
  GoalSettings,
  GoalState,
  GoalsRootActor,
  makeApplicationApi,
  parseConfig,
  type ContextRecord,
} from "../src/index.js";
import { makeContextRegistry } from "../src/testing/context.js";

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
    assert.deepEqual(
      (await Effect.runPromise(settingsFor(config))).definitions.filter(
        (goal) => goal.slug !== "personal",
      ),
      expected,
    );
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

test("Goal startup refreshes the entire definition without losing work", async () => {
  const history = testConversations();
  let saved: ContextRecord | undefined;
  for (const definition of [
    { slug: "project", description: "Detailed responsibility" },
    {
      slug: "project",
      title: "Project watch",
      description: "Updated responsibility",
      completionCriteria: "Release ships",
    },
    {
      slug: "project",
      title: "Renamed project",
      description: "Another responsibility",
      completionCriteria: "Review done",
    },
    { slug: "project", description: "Final responsibility" },
  ]) {
    const { title, description } = definition;
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
          const system = yield* ActorSystem.make().pipe(
            ActorSystem.provide(
              Layer.succeed(ContextRegistry, registry),

              Layer.succeed(ExternalAgents, {}),
              goalWorkflowLayer({
                definitions: [definition],
                history,
                reasoner: { plan: () => Effect.die("No evaluation expected") },

                deactivate: () => Effect.void,
              }),
            ),
          );
          const root = yield* system.spawn("goals", GoalsRootActor);
          yield* root.ask((replyTo) => ({ _tag: "AwaitReady", stage: "restored", replyTo }));
          const api = makeApplicationApi({
            registry,
            conversations: history,
            inspect: Effect.succeed(null),
          });
          const record = yield* api.context("/goals/project");
          const canonical = registry.get(record.path)!;
          const state = Schema.decodeUnknownSync(GoalState)(canonical.state);
          assert.deepEqual(state.definition, definition);
          assert.equal(
            Schema.decodeUnknownSync(Schema.Struct({ title: Schema.String }))(record.state).title,
            title ?? description,
          );
          assert.equal(record.description, description);
          assert.deepEqual(saved, canonical);
          assert.deepEqual(yield* api.goals.list, [record]);
          if (previous) {
            assert.deepEqual(canonical, {
              ...previous,
              revision: (previous.revision ?? 0) + 1,
              state: { ...previous.state, definition },
            });
          } else {
            // Retain completed business progress while refreshing display metadata.
            const message = { role: "user" as const, content: "Keep existing work", timestamp: 1 };
            yield* history.append("/goals/project", "kept", "test.record", message);
            yield* registry.commit(
              {
                ...canonical,
                state: {
                  ...state,
                  status: "completed",
                  summary: "Existing conclusions",
                },
                messages: [],
              },
              { expectedRevision: registry.get(record.path)?.revision ?? 0 },
            );
          }
        }),
      ),
    );
  }
});
