import { testSettings } from "./settings-fixtures.js";
import { goalBusinessState } from "../src/goals/view.js";
import { serializeFixtureState } from "../src/testing/context.js";
import { DurableContext } from "@aster/core";
import { ActorSystem } from "@aster/actor";
import { ConfigProvider, Effect, Layer, Schema } from "effect";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ContextQueries } from "../src/queries/registry.js";
import {
  ExternalAgents,
  GoalDefinition,
  GoalSettings,
  GoalSnapshot,
  GoalsRootActor,
  type StoredContext,
} from "../src/index.js";
import { makeTestContext } from "../src/testing/context.js";
import { testConversations } from "./conversation-fixtures.js";
import { goalWorkflowLayer } from "./workflow-fixtures.js";

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
        Layer.provide(
          ConfigProvider.layer(ConfigProvider.fromUnknown(config, { preserveEmptyStrings: true })),
        ),
      ),
    ),
  );

test("Goal settings retain optional titles and reject empty or whitespace titles", async () => {
  for (const goal of [
    { description: "Detailed responsibility" },
    { title: "Project watch", description: "Detailed responsibility" },
  ]) {
    const config = configFor(goal);
    const expected = [{ slug: "project", ...goal }];
    assert.deepEqual(
      (await Effect.runPromise(Effect.scoped(settingsFor(config)))).definitions.filter(
        (goal) => goal.slug !== "personal",
      ),
      expected,
    );
  }
  // ConfigProvider normalizes scalars and treats null values as absent.
  for (const [title, expected] of [
    [null, undefined],
    [42, "42"],
  ] as const) {
    const settings = await Effect.runPromise(
      Effect.scoped(settingsFor(configFor({ title, description: "Detailed responsibility" }))),
    );
    assert.equal(settings.definitions.find((goal) => goal.slug === "project")?.title, expected);
  }
  for (const title of ["", " \n\t"]) {
    const invalid = configFor({ title, description: "Detailed responsibility" });
    assert.equal(
      (await Effect.runPromise(Effect.scoped(Effect.result(settingsFor(invalid)))))._tag,
      "Failure",
    );
  }
});

test("Goal definitions reject blank and non-string titles at domain boundaries", () => {
  for (const title of ["", " \n\t", 42, null]) {
    assert.throws(() =>
      Schema.decodeUnknownSync(GoalDefinition)({
        slug: "project",
        title,
        description: "Detailed responsibility",
      }),
    );
  }
});

test("Goal startup refreshes the entire definition without losing work", async () => {
  const history = testConversations();
  let saved: StoredContext | undefined;
  for (const definition of [
    { slug: "project", description: "Detailed responsibility" },
    {
      slug: "project",
      title: "Project watch",
      description: "Updated responsibility",
    },
    {
      slug: "project",
      title: "Renamed project",
      description: "Another responsibility",
    },
    { slug: "project", description: "Final responsibility" },
  ]) {
    const { title, description } = definition;
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const previous = saved;
          const registry = yield* makeTestContext({
            loadAll: () => (saved ? [saved] : []),
            save: (record) => {
              saved = record;
            },
          });
          const system = yield* ActorSystem.make().pipe(
            ActorSystem.provide(
              testSettings,
              ContextQueries.layer,
              Layer.mergeAll(Layer.succeed(DurableContext, registry.backend)),

              Layer.succeed(ExternalAgents, {}),
              goalWorkflowLayer({
                definitions: [definition],
                history,
                reasoner: { plan: () => Effect.never },
              }),
            ),
          );
          const root = yield* system.spawn("goals", GoalsRootActor);
          yield* root.awaitStarted;
          yield* (yield* system.select("/user/goals/project").resolve()).awaitStarted;
          const canonicalRecord = registry.get("/goals/project")!;
          const record = {
            ...canonicalRecord,
            state: goalBusinessState(Schema.decodeUnknownSync(GoalSnapshot)(canonicalRecord.state)),
          };
          const canonical = registry.get(record.path)!;
          const state = Schema.decodeUnknownSync(GoalSnapshot)(canonical.state);
          assert.deepEqual(state.definition, definition);
          assert.equal(
            Schema.decodeUnknownSync(Schema.Struct({ title: Schema.String }))(record.state).title,
            title ?? description,
          );
          assert.equal(record.description, description);
          assert.deepEqual(saved?.snapshot, canonical);
          assert.deepEqual(
            Object.values(registry.snapshot()).filter((r) => /^\/goals\/[^/]+$/.test(r.path)),
            [canonical],
          );
          if (previous) {
            assert.deepEqual(canonical, {
              ...previous.snapshot,
              revision: previous.snapshot.revision + 1,
              description: definition.description,
              state: { ...previous.snapshot.state, definition },
              text: serializeFixtureState({ ...previous.snapshot.state, definition }),
            });
          } else {
            // Retain completed business progress while refreshing display metadata.
            const message = { role: "user" as const, content: "Keep existing work", timestamp: 1 };
            yield* system.stop(root);
            yield* history.append("/goals/project", "kept", "test.record", message);
            yield* registry.register(canonical.path, {
              state: GoalSnapshot,
              message: Schema.Never,
            });
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
