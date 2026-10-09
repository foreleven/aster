import { ActorSystem } from "@aster/actor";
import { ConfigProvider, Effect, Layer, Schema } from "effect";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ContextQueries } from "../src/context/queries/routes.js";
import {
  ContextRegistry,
  ExternalAgents,
  GoalDefinition,
  GoalSettings,
  GoalSnapshot,
  GoalsRootActor,
  type StoredContext,
} from "../src/index.js";
import { makeContextRegistry } from "../src/testing/context.js";
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
      (await Effect.runPromise(settingsFor(config))).definitions.filter(
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
      settingsFor(configFor({ title, description: "Detailed responsibility" })),
    );
    assert.equal(settings.definitions.find((goal) => goal.slug === "project")?.title, expected);
  }
  for (const title of ["", " \n\t"]) {
    const invalid = configFor({ title, description: "Detailed responsibility" });
    assert.equal((await Effect.runPromise(Effect.result(settingsFor(invalid))))._tag, "Failure");
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
          const registry = yield* makeContextRegistry({
            loadAll: () => (saved ? [saved] : []),
            save: (record) => {
              saved = record;
            },
          });
          const system = yield* ActorSystem.make().pipe(
            ActorSystem.provide(
              ContextQueries.layer,
              Layer.succeed(ContextRegistry, registry),

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
          const record = registry.reader.get("/goals/project")!;
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
            Object.values(registry.reader.snapshot()).filter((r) =>
              /^\/goals\/[^/]+$/.test(r.path),
            ),
            [record],
          );
          if (previous) {
            assert.deepEqual(canonical, {
              ...previous.snapshot,
              revision: previous.snapshot.revision + 1,
              state: { ...previous.snapshot.state, definition },
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
