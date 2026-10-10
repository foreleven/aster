import { testLocalConfig } from "./configuration-fixtures.js";
import { serializeFixtureState } from "@aster/core/testing";
import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { Effect, Schema, Layer } from "effect";
import { DurableContext, ExternalAgents, GoalScreeningStore } from "@aster/core";
import { ConfiguredDurableInfrastructure } from "../src/storage/configured.js";
import { FileGoalScreening } from "../src/storage/layers.js";
import { StorageSettings } from "../src/storage/settings.js";
import { makeFileContextStore } from "../src/storage/file-context-store.js";

const setup = (t: { after: (cleanup: () => void) => void }, durable: unknown) => {
  const root = mkdtempSync(join(tmpdir(), "aster-storage-config-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const configPath = join(root, "config.yaml");
  writeFileSync(
    configPath,
    JSON.stringify({ config: { durable: { root: "data", ...Object(durable) } } }),
  );
  const sources = testLocalConfig({
    configPath,
    projectRoot: "/unused-project",
    envPath: join(root, ".env"),
    environment: {},
  });
  return { root, sources };
};

test("configured Local storage reopens Contexts and keeps journals under the same root", async (t) => {
  const { root, sources } = setup(t, {});
  const settings = await Effect.runPromise(
    Effect.scoped(StorageSettings.pipe(Effect.provide(sources))),
  );
  assert.equal(settings.root, join(root, "data"));
  const record = {
    text: serializeFixtureState({}),
    path: "/personal",
    description: "Personal",
    state: {},
    messages: [],
  };
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const agents = yield* ExternalAgents;
        assert.deepEqual(Object.keys(agents).sort(), ["codex", "doubao-delegate"]);
        const contexts = yield* DurableContext;
        yield* contexts.open({
          path: record.path,
          state: Schema.Struct({}),
          message: Schema.Never,
          serialize: (state) => JSON.stringify(state),
          initial: record,
        });
      }).pipe(Effect.provide(ConfiguredDurableInfrastructure.layer.pipe(Layer.provide(sources)))),
    ),
  );
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const contexts = yield* DurableContext;
        assert.equal(contexts.get("/personal")?.revision, 1);
      }).pipe(Effect.provide(ConfiguredDurableInfrastructure.layer.pipe(Layer.provide(sources)))),
    ),
  );
  assert.deepEqual(makeFileContextStore(settings.contextDirectory).loadAll(), []);
  // Journal services resolve configuration independently but share the process lease root.
  const screening = await Effect.runPromise(
    Effect.scoped(
      GoalScreeningStore.pipe(Effect.provide(FileGoalScreening.layer.pipe(Layer.provide(sources)))),
    ),
  );
  await Effect.runPromise(
    Effect.scoped(
      screening.append({
        screeningRecordId: "s1",
        sourcePath: "/chat",
        goalSlug: "test",
        summaryFingerprint: "f1",
        input: {
          context: {
            text: serializeFixtureState({ summary: "Evidence" }),
            path: "/chat",
            revision: 1,
            description: "Chat",
          },
          goalTitle: "Test",
          goalDescription: "Test",
          goalSummary: "",
        },
        score: 0.9,
        admitted: true,
        threshold: 0.8,
        policyVersion: "v1",
        model: "fake",
        latencyMs: 0,
        rationale: "Relevant",
        createdAt: "2026-10-02T00:00:00Z",
      }),
    ),
  );
  assert.match(
    readFileSync(join(settings.root, "evaluations/goal-screening.jsonl"), "utf8"),
    /Relevant/,
  );
});

test("configured storage reopens native Contexts without rewriting unchanged owners", async (t) => {
  const { sources } = setup(t, {});
  const options = {
    path: "/retained",
    state: Schema.Struct({ value: Schema.Int }),
    message: Schema.String,
    serialize: (state: { value: number }) => String(state.value),
    initial: { description: "Retained", state: { value: 1 }, messages: ["existing"] },
  };
  for (const restart of [false, true])
    await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const durable = yield* DurableContext;
          const owner = yield* durable.open(options);
          if (!restart) yield* owner.state.update(() => ({ value: 2 }));
          else {
            assert.equal((yield* owner.snapshot).revision, 2);
            assert.deepEqual(yield* owner.messages.list, ["existing"]);
            assert.equal(yield* durable.serializeState(options.path), "2");
          }
        }).pipe(Effect.provide(ConfiguredDurableInfrastructure.layer.pipe(Layer.provide(sources)))),
      ),
    );
});

test("empty storage root is rejected", async (t) => {
  const { sources } = setup(t, { root: "" });
  await assert.rejects(
    Effect.runPromise(Effect.scoped(StorageSettings.pipe(Effect.provide(sources)))),
  );
});
