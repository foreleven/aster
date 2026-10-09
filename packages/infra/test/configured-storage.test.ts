import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { Effect, Layer } from "effect";
import { DurableContext, ExternalAgents, GoalScreeningStore } from "@aster/core";
import { LocalConfig } from "../src/config/provider.js";
import { ConfiguredDurableInfrastructure } from "../src/storage/configured.js";
import { FileGoalScreening } from "../src/storage/layers.js";
import { storageSettings } from "../src/storage/settings.js";
import { makeFileContextStore } from "../src/storage/file-context-store.js";

const setup = (t: { after: (cleanup: () => void) => void }, durable: unknown) => {
  const root = mkdtempSync(join(tmpdir(), "aster-storage-config-"));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const configPath = join(root, "config.yaml");
  writeFileSync(
    configPath,
    JSON.stringify({ config: { durable: { root: "data", ...Object(durable) } } }),
  );
  const sources = LocalConfig.layer({
    configPath,
    projectRoot: "/unused-project",
    envPath: join(root, ".env"),
    environment: {},
  });
  return { root, sources };
};

test("configured Local storage reopens Contexts and keeps journals under the same root", async (t) => {
  const { root, sources } = setup(t, {});
  const settings = await Effect.runPromise(storageSettings.pipe(Effect.provide(sources)));
  assert.equal(settings.root, join(root, "data"));
  const record = { path: "/personal", description: "Personal", state: {}, messages: [] };
  await Effect.runPromise(
    Effect.gen(function* () {
      const agents = yield* ExternalAgents;
      assert.deepEqual(Object.keys(agents).sort(), ["codex", "doubao-delegate"]);
      const contexts = yield* DurableContext;
      yield* contexts.commit(record, { expectedRevision: 0 });
    }).pipe(Effect.provide(ConfiguredDurableInfrastructure.layer.pipe(Layer.provide(sources)))),
  );
  await Effect.runPromise(
    Effect.gen(function* () {
      const contexts = yield* DurableContext;
      assert.equal(contexts.get("/personal")?.revision, 1);
    }).pipe(Effect.provide(ConfiguredDurableInfrastructure.layer.pipe(Layer.provide(sources)))),
  );
  assert.deepEqual(makeFileContextStore(settings.contextDirectory).loadAll(), [
    { snapshot: { ...record, revision: 1 }, events: [] },
  ]);
  // Journal services resolve configuration independently but share the process lease root.
  const screening = await Effect.runPromise(
    GoalScreeningStore.pipe(Effect.provide(FileGoalScreening.layer.pipe(Layer.provide(sources)))),
  );
  await Effect.runPromise(
    screening.append({
      screeningRecordId: "s1",
      sourcePath: "/chat",
      goalSlug: "test",
      summaryFingerprint: "f1",
      input: {
        context: {
          path: "/chat",
          revision: 1,
          description: "Chat",
          state: { summary: "Evidence" },
          messages: [],
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
  );
  assert.match(
    readFileSync(join(settings.root, "evaluations/goal-screening.jsonl"), "utf8"),
    /Relevant/,
  );
});

test("configured storage loads existing Local revisions without rewriting them", async (t) => {
  const { sources } = setup(t, {});
  const settings = await Effect.runPromise(storageSettings.pipe(Effect.provide(sources)));
  const record = {
    path: "/personal",
    description: "Personal",
    revision: 3,
    state: {},
    messages: ["existing"],
  };
  const store = makeFileContextStore(settings.contextDirectory);
  store.save({ snapshot: record, events: [] });
  await Effect.runPromise(
    Effect.gen(function* () {
      const contexts = yield* DurableContext;
      assert.deepEqual(contexts.get(record.path), record);
    }).pipe(Effect.provide(ConfiguredDurableInfrastructure.layer.pipe(Layer.provide(sources)))),
  );
  assert.deepEqual(store.loadAll(), [{ snapshot: record, events: [] }]);
});

test("empty storage root is rejected", async (t) => {
  const { sources } = setup(t, { root: "" });
  await assert.rejects(Effect.runPromise(storageSettings.pipe(Effect.provide(sources))));
});
