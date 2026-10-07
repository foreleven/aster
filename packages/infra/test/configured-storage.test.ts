import assert from "node:assert/strict";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { Effect, Layer } from "effect";
import { DurableContext, GoalScreeningStore } from "@aster/core";
import { LocalConfig } from "../src/config/provider.js";
import { ConfiguredDurableInfrastructure } from "../src/storage/configured.js";
import { FileGoalScreening } from "../src/storage/layers.js";
import { storageSettings, routingAuthorityStore } from "../src/storage/routing.js";
import { migrateContextStorage } from "../src/storage/migration.js";
import { makeFileContextStore } from "../src/storage/file-context-store.js";

const setup = (t: { after: (cleanup: () => void) => void }, durable: unknown) => {
  const root = mkdtempSync(join(tmpdir(), "aster-routing-config-"));
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

test("configured routing opens model-free Pi, persists authority, and keeps journals under the same root", async (t) => {
  const { root, sources } = setup(t, { pi: {}, routes: [{ prefix: "/personal", backend: "pi" }] });
  const settings = await Effect.runPromise(storageSettings.pipe(Effect.provide(sources)));
  assert.equal(settings.root, join(root, "data"));
  const record = { path: "/personal", description: "Personal", state: {}, messages: [] };
  await Effect.runPromise(
    Effect.gen(function* () {
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
  assert.deepEqual(makeFileContextStore(settings.authority.localDirectory).loadAll(), []);
  const stored = await Effect.runPromise(
    routingAuthorityStore(settings.root).pipe(Effect.flatMap((store) => store.read)),
  );
  assert.deepEqual(stored?.value, settings.authority);
  // Config mismatch is detected before any replacement backend is activated.
  writeFileSync(
    join(root, "config.yaml"),
    JSON.stringify({ config: { durable: { root: "data", pi: {}, routes: [] } } }),
  );
  await assert.rejects(
    Effect.runPromise(
      DurableContext.pipe(
        Effect.provide(ConfiguredDurableInfrastructure.layer.pipe(Layer.provide(sources))),
      ),
    ),
    /routing differs/,
  );
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
        contextSummary: "Evidence",
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

test("existing Local records require explicit migration before configured Pi activation", async (t) => {
  const { sources } = setup(t, { pi: {}, routes: [{ prefix: "/personal", backend: "pi" }] });
  const settings = await Effect.runPromise(storageSettings.pipe(Effect.provide(sources)));
  const record = {
    path: "/personal",
    description: "Personal",
    revision: 3,
    state: {},
    messages: ["existing"],
  };
  makeFileContextStore(settings.authority.localDirectory).save({ snapshot: record, events: [] });
  await assert.rejects(
    Effect.runPromise(
      DurableContext.pipe(
        Effect.provide(ConfiguredDurableInfrastructure.layer.pipe(Layer.provide(sources))),
      ),
    ),
    /ContextRecoveryError/,
  );
  await Effect.runPromise(migrateContextStorage(settings).pipe(Effect.scoped));
  await Effect.runPromise(
    Effect.gen(function* () {
      const contexts = yield* DurableContext;
      assert.deepEqual(contexts.get("/personal"), record);
    }).pipe(Effect.provide(ConfiguredDurableInfrastructure.layer.pipe(Layer.provide(sources)))),
  );
});

for (const durable of [
  { routes: [{ prefix: "/personal", backend: "pi" }] },
  { pi: { directory: "data/actors" } },
  { pi: { directory: "data/storage-authority" } },
  {
    routes: [
      { prefix: "/personal", backend: "local" },
      { prefix: "/personal", backend: "local" },
    ],
  },
])
  test(`invalid storage routing is rejected: ${JSON.stringify(durable)}`, async (t) => {
    const { sources } = setup(t, durable);
    const failure = await Effect.runPromise(
      storageSettings.pipe(Effect.provide(sources), Effect.flip),
    );
    assert.equal(failure._tag, "StorageRoutingError");
  });

test("standalone Pi execution resolves storage beside captured configuration", async (t) => {
  const { Models, PiStorageLease } = await import("@aster/agent");
  const { makeExternalAgents } = await import("../src/agents.js");
  const { root } = setup(t, {});
  const configPath = join(root, "config.yaml");
  writeFileSync(
    configPath,
    JSON.stringify({ agents: { pi: { model: "fake", storageDirectory: "execution" } } }),
  );
  const sources = LocalConfig.layer({
    configPath,
    projectRoot: "/unrelated",
    envPath: join(root, ".env"),
    environment: {},
  });
  const models = Layer.succeed(Models, {
    resolve: () =>
      Effect.succeed({
        model: {
          id: "fake",
          name: "fake",
          provider: "test",
          api: "openai-completions",
          baseUrl: "http://unused",
          reasoning: false,
          input: ["text"],
          contextWindow: 10000,
          maxTokens: 100,
          cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
        },
        getApiKey: () => "fake",
        stream: () => {
          throw new Error("Startup must not invoke a model");
        },
      }),
  });
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const agents = yield* makeExternalAgents();
        assert.ok(agents.pi);
        assert.equal(
          typeof readFileSync(join(root, "execution", ".aster-owner.json"), "utf8"),
          "string",
        );
        const conflict = yield* PiStorageLease.acquire(join(root, "execution"), "competitor").pipe(
          Effect.flip,
        );
        assert.equal(conflict._tag, "PiStorageLeaseError");
      }),
    ).pipe(Effect.provide(Layer.mergeAll(sources, models))),
  );
});
