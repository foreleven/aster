import { testLocalConfig } from "./configuration-fixtures.js";
import { serializeFixtureState } from "@aster/core/testing";
import assert from "node:assert/strict";
import { existsSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { Effect, Schema, Layer } from "effect";
import { DurableContext } from "@aster/core";
import { ContextStorage } from "../src/context/layer.js";
import { StorageSettings } from "../src/storage/settings.js";

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

test("configured Context storage acquires independently of executors", async (t) => {
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
        const contexts = yield* DurableContext;
        yield* contexts.open({
          path: record.path,
          state: Schema.Struct({}),
          message: Schema.Never,
          serialize: (state) => JSON.stringify(state),
          initial: record,
        });
      }).pipe(Effect.provide(ContextStorage.layer.pipe(Layer.provide(sources)))),
    ),
  );
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const contexts = yield* DurableContext;
        assert.equal(contexts.get("/personal")?.revision, 1);
      }).pipe(Effect.provide(ContextStorage.layer.pipe(Layer.provide(sources)))),
    ),
  );
  assert.equal(existsSync(join(settings.root, "context-sessions")), true);
  assert.equal(existsSync(join(settings.root, "actors")), false);
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
        }).pipe(Effect.provide(ContextStorage.layer.pipe(Layer.provide(sources)))),
      ),
    );
});

test("empty storage root is rejected", async (t) => {
  const { sources } = setup(t, { root: "" });
  await assert.rejects(
    Effect.runPromise(Effect.scoped(StorageSettings.pipe(Effect.provide(sources)))),
  );
});
