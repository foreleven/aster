import assert from "node:assert/strict";
import { test } from "node:test";
import { ConfigProvider, Effect, Layer } from "effect";
import { GoalSettings } from "../src/goals/settings.js";
import { ReactionSettings } from "../src/reactions/settings.js";

test("Goal settings apply reasoning defaults and reject incomplete supplied options", async () => {
  const acquire = (goals?: unknown) =>
    GoalSettings.pipe(
      Effect.provide(
        GoalSettings.layer.pipe(
          Layer.provide(
            ConfigProvider.layer(
              ConfigProvider.fromUnknown(
                { config: { agent: { model: "fallback" }, goals } },
                { preserveEmptyStrings: true },
              ),
            ),
          ),
        ),
      ),
    );
  assert.deepEqual((await Effect.runPromise(Effect.scoped(acquire()))).reasoning, {
    model: "fallback",
    contextTokens: 200000,
    reserveTokens: 8192,
  });
  assert.deepEqual(
    (await Effect.runPromise(Effect.scoped(acquire({ model: "selected" })))).reasoning,
    {
      model: "selected",
      contextTokens: 200000,
      reserveTokens: 8192,
    },
  );
  for (const input of [
    {},
    { contextTokens: 32000 },
    { model: "" },
    { model: "test", reserveTokens: 16000, contextTokens: 32000 },
  ]) {
    assert.equal(
      (await Effect.runPromise(Effect.scoped(acquire(input).pipe(Effect.result))))._tag,
      "Failure",
    );
  }
});

test("Reaction settings retain defaults but never mask invalid concurrency", async () => {
  const acquire = (reactions?: unknown) =>
    ReactionSettings.pipe(
      Effect.provide(
        ReactionSettings.layer.pipe(
          Layer.provide(
            ConfigProvider.layer(
              ConfigProvider.fromUnknown({ config: { reactions } }, { preserveEmptyStrings: true }),
            ),
          ),
        ),
      ),
    );
  assert.deepEqual(await Effect.runPromise(Effect.scoped(acquire())), {
    matchConcurrency: 4,
    deliveryConcurrency: 4,
  });
  for (const input of [
    { matchConcurrency: 0 },
    { deliveryConcurrency: 33 },
    { matchConcurrency: "" },
  ]) {
    assert.equal(
      (await Effect.runPromise(Effect.scoped(acquire(input).pipe(Effect.result))))._tag,
      "Failure",
    );
  }
});
