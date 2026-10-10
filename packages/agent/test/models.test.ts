import assert from "node:assert/strict";
import { test } from "node:test";
import { Effect, Redacted } from "effect";
import { Models, type ModelConfig } from "../src/models.js";

const entry: ModelConfig = {
  name: "test",
  provider: "openai",
  model: "test-model",
  url: "https://unused.invalid",
  apiKey: Redacted.make("${LITERAL_CREDENTIAL}"),
};

test("explicit model registration consumes resolved credentials without configuration sources", async () => {
  await Effect.runPromise(
    Effect.scoped(
      Models.use((models) =>
        models.resolve("test").pipe(
          Effect.map((resolved) => {
            assert.equal(resolved.getApiKey(), "${LITERAL_CREDENTIAL}");
            assert.equal(resolved.model.id, "test-model");
          }),
        ),
      ).pipe(Effect.provide(Models.layer([entry]))),
    ),
  );
});

test("the model registry rejects duplicate registrations without loading credentials", async () => {
  const result = await Effect.runPromise(
    Effect.scoped(Models.pipe(Effect.provide(Models.layer([entry, entry])), Effect.result)),
  );
  assert.equal(result._tag, "Failure");
  if (result._tag === "Failure") assert.match(result.failure.message, /Model names must be unique/);
});
