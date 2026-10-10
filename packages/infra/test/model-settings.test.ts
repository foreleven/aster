import { ConfiguredModels } from "../src/config/models.js";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ConfigProvider, Effect, Layer } from "effect";
import { Models } from "@aster/agent";

const model = {
  name: "test",
  provider: "openai" as const,
  model: "test-model",
  url: "https://unused.invalid",
  apiKey: "${MODEL_KEY}",
};

test("model credentials resolve during registry acquisition and remain captured", async () => {
  const environment = { MODEL_KEY: "captured" };
  const provider = ConfigProvider.make((path) =>
    path[0] === "secrets"
      ? ConfigProvider.fromEnvRecord(environment).load(path.slice(1))
      : ConfigProvider.fromUnknown({ config: { models: [model] } }).load(path),
  );
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const models = yield* Models;
        environment.MODEL_KEY = "changed";
        assert.equal((yield* models.resolve("test")).getApiKey(), "captured");
        assert.equal((yield* models.resolve("missing").pipe(Effect.result))._tag, "Failure");
      }).pipe(
        Effect.provide(ConfiguredModels.layer.pipe(Layer.provide(ConfigProvider.layer(provider)))),
      ),
    ),
  );
});

test("missing model credentials and duplicate aliases fail acquisition before execution", async () => {
  for (const models of [
    [model],
    [
      { ...model, apiKey: "literal" },
      { ...model, apiKey: "literal" },
    ],
  ]) {
    const result = await Effect.runPromise(
      Effect.scoped(
        Models.pipe(
          Effect.provide(
            ConfiguredModels.layer.pipe(
              Layer.provide(
                ConfigProvider.layer(ConfigProvider.fromUnknown({ config: { models } })),
              ),
            ),
          ),
          Effect.result,
        ),
      ),
    );
    assert.equal(result._tag, "Failure");
  }
});
