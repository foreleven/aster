import { ConfiguredModels } from "../src/models.js";
import assert from "node:assert/strict";
import { test } from "node:test";
import { ConfigProvider, Effect, Layer } from "effect";
import { AgentModels } from "@aster/core/agent";

const model = {
  name: "test",
  provider: "openai" as const,
  model: "gpt-4o-mini",
  url: "https://unused.invalid",
};

test("native model aliases resolve during registry acquisition", async () => {
  const provider = ConfigProvider.make((path) =>
    path[0] === "secrets"
      ? ConfigProvider.fromEnvRecord({}).load(path.slice(1))
      : ConfigProvider.fromUnknown({ config: { models: [model] } }).load(path),
  );
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const models = yield* AgentModels;
        assert.equal((yield* models.resolve("test")).id, "gpt-4o-mini");
        assert.equal((yield* models.resolve("missing").pipe(Effect.result))._tag, "Failure");
      }).pipe(
        Effect.provide(ConfiguredModels.layer.pipe(Layer.provide(ConfigProvider.layer(provider)))),
      ),
    ),
  );
});

test("missing model credentials and duplicate aliases fail acquisition before execution", async () => {
  const result = await Effect.runPromise(
    Effect.scoped(
      AgentModels.pipe(
        Effect.provide(
          ConfiguredModels.layer.pipe(
            Layer.provide(
              ConfigProvider.layer(
                ConfigProvider.fromUnknown({ config: { models: [{ ...model }, { ...model }] } }),
              ),
            ),
          ),
        ),
        Effect.result,
      ),
    ),
  );
  assert.equal(result._tag, "Failure");
});
