import {
  createModels,
  createProvider,
  type Model,
  type ProviderStreams,
} from "@earendil-works/pi-ai";
import { Effect, Layer } from "effect";
import { AgentModels } from "../src/agent/models.js";
import { AgentError } from "../src/agent/shared/contracts.js";

export type TestModel = Model<"openai-completions">;
export const testModel: TestModel = {
  id: "test",
  name: "test",
  provider: "test",
  api: "openai-completions",
  baseUrl: "http://unused",
  reasoning: false,
  input: ["text"],
  contextWindow: 200000,
  maxTokens: 8192,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0 },
};

export const testModels = (stream: ProviderStreams["stream"]): Layer.Layer<AgentModels> => {
  const models = createModels();
  models.setProvider(
    createProvider({
      id: "test",
      name: "test",
      models: [testModel],
      auth: { apiKey: { name: "test", resolve: async () => ({ auth: { apiKey: "unused" } }) } },
      api: { stream, streamSimple: stream },
    }),
  );
  return Layer.succeed(
    AgentModels,
    AgentModels.of({
      models,
      resolve: (alias) =>
        alias === "test"
          ? Effect.succeed(testModel)
          : Effect.fail(new AgentError(`Unknown test model: ${alias}`)),
    }),
  );
};
