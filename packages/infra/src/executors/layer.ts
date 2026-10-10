import { ExternalAgents, ProcessEnvironment } from "@aster/core";
import { Effect, Layer, Path } from "effect";
import { StorageSettings } from "../storage/settings.js";
import { agentEnvironment } from "./environment.js";
import { makeCodexExecutor } from "./codex/executor.js";
import { makeDoubaoExecutor } from "./doubao/executor.js";
import { doubaoSettings } from "./doubao/settings.js";

export const ExternalExecutors = {
  layer: Layer.effect(
    ExternalAgents,
    Effect.gen(function* () {
      const environment = agentEnvironment(yield* ProcessEnvironment);
      const path = yield* Path.Path;
      const { root } = yield* StorageSettings;
      const taskRoot = path.join(root, "tasks");
      const settings = yield* doubaoSettings;
      return {
        codex: yield* makeCodexExecutor(environment, taskRoot),
        "doubao-delegate": yield* makeDoubaoExecutor(
          { ...environment, DOUBAO_CDP_ENDPOINT: settings.endpoint },
          { ...settings, taskRoot },
        ),
      };
    }),
  ),
};
