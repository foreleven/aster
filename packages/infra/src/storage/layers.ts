import { join } from "node:path";
import { StorageSettings } from "./settings.js";
import { Effect, Layer } from "effect";
import { GoalScreeningStore } from "@aster/core";
import { makeFileGoalScreeningStore } from "./file-goal-screening.js";

export const FileGoalScreening = {
  layer: Layer.effect(
    GoalScreeningStore,
    StorageSettings.pipe(
      Effect.map(({ root }) => makeFileGoalScreeningStore(join(root, "evaluations"))),
    ),
  ),
};
