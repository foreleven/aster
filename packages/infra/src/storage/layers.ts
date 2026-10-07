import { join } from "node:path";
import { storageSettings } from "./routing.js";
import { Effect, Layer } from "effect";
import { GoalScreeningStore } from "@aster/core";
import { makeFileGoalScreeningStore } from "./file-goal-screening.js";

export const FileGoalScreening = {
  layer: Layer.effect(
    GoalScreeningStore,
    storageSettings.pipe(
      Effect.map(({ root }) => makeFileGoalScreeningStore(join(root, "evaluations"))),
    ),
  ),
};
