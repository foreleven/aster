import { join } from "node:path";
import { storageSettings } from "./routing.js";
import { Effect, Layer } from "effect";
import {
  ContextStore,
  GoalHistoryStore,
  GoalScreeningStore,
  LocalDurableContext,
} from "@aster/core";
import { makeFileContextStore } from "./file-context-store.js";
import { makeFileGoalHistory } from "./file-goal-history.js";
import { makeFileGoalScreeningStore } from "./file-goal-screening.js";

export const FileContextStore = {
  layer: Layer.effect(
    ContextStore,
    Effect.try(() => makeFileContextStore()),
  ),
};
export const FileGoalHistory = {
  layer: Layer.effect(
    GoalHistoryStore,
    storageSettings.pipe(Effect.map(({ root }) => makeFileGoalHistory(join(root, "goals")))),
  ),
};
export const FileGoalScreening = {
  layer: Layer.effect(
    GoalScreeningStore,
    storageSettings.pipe(
      Effect.map(({ root }) => makeFileGoalScreeningStore(join(root, "evaluations"))),
    ),
  ),
};

/** The host selects one authoritative backend; domain owners never open stores. */
export const FileDurableContext = {
  layer: LocalDurableContext.layer.pipe(Layer.provide(FileContextStore.layer)),
};
