import { Effect, Layer } from "effect";
import { ContextStore, GoalHistoryStore } from "@aster/core";
import { makeFileContextStore } from "./file-context-store.js";
import { makeFileGoalHistory } from "./file-goal-history.js";

export const FileContextStore = {
  layer: Layer.effect(
    ContextStore,
    Effect.try(() => makeFileContextStore()),
  ),
};
export const FileGoalHistory = {
  layer: Layer.sync(GoalHistoryStore, () => makeFileGoalHistory()),
};
