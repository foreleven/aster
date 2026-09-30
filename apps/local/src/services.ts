import { Layer } from "effect";
import { AsterRuntime } from "@aster/core";
import {
  Models,
  FileContextStore,
  FileGoalHistory,
  SystemOneClientLive,
  AgentMemoryBackend,
  ExternalAgentsLive,
  MemoryIntegration,
  LarkIntegration,
} from "@aster/integrations";

/** Product choices only: modules own their dependency graphs and lifecycle. */
export const localRuntimeLayer = AsterRuntime.layer({
  integrations: [MemoryIntegration.layer, LarkIntegration.layer],
}).pipe(
  Layer.provide(
    Layer.mergeAll(
      FileContextStore.layer,
      FileGoalHistory.layer,
      Models.configured,
      SystemOneClientLive.layer,
      AgentMemoryBackend.layer,
      ExternalAgentsLive.layer,
    ),
  ),
);
