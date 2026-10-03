import { AsterRuntime } from "@aster/core";
import {
  AgentMemoryBackend,
  ConfiguredDurableInfrastructure,
  FileGoalHistory,
  FileGoalScreening,
  LarkIntegration,
  MailIntegration,
  MemoryIntegration,
  Models,
  SystemOneClientLive,
} from "@aster/integrations";
import { Layer } from "effect";

/** Product choices only: modules own their dependency graphs and lifecycle. */
export const localRuntimeLayer = AsterRuntime.layer({
  integrations: [MemoryIntegration.layer, LarkIntegration.layer, MailIntegration.layer],
}).pipe(
  Layer.provide(
    Layer.mergeAll(
      ConfiguredDurableInfrastructure.layer,
      FileGoalHistory.layer,
      FileGoalScreening.layer,
      Models.configured,
      SystemOneClientLive.layer,
      AgentMemoryBackend.layer,
    ),
  ),
);
