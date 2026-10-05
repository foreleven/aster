import { NodeServices } from "@effect/platform-node";
import { AsterRuntime } from "@aster/core";
import {
  AgentMemoryBackend,
  ConfiguredDurableInfrastructure,
  FileGoalScreening,
  SystemOneClientLive,
} from "@aster/infra";
import { LarkIntegration, MailIntegration, AppsIntegration } from "@aster/integrations";
import { AgentConversations, Models } from "@aster/agent";
import { Layer } from "effect";

/** Product choices only: modules own their dependency graphs and lifecycle. */
export const localRuntimeLayer = AsterRuntime.layer({
  integrations: [LarkIntegration.layer, MailIntegration.layer, AppsIntegration.layer],
}).pipe(
  Layer.provide(
    Layer.mergeAll(
      ConfiguredDurableInfrastructure.layer,
      FileGoalScreening.layer,
      Models.configured,
      AgentConversations.layer,
      SystemOneClientLive.layer,
      AgentMemoryBackend.layer,
      NodeServices.layer,
    ),
  ),
);
