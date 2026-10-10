import { AsterRuntime } from "@aster/core";
import {
  AgentMemoryBackend,
  ConfiguredDurableInfrastructure,
  ConfiguredModels,
  FileGoalScreening,
  SystemOneClientLive,
  StorageSettings,
} from "@aster/infra";
import { LarkIntegration, MailIntegration, AppsIntegration } from "@aster/integrations";
import { AgentConversations } from "@aster/agent/harness";
import { join } from "node:path";
import { Effect, Layer } from "effect";

/** Conversation storage shares the host's resolved and exclusively owned root. */
export const localConversationsLayer = Layer.unwrap(
  StorageSettings.pipe(
    Effect.map(({ root }) => AgentConversations.layer(join(root, "conversations"))),
  ),
);

/** Product choices only: modules own their dependency graphs and lifecycle. */
export const localRuntimeLayer = AsterRuntime.layer({
  integrations: [LarkIntegration.layer, MailIntegration.layer, AppsIntegration.layer],
}).pipe(
  Layer.provide(
    Layer.mergeAll(
      ConfiguredDurableInfrastructure.layer,
      FileGoalScreening.layer,
      ConfiguredModels.layer,
      localConversationsLayer,
      SystemOneClientLive.layer,
      AgentMemoryBackend.layer,
    ),
  ),
);
