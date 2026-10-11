import { AsterRuntime } from "@aster/core";
import {
  AgentMemoryBackend,
  ConversationStorage,
  ContextStorage,
  ConfiguredModels,
  ExternalExecutors,
  SystemOneTransport,
} from "@aster/infra";
import {
  LarkIntegration,
  MailIntegration,
  AppsIntegration,
  WebSearchIntegration,
} from "@aster/integrations";
import { Layer } from "effect";

export const localConversationsLayer = ConversationStorage.layer;

/** Product choices only: modules own their dependency graphs and lifecycle. */
export const localRuntimeLayer = AsterRuntime.layer({
  integrations: [
    LarkIntegration.layer,
    MailIntegration.layer,
    AppsIntegration.layer,
    WebSearchIntegration.layer,
  ],
}).pipe(
  Layer.provide(
    Layer.mergeAll(
      ContextStorage.layer,
      ExternalExecutors.layer,
      ConfiguredModels.layer,
      localConversationsLayer,
      SystemOneTransport.layer,
      AgentMemoryBackend.layer,
    ),
  ),
);
