import { createHash } from "node:crypto";
import { AgentConversations } from "@aster/core/agent/session";
import { openNodeJsonlStorage } from "@earendil-works/pi-durable/storage/jsonl/node";
import { Effect, Layer, Path } from "effect";
import { StorageSettings } from "../storage/settings.js";
import { localWorkspaceEnvironment } from "./environment.js";

/** Native storage and workspace location policy belongs to the local adapter. */
export const ConversationStorage = {
  layer: Layer.unwrap(
    Effect.gen(function* () {
      const { root } = yield* StorageSettings;
      const path = yield* Path.Path;
      return AgentConversations.layer({
        openStorage: (owner, context) =>
          openNodeJsonlStorage(
            path.join(root, "conversations", createHash("sha256").update(owner).digest("hex")),
            context,
            { fsync: true },
          ),
        workspaceEnvironment: localWorkspaceEnvironment(path.join(root, "workspaces")),
      });
    }),
  ),
};
