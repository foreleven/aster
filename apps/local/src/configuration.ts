import { FetchHttpClient } from "effect/http";
import { homedir } from "node:os";
import { join } from "node:path";
import { LocalConfig, StorageSettings } from "@aster/infra";
import { NodeServices } from "@effect/platform-node";
import { Layer } from "effect";

export const localConfiguration = (projectRoot: string, configPath: string) => {
  const sources = LocalConfig.layer({
    projectRoot,
    configPath,
    envPath: join(projectRoot, ".env"),
    homeDirectory: homedir(),
    environment: { ...process.env },
  }).pipe(Layer.provideMerge(Layer.mergeAll(NodeServices.layer, FetchHttpClient.layer)));
  return StorageSettings.layer.pipe(Layer.provideMerge(sources));
};
