import { homedir } from "node:os";
import { NodeServices } from "@effect/platform-node";
import { Layer } from "effect";
import { LocalConfig, StorageSettings } from "@aster/infra";

export const testLocalConfig = (
  options: Omit<Parameters<typeof LocalConfig.layer>[0], "homeDirectory" | "environment"> & {
    readonly environment?: Readonly<Record<string, string | undefined>>;
  },
) =>
  StorageSettings.layer.pipe(
    Layer.provideMerge(
      LocalConfig.layer({
        ...options,
        homeDirectory: homedir(),
        environment: options.environment ?? {},
      }).pipe(Layer.provideMerge(NodeServices.layer)),
    ),
  );
