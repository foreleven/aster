import { homedir } from "node:os";
import { NodeServices } from "@effect/platform-node";
import { Effect, Layer, Path } from "effect";
import {
  ConfigLocation,
  decodeMemorySettings,
  LocalConfig,
  StorageSettings,
} from "../src/index.js";

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

export const testMemorySettings = (input: unknown, baseDir = "/tmp") =>
  decodeMemorySettings(input).pipe(
    Effect.provide(Path.layer),
    Effect.provideService(ConfigLocation, {
      baseDir,
      projectRoot: baseDir,
      envPath: `${baseDir}/.env`,
      homeDirectory: homedir(),
    }),
  );
