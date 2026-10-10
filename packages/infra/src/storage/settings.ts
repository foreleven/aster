import { Config, Context, Effect, Layer, Path, Schema } from "effect";
import { ConfigLocation } from "../config/location.js";

export class StorageSettings extends Context.Service<
  StorageSettings,
  {
    readonly root: string;
  }
>()("infra/storage/Settings") {
  static readonly layer = Layer.effect(
    StorageSettings,
    Effect.gen(function* () {
      const path = yield* Path.Path;
      const { baseDir, homeDirectory } = yield* ConfigLocation;
      const configuredRoot = yield* Config.schema(Schema.optional(Schema.NonEmptyString), [
        "config",
        "durable",
        "root",
      ]);
      const root =
        configuredRoot === undefined
          ? path.join(homeDirectory, ".aster")
          : path.resolve(baseDir, configuredRoot);
      return { root };
    }),
  );
}
