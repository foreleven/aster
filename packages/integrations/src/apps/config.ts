import { Config, ConfigProvider, Context, Effect, Layer, Schema } from "effect";
import { RuntimeConfigurationError } from "@aster/core";
import type { AppName } from "./commands.js";

export const appDescriptions: Record<AppName, string> = {
  xiaohongshu:
    "Query Xiaohongshu travel notes, recommendations and comments through OpenCLI. Read-only and on demand.",
  ctrip:
    "Query Ctrip destinations, hotels, transport and travel packages through OpenCLI. Read-only and on demand; prices and availability are snapshots.",
};
export interface AppsConfiguration {
  readonly description: string;
  readonly apps: readonly { readonly name: AppName; readonly description: string }[];
}
export class AppsSettings extends Context.Service<AppsSettings, AppsConfiguration>()(
  "apps/Settings",
) {
  static readonly layer = Layer.effect(
    AppsSettings,
    Effect.gen(function* () {
      const provider = yield* ConfigProvider.ConfigProvider;
      const path = ["contexts", "/apps"];
      const root = yield* provider.load(path);
      if (root && root._tag !== "Record")
        return yield* new RuntimeConfigurationError({
          message: "contexts./apps must be an object",
        });
      if (
        root?._tag === "Record" &&
        [...root.keys].some((key) => !["description", "/ctrip", "/xiaohongshu"].includes(key))
      )
        return yield* new RuntimeConfigurationError({
          message: "Unsupported /apps child; use direct /ctrip and /xiaohongshu entries",
        });
      const description = yield* Config.schema(Schema.NonEmptyString, [
        ...path,
        "description",
      ]).pipe(Config.withDefault("On-demand application queries"));
      const apps: AppsConfiguration["apps"][number][] = [];
      for (const name of ["xiaohongshu", "ctrip"] as const) {
        const childPath = [...path, `/${name}`];
        const child = yield* provider.load(childPath);
        if (!child) continue;
        if (child._tag !== "Record")
          return yield* new RuntimeConfigurationError({
            message: `contexts./apps./${name} must be an object`,
          });
        // Read scalar settings directly: an optional enclosing object can hide invalid nested values.
        const description = yield* Config.schema(Schema.NonEmptyString, [
          ...childPath,
          "description",
        ]).pipe(Config.withDefault(appDescriptions[name]));
        apps.push({ name, description });
      }
      return { description, apps };
    }),
  );
}
