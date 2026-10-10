import { parse as parseYaml } from "yaml";
import { parse as parseDotEnv } from "dotenv";
import { ConfigProvider, Effect, FileSystem, Predicate, Redacted } from "effect";

export const readSource = Effect.fn("Config.readSource")(function* (
  path: string,
  optional = false,
) {
  const fs = yield* FileSystem.FileSystem;
  return yield* fs.readFileString(path).pipe(
    Effect.catchTag("PlatformError", (error) =>
      optional && error.reason._tag === "NotFound" ? Effect.succeed("") : Effect.fail(error),
    ),
    Effect.mapError(
      (cause) =>
        new ConfigProvider.SourceError({
          message: `Cannot read configuration: ${path}`,
          cause: Redacted.make(cause),
        }),
    ),
  );
});

export const decodeYaml = Effect.fn("Config.decodeYaml")(function* (text: string, path: string) {
  const value: unknown = yield* Effect.try({
    try: () => parseYaml(text),
    catch: (cause) =>
      new ConfigProvider.SourceError({
        message: `Invalid YAML: ${path}`,
        cause: Redacted.make(cause),
      }),
  });
  if (value !== null && value !== undefined && (!Predicate.isObject(value) || Array.isArray(value)))
    return yield* new ConfigProvider.SourceError({
      message: `Invalid YAML: ${path}; expected a mapping`,
    });
  return value ?? {};
});

/** Decode once; both provider lookup and subprocess filtering need original variable names. */
export const decodeDotEnv = (text: string): Readonly<Record<string, string>> => parseDotEnv(text);
