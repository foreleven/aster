import { readFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { parseEnv } from "node:util";
import { parse } from "yaml";
import { ConfigProvider, Effect, Layer } from "effect";
import { ConfigLocation, ProcessEnvironment } from "@aster/core";

// Env trie keys cannot reconstruct camelCase, slugs or slash-prefixed keys.
// Preserve structural names from YAML/overrides while looking up env leaves by path.
const overlay = (
  primary: ConfigProvider.ConfigProvider,
  fallback: ConfigProvider.ConfigProvider,
  enumerate = true,
) =>
  ConfigProvider.make((path) =>
    Effect.gen(function* () {
      const first = yield* primary.load(path);
      if (!first) return yield* fallback.load(path);
      if (first.value !== undefined) return first;
      const second = yield* fallback.load(path);
      if (first._tag === "Record")
        return ConfigProvider.makeRecord(
          new Set([
            ...(enumerate ? first.keys : []),
            ...(second?._tag === "Record" ? second.keys : []),
          ]),
        );
      if (!enumerate && first._tag === "Array" && second?._tag === "Array")
        return ConfigProvider.makeArray(Math.max(first.length, second.length));
      return first;
    }),
  );

const read = (path: string, optional = false) =>
  Effect.tryPromise({
    try: async () => {
      try {
        return await readFile(path, "utf8");
      } catch (cause) {
        if (optional && (cause as NodeJS.ErrnoException).code === "ENOENT") return "";
        throw cause;
      }
    },
    catch: (cause) =>
      new ConfigProvider.SourceError({ message: `Cannot read configuration: ${path}`, cause }),
  });

/** Snapshot all sources once. Neither acquisition nor lookup mutates process.env. */
export const LocalConfig = {
  layer: (options: {
    readonly configPath: string;
    readonly envPath: string;
    readonly projectRoot: string;
    readonly overrides?: unknown;
    readonly environment?: NodeJS.ProcessEnv;
  }) =>
    Layer.unwrap(
      Effect.gen(function* () {
        const configPath = resolve(options.configPath);
        const source = Object.fromEntries(
          Object.entries(options.environment ?? process.env).filter(
            (entry): entry is [string, string] => entry[1] !== undefined,
          ),
        );
        const yamlText = yield* read(configPath);
        const envText = yield* read(options.envPath, true);
        const parsed = yield* Effect.try({
          try: () => {
            const value: unknown = parse(yamlText);
            if (value !== null && (typeof value !== "object" || Array.isArray(value)))
              throw new Error("Expected a mapping");
            return value ?? {};
          },
          catch: () => new ConfigProvider.SourceError({ message: `Invalid YAML: ${configPath}` }),
        });
        const dotenv = parseEnv(envText);
        const flags = { preserveEmptyStrings: true };
        const environment = ConfigProvider.fromEnv({ env: source, ...flags });
        const envFile = ConfigProvider.fromEnvRecord(dotenv, flags);
        const application = (provider: ConfigProvider.ConfigProvider) =>
          provider.pipe(ConfigProvider.nested("ASTER"), ConfigProvider.constantCase);
        const values = overlay(
          ConfigProvider.fromUnknown(options.overrides ?? {}, flags),
          overlay(
            application(environment),
            overlay(application(envFile), ConfigProvider.fromUnknown(parsed, flags), false),
            false,
          ),
        );
        const secrets = ConfigProvider.orElse(environment, envFile);
        const provider = ConfigProvider.make((path) =>
          path[0] === "secrets" ? secrets.load(path.slice(1)) : values.load(path),
        );
        return Layer.mergeAll(
          ConfigProvider.layer(provider),
          Layer.succeed(ConfigLocation, {
            baseDir: dirname(configPath),
            projectRoot: resolve(options.projectRoot),
            envPath: resolve(options.envPath),
          }),
          Layer.succeed(ProcessEnvironment, {
            values: { ...dotenv, ...source },
            privateKeys: Object.keys(dotenv),
          }),
        );
      }),
    ),
};
