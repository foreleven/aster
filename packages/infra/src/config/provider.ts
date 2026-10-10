import { ConfigProvider, Effect, Layer, Path, Redacted } from "effect";
import { ProcessEnvironment } from "@aster/core";
import { ConfigLocation } from "./location.js";
import { overlay } from "./overlay.js";
import { decodeDotEnv, decodeYaml, readSource } from "./sources.js";

/** Snapshot all sources once. Lookup never reads files or the live process environment. */
export const LocalConfig = {
  layer: (options: {
    readonly configPath: string;
    readonly envPath: string;
    readonly projectRoot: string;
    readonly homeDirectory: string;
    readonly environment: Readonly<Record<string, string | undefined>>;
    readonly overrides?: unknown;
  }) =>
    Layer.unwrap(
      Effect.gen(function* () {
        const path = yield* Path.Path;
        const configPath = path.resolve(options.configPath);
        const envPath = path.resolve(options.envPath);
        const source = Object.fromEntries(
          Object.entries(options.environment).filter(([, value]) => value !== undefined),
        );
        const yamlText = yield* readSource(configPath);
        const envText = yield* readSource(envPath, true);
        const parsed = yield* decodeYaml(yamlText, configPath);
        const dotenv = decodeDotEnv(envText);
        const flags = { preserveEmptyStrings: true };
        const environment = ConfigProvider.fromEnvRecord(source, flags);
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
            baseDir: path.dirname(configPath),
            projectRoot: path.resolve(options.projectRoot),
            envPath,
            homeDirectory: path.resolve(options.homeDirectory),
          }),
          Layer.succeed(ProcessEnvironment, {
            values: Redacted.make({ ...dotenv, ...source }),
            privateKeys: Object.keys(dotenv),
          }),
        );
      }),
    ),
};
