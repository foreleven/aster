import { Config, ConfigProvider, Effect, Schema } from "effect";

export interface ConfiguredContext {
  readonly path: string;
  readonly description: string;
}

/** Read only explicit Context metadata; never traverse module config or credentials. */
export const configuredContexts = Effect.gen(function* () {
  const provider = yield* ConfigProvider.ConfigProvider;
  const collect = Effect.fnUntraced(function* (
    location: readonly string[],
    parent: string,
  ): Effect.fn.Return<
    readonly ConfiguredContext[],
    Config.ConfigError | ConfigProvider.SourceError
  > {
    const node = yield* provider.load(location);
    if (!node) return [];
    if (node._tag !== "Record")
      return yield* new ConfigProvider.SourceError({
        message: `Expected a Context mapping at ${location.join(".")}`,
      });
    const entries: ConfiguredContext[] = [];
    for (const key of [...node.keys].sort()) {
      if (!key.startsWith("/")) continue;
      const path = `${parent}${key}`;
      const child = [...location, key];
      const descriptionPath = [...child, "description"];
      if (yield* provider.load(descriptionPath)) {
        const description = yield* Config.schema(Schema.NonEmptyString, descriptionPath).pipe(
          Effect.provideService(ConfigProvider.ConfigProvider, provider),
        );
        entries.push({ path, description });
      }
      entries.push(...(yield* collect(child, path)));
      entries.push(...(yield* collect([...child, "children"], path)));
    }
    return entries;
  });
  return yield* collect(["contexts"], "");
});
