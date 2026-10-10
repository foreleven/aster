import { ConfigProvider, Effect } from "effect";

// Env trie keys cannot reconstruct camelCase, slugs or slash-prefixed keys.
// Preserve structural names from YAML/overrides while looking up env leaves by path.
export const overlay = (
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
