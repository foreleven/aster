import { StorageSettings } from "../storage/settings.js";
import { Config, Context, Effect, Layer, Match, Path, Schema } from "effect";
import { ConfigLocation } from "../config/location.js";
import { MemoryConfigurationError } from "./errors.js";

const NonBlank = Schema.String.check(Schema.isPattern(/\S/));
const Llm = Schema.Struct({
  provider: Schema.Literals(["openai", "anthropic", "minimax", "gemini", "openrouter"]),
  model: NonBlank,
  apiKeyEnv: NonBlank,
  baseUrl: Schema.optional(NonBlank),
}).check(
  Schema.makeFilter(
    (value) =>
      value.baseUrl === undefined || ["openai", "anthropic", "minimax"].includes(value.provider),
    { expected: "This LLM provider does not support a custom baseUrl" },
  ),
);
const Embedding = Schema.Struct({
  provider: Schema.Literals(["local", "openai", "gemini", "voyage", "cohere", "openrouter"]),
  model: Schema.optional(NonBlank),
  apiKeyEnv: Schema.optional(NonBlank),
  baseUrl: Schema.optional(NonBlank),
}).check(
  Schema.makeFilter((value) => value.provider === "local" || value.apiKeyEnv !== undefined, {
    expected: "Remote embeddings require apiKeyEnv",
  }),
  Schema.makeFilter((value) => value.baseUrl === undefined || value.provider === "openai", {
    expected: "Only OpenAI embeddings support baseUrl",
  }),
  Schema.makeFilter(
    (value) => value.model === undefined || ["openai", "openrouter"].includes(value.provider),
    { expected: "This embedding provider has a fixed model" },
  ),
);
const Options = Schema.Struct({
  dataDir: Schema.optional(NonBlank),
  port: Schema.Int.check(Schema.isBetween({ minimum: 1024, maximum: 19512 })).pipe(
    Schema.withDecodingDefaultKey(Effect.succeed(3111)),
  ),
  autoCompress: Schema.optional(Schema.Boolean),
  llm: Schema.optionalKey(Llm),
  embedding: Schema.optionalKey(Schema.Union([Schema.Literal(false), Embedding])),
}).check(
  Schema.makeFilter((value) => value.autoCompress !== true || value.llm !== undefined, {
    expected: "autoCompress requires an explicit llm",
  }),
);
export const MemoryEntry = Schema.Struct({
  description: NonBlank.pipe(Schema.withDecodingDefaultKey(Effect.succeed("My long-term memory"))),
  config: Options.pipe(Schema.withDecodingDefaultKey(Effect.succeed({ port: 3111 }))),
});
export interface MemorySettingsValue {
  readonly description: string;
  readonly dataDir: string;
  readonly port: number;
  readonly autoCompress: boolean;
  readonly llm?: typeof Llm.Type;
  readonly embedding?: typeof Embedding.Type;
}

const normalize = Effect.fnUntraced(function* (entry: typeof MemoryEntry.Type) {
  const path = yield* Path.Path;
  const { baseDir, homeDirectory } = yield* ConfigLocation;
  const { root } = yield* StorageSettings;
  const { config } = entry;
  const dataDir = Match.value(config.dataDir).pipe(
    Match.when(undefined, () => path.join(root, "memory")),
    Match.when(
      (value) => value.startsWith("~/"),
      (value) => path.resolve(homeDirectory, value.slice(2)),
    ),
    Match.orElse((value) => path.resolve(baseDir, value)),
  );
  return {
    description: entry.description,
    dataDir,
    port: config.port,
    autoCompress: config.autoCompress ?? config.llm !== undefined,
    llm: config.llm,
    embedding: config.embedding || undefined,
  } satisfies MemorySettingsValue;
});
export const decodeMemorySettings = Effect.fn("Memory.decodeSettings")(function* (input: unknown) {
  const entry = yield* Schema.decodeUnknownEffect(MemoryEntry)(input ?? {}).pipe(
    Effect.mapError(
      (cause) =>
        new MemoryConfigurationError({
          message: `Invalid Memory settings: ${cause.message}`,
        }),
    ),
  );
  return yield* normalize(entry);
});
export class MemorySettings extends Context.Service<MemorySettings, MemorySettingsValue>()(
  "infra/memory/Settings",
) {
  static readonly layer = Layer.effect(
    MemorySettings,
    Effect.gen(function* () {
      const entry = yield* Config.schema(MemoryEntry, ["contexts", "/memory"]).pipe(
        Config.withDefault({ description: "My long-term memory", config: { port: 3111 } }),
      );
      return yield* normalize(entry);
    }),
  );
}
