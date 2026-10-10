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
  const { config } = entry;
  const dataDir = Match.value(config.dataDir).pipe(
    Match.when(undefined, () => path.join(homeDirectory, ".aster", "memory")),
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

const keyVariables = [
  "OPENAI_API_KEY",
  "ANTHROPIC_API_KEY",
  "MINIMAX_API_KEY",
  "GEMINI_API_KEY",
  "GOOGLE_API_KEY",
  "OPENROUTER_API_KEY",
  "VOYAGE_API_KEY",
  "COHERE_API_KEY",
];

/** Blank overrides also mask ~/.agentmemory/.env; missing variables would not. */
export const memoryEnvironment = Effect.fn("Memory.environment")(function* (
  config: MemorySettingsValue,
  source: Readonly<Record<string, string | undefined>>,
) {
  const env: Record<string, string | undefined> = { ...source };
  for (const key of keyVariables) env[key] = "";
  for (const prefix of ["OPENAI", "ANTHROPIC", "MINIMAX", "GEMINI", "OPENROUTER"]) {
    env[`${prefix}_BASE_URL`] = "";
    env[`${prefix}_MODEL`] = "";
  }
  Object.assign(env, {
    NO_PROXY: [source.NO_PROXY ?? source.no_proxy ?? "", "127.0.0.1", "localhost", "::1"]
      .filter(Boolean)
      .join(","),
    CI: "1",
    AGENTMEMORY_USE_DOCKER: "false",
    AGENTMEMORY_ALLOW_AGENT_SDK: "false",
    AGENTMEMORY_AUTO_COMPRESS: String(config.autoCompress),
    AGENTMEMORY_GRAPH_EXTRACTION: "false",
    FALLBACK_PROVIDERS: "",
    CLAUDE_MEMORY_BRIDGE: "false",
    AGENTMEMORY_INJECT_CONTEXT: "false",
    AGENTMEMORY_III_VERSION: "0.11.2",
    AGENTMEMORY_III_CONFIG: "",
    AGENTMEMORY_DATA_DIR: config.dataDir,
    III_REST_PORT: String(config.port),
    III_STREAM_PORT: String(config.port + 1),
    III_STREAMS_PORT: String(config.port + 1),
    III_ENGINE_PORT: String(config.port + 46023),
    III_ENGINE_URL: `ws://127.0.0.1:${config.port + 46023}`,
    AGENTMEMORY_URL: `http://127.0.0.1:${config.port}`,
    AGENTMEMORY_VIEWER_URL: `http://127.0.0.1:${config.port + 2}`,
    AGENTMEMORY_AGENT_SCOPE: "shared",
    AGENT_ID: "signals",
    OPENAI_API_KEY_FOR_LLM: config.llm?.provider === "openai" ? "true" : "false",
    EMBEDDING_PROVIDER: config.embedding?.provider ?? "none",
    OPENAI_EMBEDDING_BASE_URL: "",
    OPENAI_EMBEDDING_MODEL: "",
    OPENAI_EMBEDDING_DIMENSIONS: "",
    OPENROUTER_EMBEDDING_MODEL: "",
    OPENROUTER_EMBEDDING_DIMENSIONS: "",
  });
  const credential = (name: string) =>
    source[name]?.trim()
      ? Effect.succeed(source[name]!)
      : Effect.fail(
          new MemoryConfigurationError({
            message: `Memory credential environment variable is missing: ${name}`,
          }),
        );
  if (config.llm) {
    const prefix = config.llm.provider.toUpperCase();
    env[`${prefix}_API_KEY`] = yield* credential(config.llm.apiKeyEnv);
    env[`${prefix}_MODEL`] = config.llm.model;
    if (config.llm.baseUrl) env[`${prefix}_BASE_URL`] = config.llm.baseUrl;
  }
  if (config.embedding && config.embedding.provider !== "local") {
    const prefix = config.embedding.provider.toUpperCase();
    const key = yield* credential(config.embedding.apiKeyEnv!);
    // Upstream shares provider credentials between its LLM and embedding adapters.
    if (config.llm?.provider === config.embedding.provider && env[`${prefix}_API_KEY`] !== key) {
      return yield* new MemoryConfigurationError({
        message:
          "agentmemory requires the same credential for LLM and embeddings of the same provider",
      });
    }
    env[`${prefix}_API_KEY`] = key;
    if (config.embedding.model) env[`${prefix}_EMBEDDING_MODEL`] = config.embedding.model;
    if (config.embedding.baseUrl) env[`${prefix}_EMBEDDING_BASE_URL`] = config.embedding.baseUrl;
  }
  // Keys for embeddings must not change the explicitly selected LLM (upstream detects by priority).
  const selected = config.llm?.provider;
  const other = config.embedding?.provider;
  const priorities = ["openai", "minimax", "anthropic", "gemini", "openrouter"];
  if (
    other &&
    priorities.includes(other) &&
    other !== "openai" &&
    other !== selected &&
    (!selected || priorities.indexOf(other) < priorities.indexOf(selected))
  ) {
    return yield* new MemoryConfigurationError({
      message:
        "This embedding provider would override the selected LLM in agentmemory 0.9.29; use local/OpenAI embeddings or the same provider",
    });
  }
  return env;
});
