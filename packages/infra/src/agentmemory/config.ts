import { homedir } from "node:os";
import { resolve } from "node:path";
import { Schema } from "effect";

const Llm = Schema.Struct({
  provider: Schema.Literals(["openai", "anthropic", "minimax", "gemini", "openrouter"]),
  model: Schema.String,
  apiKeyEnv: Schema.String,
  baseUrl: Schema.optional(Schema.String),
});
const Embedding = Schema.Struct({
  provider: Schema.Literals(["local", "openai", "gemini", "voyage", "cohere", "openrouter"]),
  model: Schema.optional(Schema.String),
  apiKeyEnv: Schema.optional(Schema.String),
  baseUrl: Schema.optional(Schema.String),
});
export const MemoryEntry = Schema.Struct({
  description: Schema.optional(Schema.String),
  config: Schema.optional(
    Schema.Struct({
      dataDir: Schema.optional(Schema.String),
      port: Schema.optional(Schema.Number),
      autoCompress: Schema.optional(Schema.Boolean),
      llm: Schema.optional(Llm),
      embedding: Schema.optional(Schema.Union([Schema.Literal(false), Embedding])),
    }),
  ),
});

export interface MemoryConfig {
  readonly description: string;
  readonly dataDir: string;
  readonly port: number;
  readonly autoCompress: boolean;
  readonly llm?: typeof Llm.Type;
  readonly embedding?: typeof Embedding.Type;
}

export const parseMemoryConfig = (entry: unknown, baseDir: string): MemoryConfig => {
  const parsed = Schema.decodeUnknownSync(MemoryEntry)(entry ?? {});
  const config = parsed.config ?? {};
  const description = parsed.description ?? "My long-term memory";
  const { llm } = config;
  const embedding = config.embedding || undefined;
  const port = config.port ?? 3111;
  if (!Number.isInteger(port) || port < 1024 || port > 19512) {
    throw new Error("Memory port must be between 1024 and 19512 (iii engine uses port + 46023)");
  }
  if (!description.trim()) throw new Error("Memory description must be nonempty");
  if (llm && (!llm.model.trim() || !llm.apiKeyEnv.trim()))
    throw new Error("Memory llm model and apiKeyEnv are required");
  if (llm?.baseUrl && !["openai", "anthropic", "minimax"].includes(llm.provider)) {
    throw new Error(`${llm.provider} does not support a custom LLM baseUrl in agentmemory 0.9.29`);
  }
  if (embedding?.provider !== "local" && embedding && !embedding.apiKeyEnv?.trim()) {
    throw new Error("Remote memory embeddings require apiKeyEnv");
  }
  if (embedding?.baseUrl && embedding.provider !== "openai") {
    throw new Error("Only OpenAI embeddings support a custom baseUrl in agentmemory 0.9.29");
  }
  if (embedding?.model && !["openai", "openrouter"].includes(embedding.provider)) {
    throw new Error("agentmemory 0.9.29 has a fixed model for this embedding provider");
  }
  if (config.autoCompress && !llm) throw new Error("autoCompress requires an explicit llm");
  return {
    description,
    dataDir:
      config.dataDir === undefined
        ? resolve(homedir(), ".aster", "memory")
        : config.dataDir.startsWith("~/")
          ? resolve(homedir(), config.dataDir.slice(2))
          : resolve(baseDir, config.dataDir),
    port,
    llm,
    embedding,
    autoCompress: config.autoCompress ?? llm !== undefined,
  };
};

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
export const memoryEnvironment = (
  config: MemoryConfig,
  source: NodeJS.ProcessEnv,
): NodeJS.ProcessEnv => {
  const env: NodeJS.ProcessEnv = { ...source };
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
  const credential = (name: string): string => {
    if (!source[name]?.trim())
      throw new Error(`Memory credential environment variable is missing: ${name}`);
    return source[name]!;
  };
  if (config.llm) {
    const prefix = config.llm.provider.toUpperCase();
    env[`${prefix}_API_KEY`] = credential(config.llm.apiKeyEnv);
    env[`${prefix}_MODEL`] = config.llm.model;
    if (config.llm.baseUrl) env[`${prefix}_BASE_URL`] = config.llm.baseUrl;
  }
  if (config.embedding && config.embedding.provider !== "local") {
    const prefix = config.embedding.provider.toUpperCase();
    const key = credential(config.embedding.apiKeyEnv!);
    // Upstream shares provider credentials between its LLM and embedding adapters.
    if (config.llm?.provider === config.embedding.provider && env[`${prefix}_API_KEY`] !== key) {
      throw new Error(
        "agentmemory requires the same credential for LLM and embeddings of the same provider",
      );
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
    throw new Error(
      "This embedding provider would override the selected LLM in agentmemory 0.9.29; use local/OpenAI embeddings or the same provider",
    );
  }
  return env;
};
