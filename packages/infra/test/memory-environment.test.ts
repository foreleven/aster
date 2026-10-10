import { Effect } from "effect";
import { testMemorySettings } from "./configuration-fixtures.js";
import assert from "node:assert/strict";
import { test } from "node:test";
import { memoryEnvironment } from "../src/memory/environment.js";

test("explicit provider selection masks inherited keys and keeps embeddings opt-in", () => {
  const config = Effect.runSync(
    testMemorySettings(
      {
        description: "Long-term memory",
        config: {
          llm: { provider: "anthropic", model: "test-model", apiKeyEnv: "MEMORY_TEST_KEY" },
        },
      },
      "/tmp",
    ),
  );
  const env = Effect.runSync(
    memoryEnvironment(config, {
      OPENAI_API_KEY: "unrelated",
      GEMINI_API_KEY: "unrelated",
      MEMORY_TEST_KEY: "test",
    }),
  );
  assert.equal(env.ANTHROPIC_API_KEY, "test");
  assert.equal(env.ANTHROPIC_MODEL, "test-model");
  assert.equal(env.OPENAI_API_KEY, "");
  assert.equal(env.GEMINI_API_KEY, "");
  assert.equal(env.EMBEDDING_PROVIDER, "none");
  assert.equal(env.AGENTMEMORY_AUTO_COMPRESS, "true");
  const defaults = Effect.runSync(
    memoryEnvironment(Effect.runSync(testMemorySettings(undefined, "/tmp")), {
      OPENAI_API_KEY: "unrelated",
    }),
  );
  assert.equal(defaults.OPENAI_API_KEY, "");
  assert.equal(defaults.AGENTMEMORY_AUTO_COMPRESS, "false");
});

test("MiniMax CN uses its configured endpoint and named credential for memory compression", () => {
  const config = Effect.runSync(
    testMemorySettings(
      {
        description: "Memory",
        config: {
          llm: {
            provider: "minimax",
            model: "MiniMax-M3",
            baseUrl: "https://api.minimax.cn/anthropic",
            apiKeyEnv: "CUSTOM_MINIMAX_KEY",
          },
          autoCompress: true,
        },
      },
      "/tmp",
    ),
  );
  const env = Effect.runSync(
    memoryEnvironment(config, {
      CUSTOM_MINIMAX_KEY: "test-cn-key",
      MINIMAX_API_KEY: "unrelated-global-key",
      OPENAI_API_KEY: "unrelated-key",
    }),
  );
  assert.equal(env.MINIMAX_API_KEY, "test-cn-key");
  assert.equal(env.MINIMAX_MODEL, "MiniMax-M3");
  assert.equal(env.MINIMAX_BASE_URL, "https://api.minimax.cn/anthropic");
  assert.equal(env.OPENAI_API_KEY, "");
  assert.equal(env.AGENTMEMORY_AUTO_COMPRESS, "true");
  assert.equal(env.EMBEDDING_PROVIDER, "none");
});

test("independent embedding configuration cannot silently change the LLM provider", () => {
  const config = Effect.runSync(
    testMemorySettings(
      {
        description: "memory",
        config: {
          llm: { provider: "anthropic", model: "test", apiKeyEnv: "LLM" },
          embedding: {
            provider: "openai",
            model: "text-embedding-3-small",
            apiKeyEnv: "EMBED",
            baseUrl: "http://localhost:8000/v1",
          },
        },
      },
      "/tmp",
    ),
  );
  const env = Effect.runSync(
    memoryEnvironment(config, { LLM: "test-llm", EMBED: "test-embedding" }),
  );
  assert.equal(env.OPENAI_API_KEY_FOR_LLM, "false");
  assert.equal(env.ANTHROPIC_API_KEY, "test-llm");
  assert.equal(env.OPENAI_EMBEDDING_BASE_URL, "http://localhost:8000/v1");
  assert.throws(
    () =>
      Effect.runSync(
        memoryEnvironment(
          {
            ...config,
            llm: undefined,
            embedding: {
              provider: "gemini",
              apiKeyEnv: "EMBED",
            },
          },
          { EMBED: "test" },
        ),
      ),
    /override/,
  );
});
