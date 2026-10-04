import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import {
  makeMemoryClient,
  makeMemoryReader,
  memoryEnvironment,
  parseMemoryConfig,
} from "../src/agentmemory/index.js";

test("explicit provider selection masks inherited keys and keeps embeddings opt-in", () => {
  const config = parseMemoryConfig(
    {
      description: "Long-term memory",
      config: {
        llm: { provider: "anthropic", model: "test-model", apiKeyEnv: "MEMORY_TEST_KEY" },
      },
    },
    "/tmp",
  );
  const env = memoryEnvironment(config, {
    OPENAI_API_KEY: "unrelated",
    GEMINI_API_KEY: "unrelated",
    MEMORY_TEST_KEY: "test",
  });
  assert.equal(env.ANTHROPIC_API_KEY, "test");
  assert.equal(env.ANTHROPIC_MODEL, "test-model");
  assert.equal(env.OPENAI_API_KEY, "");
  assert.equal(env.GEMINI_API_KEY, "");
  assert.equal(env.EMBEDDING_PROVIDER, "none");
  assert.equal(env.AGENTMEMORY_AUTO_COMPRESS, "true");
  const defaults = memoryEnvironment(parseMemoryConfig(undefined, "/tmp"), {
    OPENAI_API_KEY: "unrelated",
  });
  assert.equal(defaults.OPENAI_API_KEY, "");
  assert.equal(defaults.AGENTMEMORY_AUTO_COMPRESS, "false");
});

test("MiniMax CN uses its configured endpoint and named credential for memory compression", () => {
  const config = parseMemoryConfig(
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
  );
  const env = memoryEnvironment(config, {
    CUSTOM_MINIMAX_KEY: "test-cn-key",
    MINIMAX_API_KEY: "unrelated-global-key",
    OPENAI_API_KEY: "unrelated-key",
  });
  assert.equal(env.MINIMAX_API_KEY, "test-cn-key");
  assert.equal(env.MINIMAX_MODEL, "MiniMax-M3");
  assert.equal(env.MINIMAX_BASE_URL, "https://api.minimax.cn/anthropic");
  assert.equal(env.OPENAI_API_KEY, "");
  assert.equal(env.AGENTMEMORY_AUTO_COMPRESS, "true");
  assert.equal(env.EMBEDDING_PROVIDER, "none");
});

test("independent embedding configuration cannot silently change the LLM provider", () => {
  const config = parseMemoryConfig(
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
  );
  const env = memoryEnvironment(config, { LLM: "test-llm", EMBED: "test-embedding" });
  assert.equal(env.OPENAI_API_KEY_FOR_LLM, "false");
  assert.equal(env.ANTHROPIC_API_KEY, "test-llm");
  assert.equal(env.OPENAI_EMBEDDING_BASE_URL, "http://localhost:8000/v1");
  assert.throws(
    () =>
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
    /override/,
  );
});

test("session ends only after compression; search provenance survives reopening", async () => {
  const dir = await mkdtemp(join(tmpdir(), "memory-client-test-"));
  const calls: string[] = [];
  let polls = 0;
  let observed: unknown;
  let searchRequest: Record<string, unknown> = {};
  let derived = false;
  const fakeFetch: typeof fetch = async (input, init) => {
    const path = new URL(String(input)).pathname.split("/").slice(2).join("/");
    calls.push(path);
    const body = init?.body ? JSON.parse(String(init.body)) : {};
    let result: unknown = {};
    if (path === "observe") {
      observed = body;
      result = { observationId: "obs-1" };
    }
    if (path === "observations")
      result = {
        observations: ++polls === 1 ? [{ id: "obs-1" }] : [{ id: "obs-1", title: "processed" }],
      };
    if (path === "smart-search") {
      searchRequest = body;
      result = body.query
        ? {
            mode: "compact",
            results: [
              {
                obsId: derived ? "mem_1" : "obs-1",
                sessionId: derived ? "memory" : "run-1",
                title: "processed",
              },
            ],
          }
        : {
            mode: "expanded",
            results: derived
              ? []
              : [{ obsId: "obs-1", sessionId: "run-1", observation: { narrative: "remembered" } }],
            truncated: false,
          };
    }
    if (path === "memories/mem_1")
      result = { memory: { content: "consolidated", sourceObservationIds: ["obs-1"] } };
    return Response.json(result);
  };
  const connection = {
    url: "http://localhost:3111",
    secret: "test",
    dataDir: dir,
    project: "test",
    cwd: "/tmp",
  };
  let client = makeMemoryClient(connection, fakeFetch);
  try {
    const record = {
      path: "/lark",
      description: "My work account",
      state: { name: "Alice" },
      messages: [],
    };
    const operation = client.capture({ sessionId: "run-1", records: [record] });
    record.state.name = "Later";
    await operation;
    assert.equal(calls.at(-1), "session/end");
    assert.equal(polls, 2);
    const payload = observed as { hookType: string; data: { tool_output: string } };
    assert.equal(payload.hookType, "post_tool_use");
    assert.equal(JSON.parse(payload.data.tool_output).state.name, "Alice");
    client.close();
    client = makeMemoryClient(connection, fakeFetch);
    const searchStart = calls.length;
    const hits = await client.search("Alice");
    assert.equal(hits.mode, "compact");
    assert.deepEqual(hits.results[0]?.sourcePaths, ["/lark"]);
    assert.equal("observation" in hits.results[0]!, false);
    assert.deepEqual(calls.slice(searchStart), ["smart-search"]);
    assert.deepEqual(searchRequest, {
      query: "Alice",
      limit: 10,
      project: "test",
      includeLessons: false,
    });
    const expanded = await client.expand([{ obsId: "obs-1", sessionId: "run-1" }]);
    assert.equal(expanded.mode, "expanded");
    assert.equal(expanded.truncated, false);
    assert.deepEqual(expanded.results[0]?.sourcePaths, ["/lark"]);
    assert.deepEqual(expanded.results[0]?.observation, { narrative: "remembered" });
    assert.deepEqual(searchRequest, { expandIds: [{ obsId: "obs-1", sessionId: "run-1" }] });
    derived = true;
    const reader = makeMemoryReader(connection, fakeFetch);
    try {
      const before = calls.length;
      const memories = await reader.search("Alice", { limit: 5 });
      assert.equal(memories.mode, "compact");
      assert.deepEqual(calls.slice(before), ["smart-search"]);
      assert.equal(searchRequest.limit, 5);
      assert.equal("observation" in memories.results[0]!, false);
      // Consolidated provenance is resolved only when the Agent selects that memory.
      const details = await reader.expand(["mem_1"]);
      assert.deepEqual(details.results[0]?.sourcePaths, ["/lark"]);
      assert.equal(
        (details.results[0]?.observation as { content: string }).content,
        "consolidated",
      );
    } finally {
      reader.close();
    }
  } finally {
    client.close();
    await rm(dir, { recursive: true, force: true });
  }
});

test("recall expands only selected IDs, omits missing records, and respects the upstream batch cap", async () => {
  const dir = await mkdtemp(join(tmpdir(), "memory-progressive-test-"));
  const calls: Array<{ path: string; body: Record<string, unknown> }> = [];
  const client = makeMemoryClient(
    { url: "http://localhost:3111", secret: "test", dataDir: dir, project: "test", cwd: "/tmp" },
    async (input, init) => {
      const path = new URL(String(input)).pathname.split("/").slice(2).join("/");
      const body = init?.body ? (JSON.parse(String(init.body)) as Record<string, unknown>) : {};
      calls.push({ path, body });
      if (path.startsWith("memories/"))
        return Response.json({ memory: { content: "Consolidated", sourceObservationIds: [] } });
      if (body.query)
        return Response.json({
          mode: "compact",
          results: [
            { obsId: "obs-1", sessionId: "session-1", title: "Relevant" },
            { obsId: "obs-2", sessionId: "session-2", title: "Other" },
          ],
        });
      return Response.json({
        mode: "expanded",
        results: [
          {
            obsId: "obs-1",
            sessionId: "session-1",
            observation: { narrative: "The selected fact" },
          },
        ],
      });
    },
  );
  try {
    await client.search("What is my work email address");
    assert.equal(calls.length, 1);
    const detail = await client.expand(["obs-1", "missing"]);
    assert.deepEqual(calls[1]?.body, { expandIds: [{ obsId: "obs-1" }, { obsId: "missing" }] });
    assert.deepEqual(
      detail.results.map((hit) => hit.obsId),
      ["obs-1"],
    );
    const batch = await client.expand([
      "obs-1",
      ...Array.from({ length: 20 }, (_, index) => `mem_${index}`),
    ]);
    assert.equal((calls[2]?.body.expandIds as unknown[]).length, 20);
    assert.equal(batch.truncated, true);
    assert.equal(
      calls.some(({ path }) => path === "memories/mem_19"),
      false,
    );
    const beforeInvalid = calls.length;
    await assert.rejects(client.search(" "), /nonempty/);
    await assert.rejects(client.search("test", { limit: 0 }), /limit/);
    await assert.rejects(client.search("test", { limit: 1.5 }), /limit/);
    await assert.rejects(client.expand([]), /at least one/);
    await assert.rejects(client.expand([""]), /nonempty/);
    assert.equal(calls.length, beforeInvalid);
  } finally {
    client.close();
    await rm(dir, { recursive: true, force: true });
  }
});

test("failed compression leaves its session open and reports failure", async () => {
  const dir = await mkdtemp(join(tmpdir(), "memory-client-failure-"));
  const calls: string[] = [];
  const client = makeMemoryClient(
    { url: "http://localhost:3111", secret: "test", dataDir: dir, project: "test", cwd: "/tmp" },
    async (input) => {
      const path = new URL(String(input)).pathname;
      calls.push(path);
      return Response.json(
        path.endsWith("observe")
          ? { observationId: "obs-fail" }
          : { observations: [{ id: "obs-fail" }] },
      );
    },
    0,
  );
  try {
    await assert.rejects(
      client.capture({
        sessionId: "failed",
        records: [{ path: "/x", description: "x", state: {}, messages: [] }],
      }),
      /timed out/,
    );
    assert.equal(
      calls.some((p) => p.endsWith("session/end")),
      false,
    );
    await client.drain();
  } finally {
    client.close();
    await rm(dir, { recursive: true, force: true });
  }
});
