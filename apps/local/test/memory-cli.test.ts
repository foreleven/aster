import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { makeMemoryClient } from "@aster/integrations";

test("CLI returns compact candidates, then expands only the requested memory", async () => {
  const dir = await mkdtemp(join(tmpdir(), "signals-memory-cli-"));
  const requests: Array<Record<string, unknown>> = [];
  const server = createServer(async (request, response) => {
    const chunks: Buffer[] = [];
    for await (const chunk of request) chunks.push(Buffer.from(chunk));
    const body = JSON.parse(Buffer.concat(chunks).toString()) as Record<string, unknown>;
    requests.push(body);
    response.setHeader("Content-Type", "application/json");
    response.end(
      JSON.stringify(
        body.query
          ? {
              mode: "compact",
              results: [
                {
                  obsId: "obs-1",
                  sessionId: "run-1",
                  title: "Account",
                  type: "discovery",
                  score: 0.8,
                  timestamp: "2026-09-28T00:00:00Z",
                },
                {
                  obsId: "obs-2",
                  sessionId: "run-2",
                  title: "Work mailbox",
                  type: "discovery",
                  score: 0.7,
                  timestamp: "2026-09-28T00:00:00Z",
                },
              ],
            }
          : {
              mode: "expanded",
              truncated: false,
              results: [
                {
                  obsId: "obs-2",
                  sessionId: "run-2",
                  observation: { narrative: "Work email is alice@example.com" },
                },
              ],
            },
      ),
    );
  });
  try {
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    const address = server.address();
    if (!address || typeof address === "string") throw new Error("Test server has no port");
    const connection = {
      url: `http://127.0.0.1:${address.port}`,
      secret: "test",
      dataDir: dir,
      project: "test",
      cwd: dir,
    };
    makeMemoryClient(connection).close();
    const connectionFile = join(dir, "connection.json");
    await writeFile(connectionFile, JSON.stringify(connection));
    const run = (args: string[]) =>
      promisify(execFile)(
        process.execPath,
        [fileURLToPath(new URL("../src/cli.js", import.meta.url)), "memory", ...args],
        {
          env: {
            ...process.env,
            SIGNALS_MEMORY_CONNECTION: connectionFile,
            SIGNALS_AGENT_NETWORK_PROXY: "false",
          },
        },
      );
    const compact = JSON.parse(
      (await run(["search", "What is my work email address", "--limit", "2"])).stdout,
    );
    assert.equal(compact.mode, "compact");
    assert.equal(compact.results.length, 2);
    assert.equal("observation" in compact.results[1], false);
    assert.deepEqual(requests, [
      { query: "What is my work email address", limit: 2, project: "test", includeLessons: false },
    ]);
    const expanded = JSON.parse((await run(["expand", "obs-2", "--session", "run-2"])).stdout);
    assert.equal(expanded.mode, "expanded");
    assert.equal(expanded.results[0].observation.narrative, "Work email is alice@example.com");
    assert.deepEqual(requests[1], { expandIds: [{ obsId: "obs-2", sessionId: "run-2" }] });
    await run(["expand", "obs-1", "obs-2"]);
    assert.deepEqual(requests[2], { expandIds: [{ obsId: "obs-1" }, { obsId: "obs-2" }] });
    await assert.rejects(
      run(["search", "email", "--limit", "0"]),
      (error: unknown) =>
        typeof error === "object" &&
        error !== null &&
        "stderr" in error &&
        String(error.stderr).includes("limit must be an integer"),
    );
    await assert.rejects(
      run(["expand", "obs-2", "--limit", "2"]),
      (error: unknown) =>
        typeof error === "object" &&
        error !== null &&
        "stderr" in error &&
        String(error.stderr).includes("Usage:"),
    );
    assert.equal(requests.length, 3);
  } finally {
    await new Promise<void>((resolve, reject) =>
      server.close((error) => (error ? reject(error) : resolve())),
    );
    await rm(dir, { recursive: true, force: true });
  }
});
