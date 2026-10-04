import { mkdirSync } from "node:fs";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import type { ContextRecord } from "@aster/core";

export interface MemoryConnection {
  readonly url: string;
  readonly secret: string;
  readonly dataDir: string;
  readonly project: string;
  readonly cwd: string;
}
export interface MemoryCapture {
  readonly sessionId: string;
  readonly records: ReadonlyArray<ContextRecord>;
}
export interface MemorySearchOptions {
  readonly signal?: AbortSignal;
  readonly limit?: number;
}
export type MemoryReference = string | { readonly obsId: string; readonly sessionId?: string };
export interface MemorySearchResult {
  readonly mode: "compact";
  readonly results: ReadonlyArray<Record<string, unknown>>;
}
export interface MemoryExpansionResult {
  readonly mode: "expanded";
  readonly results: ReadonlyArray<Record<string, unknown>>;
  readonly truncated: boolean;
}
export interface MemoryClient {
  readonly capture: (input: MemoryCapture) => Promise<void>;
  readonly search: (query: string, options?: MemorySearchOptions) => Promise<MemorySearchResult>;
  readonly expand: (
    references: ReadonlyArray<MemoryReference>,
    signal?: AbortSignal,
  ) => Promise<MemoryExpansionResult>;
  readonly drain: () => Promise<void>;
  readonly close: () => void;
}

/** Durable provenance, independent of what the LLM retains in a compressed narrative. */
const makeClient = (
  connection: MemoryConnection,
  fetcher: typeof fetch = fetch,
  processingTimeoutMs: number,
  readOnly: boolean,
): MemoryClient => {
  if (!readOnly) mkdirSync(connection.dataDir, { recursive: true });
  const db = new DatabaseSync(join(connection.dataDir, "signals-sources.sqlite"), { readOnly });
  if (!readOnly)
    db.exec(`PRAGMA journal_mode = WAL;
    CREATE TABLE IF NOT EXISTS sources (
      session_id TEXT NOT NULL, observation_id TEXT NOT NULL, paths TEXT NOT NULL,
      PRIMARY KEY (session_id, observation_id)
    )`);
  const insert = readOnly
    ? undefined
    : db.prepare("INSERT OR REPLACE INTO sources VALUES (?, ?, ?)");
  const pathsById = db.prepare("SELECT paths FROM sources WHERE observation_id = ?");
  const pathsBySession = db.prepare("SELECT paths FROM sources WHERE session_id = ?");
  const pending = new Set<Promise<void>>();
  const active = new Map<string, Promise<void>>();
  const request = async (
    path: string,
    body?: unknown,
    signal?: AbortSignal,
  ): Promise<Record<string, unknown>> => {
    signal?.throwIfAborted();
    const response = await fetcher(`${connection.url}/agentmemory/${path}`, {
      method: body === undefined ? "GET" : "POST",
      headers: { Authorization: `Bearer ${connection.secret}`, "Content-Type": "application/json" },
      ...(body === undefined ? {} : { body: JSON.stringify(body) }),
      signal: signal
        ? AbortSignal.any([signal, AbortSignal.timeout(15_000)])
        : AbortSignal.timeout(15_000),
    }).catch((error: unknown) => {
      const nested =
        error instanceof Error && error.cause instanceof Error ? error.cause : undefined;
      const code = nested && "code" in nested ? nested.code : undefined;
      const cause = code
        ? String(code)
        : (nested?.message ?? (error instanceof Error ? error.message : "unknown error"));
      throw new Error(`agentmemory ${path.split("?")[0]} connection failed: ${cause}`);
    });
    signal?.throwIfAborted();
    if (!response.ok)
      throw new Error(`agentmemory ${path.split("?")[0]} returned HTTP ${response.status}`);
    const result = (await response.json()) as Record<string, unknown>;
    signal?.throwIfAborted();
    if (result.error || result.success === false)
      throw new Error(`agentmemory ${path.split("?")[0]} failed`);
    return result;
  };
  const list = (value: unknown): Record<string, unknown>[] =>
    Array.isArray(value)
      ? value.filter(
          (item): item is Record<string, unknown> => typeof item === "object" && item !== null,
        )
      : [];
  const withSourcePaths = (hit: Record<string, unknown>) => {
    const detail = hit.observation as Record<string, unknown> | undefined;
    const sourceIds = [
      hit.obsId,
      ...(Array.isArray(detail?.sourceObservationIds) ? detail.sourceObservationIds : []),
    ];
    let provenance = sourceIds.flatMap((id) => (typeof id === "string" ? pathsById.all(id) : []));
    if (provenance.length === 0) {
      const sessions = [
        hit.sessionId,
        ...(Array.isArray(detail?.sessionIds) ? detail.sessionIds : []),
      ];
      provenance = sessions.flatMap((id) => (typeof id === "string" ? pathsBySession.all(id) : []));
    }
    const sourcePaths = [
      ...new Set(provenance.flatMap((row) => JSON.parse(String(row.paths)) as string[])),
    ];
    return { ...hit, sourcePaths };
  };
  const capture = async (input: MemoryCapture) => {
    if (!insert) throw new Error("Memory reader cannot capture observations");
    const { sessionId, records } = structuredClone(input);
    const identity = { sessionId, project: connection.project, cwd: connection.cwd };
    await request("session/start", {
      ...identity,
      title: records.map((r) => r.description).join("; "),
    });
    const ids = new Set<string>();
    for (const record of records) {
      const result = await request("observe", {
        ...identity,
        timestamp: new Date().toISOString(),
        hookType: "post_tool_use",
        data: {
          tool_name: `Context: ${record.description}`,
          tool_input: { path: record.path },
          tool_output: JSON.stringify(record),
        },
      });
      if (typeof result.observationId !== "string")
        throw new Error("agentmemory returned no observationId");
      ids.add(result.observationId);
      insert.run(sessionId, result.observationId, JSON.stringify([record.path]));
    }
    // observe may enqueue LLM compression and return before it finishes. Do not end early.
    const deadline = Date.now() + processingTimeoutMs;
    while (ids.size > 0) {
      const result = await request(`observations?sessionId=${encodeURIComponent(sessionId)}`);
      for (const observation of list(result.observations)) {
        if (
          typeof observation.id === "string" &&
          typeof observation.title === "string" &&
          observation.title
        ) {
          ids.delete(observation.id);
        }
      }
      if (ids.size === 0) break;
      if (Date.now() >= deadline)
        throw new Error(`agentmemory observation processing timed out for ${sessionId}`);
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    await request("session/end", { sessionId });
  };
  return {
    capture: (input) => {
      const existing = active.get(input.sessionId);
      if (existing) return existing;
      const operation = capture(input);
      active.set(input.sessionId, operation);
      pending.add(operation);
      void operation
        .finally(() => {
          pending.delete(operation);
          active.delete(input.sessionId);
        })
        .catch(() => {});
      return operation;
    },
    search: async (query, { limit = 10, signal } = {}) => {
      if (!query.trim()) throw new Error("Memory search query must be nonempty");
      if (!Number.isInteger(limit) || limit < 1 || limit > 100)
        throw new Error("Memory search limit must be an integer from 1 to 100");
      const compact = await request(
        "smart-search",
        {
          query,
          limit,
          project: connection.project,
          includeLessons: false,
        },
        signal,
      );
      return { mode: "compact", results: list(compact.results).map(withSourcePaths) };
    },
    expand: async (references, signal) => {
      if (references.length === 0)
        throw new Error("Memory expansion requires at least one observation ID");
      const ids = references.map((reference) =>
        typeof reference === "string" ? { obsId: reference } : reference,
      );
      if (
        ids.some(
          ({ obsId, sessionId }) => !obsId.trim() || (sessionId !== undefined && !sessionId.trim()),
        )
      ) {
        throw new Error("Memory observation and session IDs must be nonempty");
      }
      // Match upstream's expansion cap, including the consolidated-memory fallback.
      const selected = ids.slice(0, 20);
      const expanded = await request("smart-search", { expandIds: selected }, signal);
      const details = new Map(list(expanded.results).map((row) => [row.obsId, row]));
      // The pinned smart-search expansion only resolves observations, not consolidated memories.
      for (const { obsId, sessionId } of selected) {
        if (obsId.startsWith("mem_") && !details.has(obsId)) {
          const result = await request(`memories/${encodeURIComponent(obsId)}`, undefined, signal);
          details.set(obsId, {
            obsId,
            sessionId: sessionId ?? "memory",
            observation: result.memory,
          });
        }
      }
      return {
        mode: "expanded",
        results: selected.flatMap(({ obsId }) => {
          const hit = details.get(obsId);
          return hit ? [withSourcePaths(hit)] : [];
        }),
        truncated: references.length > selected.length || expanded.truncated === true,
      };
    },
    drain: async () => {
      while (pending.size > 0) await Promise.allSettled([...pending]);
    },
    close: () => db.close(),
  };
};

export const makeMemoryClient = (
  connection: MemoryConnection,
  fetcher: typeof fetch = fetch,
  processingTimeoutMs = 120_000,
): MemoryClient => makeClient(connection, fetcher, processingTimeoutMs, false);

export const makeMemoryReader = (
  connection: MemoryConnection,
  fetcher: typeof fetch = fetch,
): Pick<MemoryClient, "search" | "expand" | "close"> => makeClient(connection, fetcher, 0, true);
