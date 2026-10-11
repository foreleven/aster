import { Effect, Redacted, Schema } from "effect";
import { HttpClient, HttpClientRequest } from "effect/http";
import { MemoryRecallError, MemoryWriteError, type MemorySaveInput } from "@aster/core";
import { MemoryProtocolError } from "./errors.js";
import {
  Envelope,
  Row,
  SearchResults,
  ExpandedResults,
  ConsolidatedMemory,
  MemoryWriteResponse,
  type MemoryConnection,
  type MemoryReference,
} from "./protocol.js";
import type { Provenance } from "./provenance.js";

export const makeMemoryClient = Effect.fn("Memory.client")(function* (
  connection: MemoryConnection,
  provenance: Provenance,
) {
  const http = yield* HttpClient.HttpClient;
  const request = Effect.fn("Memory.http")(function* (path: string, body?: unknown) {
    return yield* Effect.scoped(
      Effect.gen(function* () {
        let input = HttpClientRequest.make(body === undefined ? "GET" : "POST")(
          `${connection.url}/agentmemory/${path}`,
        ).pipe(
          HttpClientRequest.setHeader(
            "Authorization",
            `Bearer ${Redacted.value(connection.secret)}`,
          ),
        );
        if (body !== undefined) input = input.pipe(HttpClientRequest.bodyJsonUnsafe(body));
        const response = yield* http.execute(input);
        if (response.status < 200 || response.status >= 300)
          return yield* new MemoryProtocolError({
            message: `agentmemory ${path.split("?")[0]} returned HTTP ${response.status}`,
            outcome: "failed",
          });
        const value = yield* response.json.pipe(
          Effect.flatMap(Schema.decodeUnknownEffect(Envelope)),
        );
        if (value.error || value.success === false)
          return yield* new MemoryProtocolError({
            message: `agentmemory ${path.split("?")[0]} failed`,
            outcome: "failed",
          });
        return value;
      }),
    ).pipe(
      Effect.timeout(15000),
      Effect.mapError((cause) =>
        cause instanceof MemoryProtocolError
          ? cause
          : new MemoryProtocolError({
              message: `agentmemory ${path.split("?")[0]} request failed`,
              outcome: "unknown",
              cause: Redacted.make(cause),
            }),
      ),
    );
  });
  const recallError = (cause: unknown) =>
    cause instanceof MemoryRecallError
      ? cause
      : new MemoryRecallError({ message: "Memory retrieval failed", cause: Redacted.make(cause) });
  return {
    request,
    save: (input: MemorySaveInput) =>
      Effect.gen(function* () {
        const response = yield* request("remember", {
          content: input.content,
          type: input.type ?? "fact",
          concepts: input.concepts,
          files: input.files,
          project: connection.project,
        });
        const result = yield* Schema.decodeUnknownEffect(MemoryWriteResponse)(response).pipe(
          Effect.mapError(
            () =>
              new MemoryWriteError({
                message: "Invalid agentmemory remember response",
                // A successful HTTP response with an invalid body may still mean the
                // daemon committed the memory. Do not resubmit it automatically.
                outcome: "unknown",
              }),
          ),
        );
        if (!result.memory?.id)
          return yield* new MemoryWriteError({
            message: "agentmemory remember returned no memory id",
            // The endpoint may have accepted the write before returning an
            // incomplete response, so this outcome cannot be safely retried.
            outcome: "unknown",
          });
        return { memoryId: result.memory.id };
      }).pipe(
        Effect.mapError((cause) =>
          cause instanceof MemoryWriteError
            ? cause
            : new MemoryWriteError({
                message: "Memory write outcome is unknown",
                outcome:
                  cause instanceof MemoryProtocolError ? (cause.outcome ?? "unknown") : "unknown",
                cause: Redacted.make(cause),
              }),
        ),
      ),
    search: (query: string, limit = 10) =>
      Effect.gen(function* () {
        if (!query.trim() || !Number.isInteger(limit) || limit < 1 || limit > 100)
          return yield* new MemoryRecallError({
            message:
              "Memory search query must be nonempty and limit must be an integer from 1 to 100",
          });
        const response = yield* request("smart-search", {
          query,
          limit,
          project: connection.project,
          includeLessons: false,
        });
        const result = yield* Schema.decodeUnknownEffect(SearchResults)(response);
        return {
          mode: "compact" as const,
          results: yield* Effect.forEach(result.results, provenance.attach),
        };
      }).pipe(Effect.mapError(recallError)),
    expand: (references: readonly (string | MemoryReference)[]) =>
      Effect.gen(function* () {
        const ids = references.map((value) =>
          typeof value === "string" ? { obsId: value } : value,
        );
        if (
          !ids.length ||
          ids.some((id) => !id.obsId.trim() || (id.sessionId !== undefined && !id.sessionId.trim()))
        )
          return yield* new MemoryRecallError({
            message: "Memory expansion requires nonempty observation and session IDs",
          });
        const selected = ids.slice(0, 20);
        const result = yield* request("smart-search", { expandIds: selected }).pipe(
          Effect.flatMap(Schema.decodeUnknownEffect(ExpandedResults)),
        );
        const details = new Map(result.results.map((row) => [row.obsId, row]));
        for (const id of selected)
          if (id.obsId.startsWith("mem_") && !details.has(id.obsId)) {
            const response = yield* request(`memories/${encodeURIComponent(id.obsId)}`).pipe(
              Effect.flatMap(Schema.decodeUnknownEffect(ConsolidatedMemory)),
            );
            details.set(id.obsId, {
              obsId: id.obsId,
              sessionId: id.sessionId ?? "memory",
              observation: response.memory,
            });
          }
        const rows: Row[] = selected.flatMap((id) => {
          const row = details.get(id.obsId);
          return row ? [row] : [];
        });
        return {
          mode: "expanded" as const,
          results: yield* Effect.forEach(rows, provenance.attach),
          truncated: ids.length > selected.length || result.truncated === true,
        };
      }).pipe(Effect.mapError(recallError)),
  };
});
export type MemoryClient = Effect.Success<ReturnType<typeof makeMemoryClient>>;
