import { AgentConversations } from "@aster/agent";
import { ApplicationError, ContextQueryInput } from "@aster/api-contracts";
import { Effect, Schema } from "effect";
import { isDeepStrictEqual } from "node:util";
import type { ContextQueries } from "./queries.js";

const SavedResult = Schema.Struct({ input: ContextQueryInput, text: Schema.String });
const queryError = (error: { message: string; kind?: string }) =>
  new ApplicationError({
    kind:
      error.kind === "not-found" || error.kind === "conflict" || error.kind === "invalid-input"
        ? error.kind
        : "unavailable",
    message: error.message,
  });

export const textPage = (text: string, offset: number, size: number) => ({
  content: text.slice(offset, offset + size),
  totalCharacters: text.length,
  nextOffset: offset + size < text.length ? offset + size : null,
});

/** Pi retains full evidence so native replay can skip a query without losing later pages. */
export const queryResults = (
  messages: AgentConversations["Service"],
  queries: ContextQueries["Service"],
) => {
  const page = Effect.fn("ContextQuery.page")(function* (
    owner: string,
    resultId: number,
    offset: number,
  ) {
    const entry = yield* messages.get(owner, resultId).pipe(Effect.mapError(queryError));
    if (entry.kind !== "tool.query-result")
      return yield* new ApplicationError({
        kind: "not-found",
        message: "Query result not found in this conversation",
      });
    const saved = yield* Schema.decodeUnknownEffect(SavedResult)(entry.data).pipe(
      Effect.mapError(
        () =>
          new ApplicationError({ kind: "invalid-input", message: "Invalid saved query result" }),
      ),
    );
    return { resultId, path: saved.input.path, ...textPage(saved.text, offset, 2000) };
  });
  const query = Effect.fn("ContextQuery.run")(function* (
    owner: string,
    requestId: string,
    input: ContextQueryInput,
  ) {
    const entries = yield* messages.read(owner).pipe(Effect.mapError(queryError));
    const previous = entries.find((entry) => entry.requestId === requestId);
    if (previous) {
      const saved = yield* Schema.decodeUnknownEffect(SavedResult)(previous.data).pipe(
        Effect.mapError(
          () =>
            new ApplicationError({
              kind: "conflict",
              message: "Query identity belongs to another operation",
            }),
        ),
      );
      if (previous.kind !== "tool.query-result" || !isDeepStrictEqual(saved.input, input))
        return yield* new ApplicationError({
          kind: "conflict",
          message: "Query identity belongs to another input",
        });
      return yield* page(owner, previous.id, 0);
    }
    const result = yield* queries.query(input).pipe(Effect.mapError(queryError));
    const text = JSON.stringify(result);
    if (text.length > 1_000_000)
      return yield* new ApplicationError({
        kind: "invalid-input",
        message: "Query result exceeds the 1,000,000 character limit; narrow the query",
      });
    const entry = yield* messages
      .append(owner, requestId, "tool.query-result", { input, text })
      .pipe(Effect.mapError(queryError));
    return { resultId: entry.id, path: input.path, ...textPage(text, 0, 2000) };
  });
  return { query, page };
};
