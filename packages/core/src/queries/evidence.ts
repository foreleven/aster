import { AgentConversations } from "@aster/agent/harness";
import { ApplicationError } from "../operations.js";
import { ContextQueryInput } from "./contracts.js";
import { DateTime, Effect, Match, Option, Schema } from "effect";
import { isDeepStrictEqual } from "node:util";

const SavedResult = Schema.Struct({
  input: ContextQueryInput,
  originalInput: ContextQueryInput,
  text: Schema.String,
});
const ErrorFields = Schema.Struct({ message: Schema.String, kind: Schema.optional(Schema.String) });
export const queryError = (error: unknown) =>
  Match.value(error).pipe(
    Match.when(
      Schema.is(ErrorFields),
      ({ message, kind }) =>
        new ApplicationError({
          kind: Match.value(kind).pipe(
            Match.when("duplicate", () => "conflict" as const),
            Match.whenOr("not-found", "conflict", "invalid-input", (value) => value),
            Match.orElse(() => "unavailable" as const),
          ),
          message,
        }),
    ),
    Match.orElse(
      () => new ApplicationError({ kind: "unavailable", message: "Context query failed" }),
    ),
  );

export const textPage = (text: string, offset: number, size: number) => ({
  content: text.slice(offset, offset + size),
  totalCharacters: text.length,
  nextOffset: offset + size < text.length ? offset + size : null,
});

/** Pi retains full evidence so native replay can skip a query without losing later pages. */
export const queryResults = (messages: AgentConversations["Service"]) => {
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
    if (!Number.isInteger(offset) || offset < 0 || offset > saved.text.length)
      return yield* new ApplicationError({
        kind: "invalid-input",
        message: "Invalid query result offset",
      });
    return { resultId, path: saved.input.path, ...textPage(saved.text, offset, 2000) };
  });
  const find = Effect.fn("ContextQuery.find")(function* (owner: string, requestId: string) {
    const found = yield* messages.find(owner, requestId).pipe(Effect.mapError(queryError));
    if (Option.isSome(found)) {
      const previous = found.value;
      const saved = yield* Schema.decodeUnknownEffect(SavedResult)(previous.data).pipe(
        Effect.mapError(
          () =>
            new ApplicationError({
              kind: "conflict",
              message: "Query identity belongs to another operation",
            }),
        ),
      );
      if (previous.kind !== "tool.query-result")
        return yield* new ApplicationError({
          kind: "conflict",
          message: "Query identity belongs to another operation",
        });
      return Option.some({ resultId: previous.id, ...saved });
    }
    return Option.none();
  });
  const save = Effect.fn("ContextQuery.save")(function* (
    owner: string,
    requestId: string,
    input: ContextQueryInput,
    text: string,
    originalInput: ContextQueryInput,
  ) {
    if (text.length > 1_000_000)
      return yield* new ApplicationError({
        kind: "invalid-input",
        message: "Query result exceeds the 1,000,000 character limit; narrow the query",
      });
    const entry = yield* messages
      .append(owner, requestId, "tool.query-result", {
        input,
        originalInput,
        text,
        observedAt: DateTime.formatIso(yield* DateTime.now),
      })
      .pipe(Effect.mapError(queryError));
    return { resultId: entry.id, path: input.path, ...textPage(text, 0, 2000) };
  });
  return {
    find,
    save,
    page,
    matches: (saved: typeof SavedResult.Type, input: ContextQueryInput) =>
      isDeepStrictEqual(saved.input, input) || isDeepStrictEqual(saved.originalInput, input),
    first: (saved: typeof SavedResult.Type & { resultId: number }) => ({
      resultId: saved.resultId,
      path: saved.input.path,
      ...textPage(saved.text, 0, 2000),
    }),
  };
};
