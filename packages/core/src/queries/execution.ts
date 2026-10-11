import { AgentConversations } from "@aster/agent/harness";
import { Context, Effect, Fiber, Layer, Option, Ref, Schema, Scope, Semaphore } from "effect";
import { ContextQueryError, ContextQueryInput } from "./contracts.js";
import { ApplicationError } from "../operations.js";
import { queryResults } from "./evidence.js";
import { ContextQueries } from "./registry.js";

const makeExecution = Effect.gen(function* () {
  const scope = yield* Scope.Scope;
  const queries = yield* ContextQueries;
  const evidence = queryResults(yield* AgentConversations);
  const slots = yield* Semaphore.make(4);
  const pending = yield* Ref.make(new Set<string>());
  const bounded = Effect.fnUntraced(function* <A, E>(work: Effect.Effect<A, E>) {
    const result = yield* work.pipe(slots.withPermitsIfAvailable(1));
    if (Option.isNone(result))
      return yield* new ContextQueryError({
        kind: "busy",
        message: "Context query capacity reached",
      });
    return result.value;
  });
  // Runtime retirement owns all remote reads; the caller owns its wait and interrupts its worker.
  const run = <A, E>(work: Effect.Effect<A, E>) =>
    Effect.acquireUseRelease(work.pipe(Effect.forkIn(scope)), Fiber.join, Fiber.interrupt);
  const query = Effect.fn("QueryExecution.query")(function* (
    owner: string,
    requestId: string,
    raw: ContextQueryInput,
  ) {
    const input = yield* decodeInput(raw);
    const key = JSON.stringify([owner, requestId]);
    return yield* Effect.acquireUseRelease(
      Ref.modify(pending, (current) => [!current.has(key), new Set(current).add(key)]).pipe(
        Effect.flatMap((accepted) =>
          accepted
            ? Effect.void
            : Effect.fail(
                new ContextQueryError({
                  kind: "duplicate",
                  message: "Context query identity already running",
                }),
              ),
        ),
      ),
      () =>
        run(
          Effect.gen(function* () {
            const saved = yield* evidence.find(owner, requestId);
            if (Option.isSome(saved)) {
              if (evidence.matches(saved.value, input)) return evidence.first(saved.value);
              const canonical = yield* queries.normalize(input).pipe(
                Effect.catchTag("ContextQueryError", (error) =>
                  Effect.fail(
                    new ApplicationError({
                      kind: "conflict",
                      message:
                        error.kind === "unavailable"
                          ? "Query owner retired; replay with the exact original or canonical arguments"
                          : "Query identity belongs to another input",
                    }),
                  ),
                ),
              );
              if (!evidence.matches(saved.value, canonical))
                return yield* new ApplicationError({
                  kind: "conflict",
                  message: "Query identity belongs to another input",
                });
              return evidence.first(saved.value);
            }
            return yield* bounded(
              Effect.gen(function* () {
                const result = yield* queries.textWithInput(input);
                return yield* evidence.save(owner, requestId, result.input, result.text, input);
              }),
            );
          }),
        ),
      () =>
        Ref.update(pending, (current) => {
          const next = new Set(current);
          next.delete(key);
          return next;
        }),
    );
  });
  return {
    list: queries.list,
    describe: queries.describe,
    query,
    page: evidence.page,
    /** Public transport queries share remote capacity but do not retain model evidence. */
    json: (input: ContextQueryInput) => run(bounded(queries.json(input))),
  };
});

const decodeInput = (input: ContextQueryInput) =>
  Schema.decodeUnknownEffect(ContextQueryInput)(input).pipe(
    Effect.mapError(
      () => new ContextQueryError({ kind: "invalid-input", message: "Invalid Context query" }),
    ),
  );

export class QueryExecution extends Context.Service<
  QueryExecution,
  Effect.Success<typeof makeExecution>
>()("queries/Execution") {
  static readonly layer = Layer.effect(QueryExecution, makeExecution);
}
