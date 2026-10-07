import { Context, Effect, Layer, Schema, type Scope } from "effect";
import { ContextQueryError, ContextQueryInput, type ContextQueryResult } from "../contracts.js";
export { ContextQueryError, ContextQueryInput, ContextQueryResult } from "../contracts.js";

type Query = (input: ContextQueryInput) => Effect.Effect<ContextQueryResult, ContextQueryError>;

/** Only active integrations can advertise query routes. Registrations belong to their Actor scopes. */
export class ContextQueries extends Context.Service<
  ContextQueries,
  {
    readonly register: (
      path: string,
      query: Query,
    ) => Effect.Effect<void, ContextQueryError, Scope.Scope>;
    readonly query: Query;
  }
>()("context/Queries") {
  static readonly layer = Layer.effect(
    ContextQueries,
    Effect.sync(() => {
      const routes = new Map<string, Query>();
      return {
        register: (path, query) =>
          Effect.acquireRelease(
            Effect.gen(function* () {
              if (routes.has(path))
                return yield* new ContextQueryError({
                  kind: "unavailable",
                  message: "Context query route already registered",
                });
              routes.set(path, query);
            }),
            () =>
              Effect.sync(() => {
                if (routes.get(path) === query) routes.delete(path);
              }),
          ),
        query: Effect.fn("ContextQueries.query")(function* (raw) {
          const input = yield* Schema.decodeUnknownEffect(ContextQueryInput)(raw).pipe(
            Effect.mapError(
              () =>
                new ContextQueryError({ kind: "invalid-input", message: "Invalid Context query" }),
            ),
          );
          const route = routes.get(input.path);
          if (!route)
            return yield* new ContextQueryError({
              kind: "unavailable",
              message: "No active query handler for this Context",
            });
          return yield* route(input);
        }),
      } satisfies ContextQueries["Service"];
    }),
  );
}
