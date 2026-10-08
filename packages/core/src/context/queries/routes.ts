import { Cause, Context, Effect, Layer, Schema, Scope, Fiber } from "effect";
import { ContextQueryError, ContextQueryInput, type ContextQueryResult } from "../contracts.js";
export { ContextQueryError, ContextQueryInput, ContextQueryResult } from "../contracts.js";

type Query = (input: ContextQueryInput) => Effect.Effect<ContextQueryResult, ContextQueryError>;
export interface ContextQueryDefinition {
  readonly description: string;
  readonly commands: Readonly<
    Record<
      string,
      {
        readonly description: string;
        readonly schema: Schema.ConstraintDecoder<unknown>;
      }
    >
  >;
}
const invalid = (message: string) => new ContextQueryError({ kind: "invalid-input", message });
const unavailable = () =>
  new ContextQueryError({
    kind: "unavailable",
    message: "No active query handler for this Context",
  });
const makeQueries = Effect.sync(() => {
  const routes = new Map<
    string,
    { definition: ContextQueryDefinition; query: Query; scope: Scope.Scope }
  >();
  return {
    register: (path: string, definition: ContextQueryDefinition, query: Query) => {
      return Effect.gen(function* () {
        const route = { definition, query, scope: yield* Scope.Scope };
        return yield* Effect.acquireRelease(
          Effect.gen(function* () {
            if (routes.has(path))
              return yield* new ContextQueryError({
                kind: "unavailable",
                message: "Context query route already registered",
              });
            routes.set(path, route);
          }),
          () =>
            Effect.sync(() => {
              if (routes.get(path) === route) routes.delete(path);
            }),
        );
      }).pipe(Effect.asVoid);
    },
    list: Effect.fn("ContextQueries.list")(function* (parent = "/", offset = 0) {
      if (!parent.startsWith("/") || !Number.isInteger(offset) || offset < 0)
        return yield* invalid("Invalid directory arguments");
      const prefix = parent === "/" ? "/" : `${parent.replace(/\/$/, "")}/`;
      const entries = [...routes]
        .filter(([path]) => path.startsWith(prefix))
        .sort(([a], [b]) => a.localeCompare(b))
        .map(([path, route]) => ({ path, description: route.definition.description }));
      return {
        items: entries.slice(offset, offset + 20),
        total: entries.length,
        nextOffset: offset + 20 < entries.length ? offset + 20 : null,
      };
    }),
    describe: Effect.fn("ContextQueries.describe")(function* (path: string) {
      const route = routes.get(path);
      if (!route) return yield* unavailable();
      return {
        path,
        description: route.definition.description,
        commands: Object.entries(route.definition.commands).map(([command, spec]) => ({
          command,
          description: spec.description,
          arguments: Schema.toJsonSchemaDocument(spec.schema, { onExcessProperty: "error" }).schema,
        })),
      };
    }),
    query: Effect.fn("ContextQueries.query")(function* (raw: ContextQueryInput) {
      const input = yield* Schema.decodeUnknownEffect(ContextQueryInput)(raw).pipe(
        Effect.mapError(() => invalid("Invalid Context query")),
      );
      const route = routes.get(input.path);
      if (!route) return yield* unavailable();
      const commands = route.definition.commands;
      const spec = Object.hasOwn(commands, input.command) ? commands[input.command] : undefined;
      if (!spec)
        return yield* invalid(
          "Unsupported command; use describe_context to inspect supported commands",
        );
      yield* Schema.decodeUnknownEffect(spec.schema, { onExcessProperty: "error" })(
        input.args,
      ).pipe(Effect.mapError(() => invalid("Invalid command arguments")));
      return yield* Effect.acquireUseRelease(
        route.query(input).pipe(Effect.forkIn(route.scope)),
        Effect.fnUntraced(function* (fiber) {
          const exit = yield* Fiber.await(fiber);
          if (exit._tag === "Success") return exit.value;
          // Owner shutdown is a failed remote query; caller cancellation and defects stay intact.
          if (route.scope.state._tag === "Closed" && Cause.hasInterruptsOnly(exit.cause))
            return yield* unavailable();
          return yield* Effect.failCause(exit.cause);
        }),
        Fiber.interrupt,
      );
    }),
  };
});

/** Capability metadata and handlers share the owner's scope; storage is independent. */
export class ContextQueries extends Context.Service<
  ContextQueries,
  Effect.Success<typeof makeQueries>
>()("context/Queries") {
  static readonly layer = Layer.effect(ContextQueries, makeQueries);
}
