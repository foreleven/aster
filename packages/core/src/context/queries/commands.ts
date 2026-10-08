import { ContextQueries, ContextQueryError } from "./routes.js";
import { ContextRegistry } from "../registry.js";
import type { ContextSnapshot } from "../model.js";
import { publicJson } from "../../json.js";
import { DateTime, Effect, Schema } from "effect";
export const ContextListArgs = Schema.Struct({
  query: Schema.optional(Schema.String),
  offset: Schema.optional(Schema.Int.check(Schema.isGreaterThanOrEqualTo(0))),
  limit: Schema.optional(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 100 }))),
});
export const contextPage = <A>(
  items: readonly A[],
  args: { readonly offset?: number; readonly limit?: number },
) => {
  const offset = args.offset ?? 0;
  const limit = args.limit ?? 20;
  return {
    items: items.slice(offset, offset + limit),
    total: items.length,
    nextOffset: offset + limit < items.length ? offset + limit : null,
  };
};

/** Shared list/read protocol; each domain explicitly selects its public business fields. */
export const registerCollectionQueries = Effect.fn("Context.registerCollectionQueries")(function* (
  root: string,
  select: (
    record: ContextSnapshot,
    detail: boolean,
  ) => Effect.Effect<Schema.Json, ContextQueryError>,
) {
  const registry = yield* ContextRegistry;
  const queries = yield* ContextQueries;
  const readArgs = Schema.Struct({ path: Schema.NonEmptyString });
  const isChild = (path: string) =>
    path.startsWith(`${root}/`) &&
    path.length > root.length + 1 &&
    !path.slice(root.length + 1).includes("/");
  yield* queries.register(
    root,
    {
      description: `Read ${root.slice(1)} and their current business status.`,
      commands: {
        list: {
          description:
            "List retained items, optionally filtering by text. Excludes conversations and execution transcripts.",
          schema: ContextListArgs,
        },
        read: {
          description: "Read one item's business details by its full path.",
          schema: readArgs,
        },
      },
    },
    Effect.fn("Context.queryCollection")(function* (input) {
      const data = yield* Effect.gen(function* () {
        if (input.command === "read") {
          const { path } = yield* Schema.decodeUnknownEffect(readArgs)(input.args).pipe(
            Effect.orDie,
          );
          if (!isChild(path))
            return yield* new ContextQueryError({
              kind: "invalid-input",
              message: "Invalid domain path",
            });
          const record = registry.get(path);
          if (!record)
            return yield* new ContextQueryError({
              kind: "unavailable",
              message: "Context not found",
            });
          return yield* select(record, true);
        }
        const args = yield* Schema.decodeUnknownEffect(ContextListArgs)(input.args).pipe(
          Effect.orDie,
        );
        const paths = registry.reader
          .directory()
          .map((r) => r.path)
          .filter(isChild)
          .sort();
        const items = yield* Effect.forEach(paths, (path) => select(registry.get(path)!, false));
        const query = args.query?.toLowerCase();
        return publicJson(
          contextPage(
            query
              ? items.filter((item) => JSON.stringify(item).toLowerCase().includes(query))
              : items,
            args,
          ),
        );
      });
      return {
        path: input.path,
        command: input.command,
        queriedAt: DateTime.formatIso(yield* DateTime.now),
        data,
      };
    }),
  );
});
