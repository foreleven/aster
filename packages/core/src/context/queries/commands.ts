import { DateTime, Effect, Schema } from "effect";
import { publicJson } from "../../json.js";
import type { ContextSnapshot } from "../model.js";
import { ContextRegistry } from "../registry.js";
import { ContextCommand } from "./protocol.js";
import { ContextQueryError } from "./routes.js";
const readArgs = Schema.Struct({ path: Schema.NonEmptyString });
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
export const makeCollectionQueries = Effect.fn("Context.makeCollectionQueries")(function* (
  root: string,
  select: (
    record: ContextSnapshot,
    detail: boolean,
  ) => Effect.Effect<Schema.Json, ContextQueryError>,
) {
  const registry = yield* ContextRegistry;
  const isChild = (path: string) =>
    path.startsWith(`${root}/`) &&
    path.length > root.length + 1 &&
    !path.slice(root.length + 1).includes("/");
  return Effect.fn("Context.queryCollection")(function* (command: List | Read) {
    const data = yield* Effect.gen(function* () {
      if (command._tag === "read") {
        const { path } = command;
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
      const args = command;
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
      path: root,
      command: command._tag,
      queriedAt: DateTime.formatIso(yield* DateTime.now),
      data,
    };
  });
});

export class List extends ContextCommand.Class<List>()("list", {
  description:
    "List retained items, optionally filtering by text. Excludes conversations and execution transcripts.",
  payload: ContextListArgs.fields,
}) {}
export class Read extends ContextCommand.Class<Read>()("read", {
  description: "Read one item's business details by its full path.",
  payload: readArgs.fields,
}) {}
export const CollectionCommands = [List, Read] as const;
