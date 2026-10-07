import { isDeepStrictEqual } from "node:util";
import { Effect, Schema, Stream } from "effect";
import { DurableContext, ContextPath } from "@aster/core";
import { ContextRecoveryError } from "@aster/core";

export const ContextRoute = Schema.Struct({
  prefix: ContextPath,
  backend: Schema.Literals(["local", "pi"]),
});
export type ContextRoute = typeof ContextRoute.Type;

/** Longest segment prefix wins; /goals never matches /goals-archive. */
export const contextBackendFor = (
  path: string,
  routes: readonly ContextRoute[],
): "local" | "pi" => {
  let selected: ContextRoute | undefined;
  for (const route of routes) {
    if (path !== route.prefix && !path.startsWith(`${route.prefix}/`)) continue;
    if (!selected || route.prefix.length > selected.prefix.length) selected = route;
  }
  return selected?.backend ?? "local";
};

/** The unselected store is validation evidence only. Missing/stale selected data
 * must be migrated offline, never silently copied by a running domain owner. */
const make = Effect.fn("RoutedDurableContext.make")(function* (
  backends: { readonly local: DurableContext["Service"]; readonly pi?: DurableContext["Service"] },
  input: readonly ContextRoute[],
) {
  const routes = yield* Schema.decodeUnknownEffect(Schema.Array(ContextRoute))(input).pipe(
    Effect.mapError((cause) => new ContextRecoveryError({ path: "/", cause })),
  );
  if (new Set(routes.map((route) => route.prefix)).size !== routes.length)
    return yield* new ContextRecoveryError({ path: "/", cause: "Duplicate Context route prefix" });
  if (!backends.pi && routes.some((route) => route.backend === "pi"))
    return yield* new ContextRecoveryError({
      path: "/",
      cause: "Pi route has no configured backend",
    });
  const owner = (path: string) => contextBackendFor(path, routes);
  const selected = (path: string) => backends[owner(path)]!;
  const entries = Object.entries(backends).filter((entry) => entry[1] !== undefined);
  const snapshots = entries.map(([name, backend]) => ({
    name,
    snapshot: Object.fromEntries(
      backend!.exportRecords().map((record) => [record.snapshot.path, record]),
    ),
  }));
  const paths = new Set(snapshots.flatMap(({ snapshot }) => Object.keys(snapshot)));
  for (const path of paths) {
    const authoritative = snapshots.find(({ name }) => name === owner(path))?.snapshot[path];
    if (!authoritative)
      return yield* new ContextRecoveryError({
        path,
        cause:
          "Selected backend is missing a Context present in another store; migrate before activation",
      });
    for (const { name, snapshot } of snapshots) {
      const shadow = snapshot[path];
      if (name === owner(path) || !shadow) continue;
      if (
        shadow.snapshot.revision > authoritative.snapshot.revision ||
        (shadow.snapshot.revision === authoritative.snapshot.revision &&
          !isDeepStrictEqual(shadow, authoritative))
      )
        return yield* new ContextRecoveryError({
          path,
          cause:
            "Selected backend is stale or diverges at the same revision; migration requires reconciliation",
        });
    }
  }
  return DurableContext.of({
    journal: () =>
      entries.flatMap(([name, backend]) =>
        backend!.journal().filter((event) => owner(event.record.path) === name),
      ),
    exportRecords: () =>
      entries.flatMap(([name, backend]) =>
        backend!.exportRecords().filter((record) => owner(record.snapshot.path) === name),
      ),
    commit: (record, options) => selected(record.path).commit(record, options),
    recover: (path, validate) => selected(path).recover(path, validate),
    get: (path) => selected(path).get(path),
    directory: () =>
      entries.flatMap(([name, backend]) =>
        backend!.directory().filter((entry) => owner(entry.path) === name),
      ),
    snapshot: () =>
      Object.fromEntries(
        entries.flatMap(([name, backend]) =>
          Object.entries(backend!.snapshot()).filter(([path]) => owner(path) === name),
        ),
      ),
    changes: Stream.mergeAll(
      entries.map(([name, backend]) =>
        backend!.changes.pipe(Stream.filter((change) => owner(change.record.path) === name)),
      ),
      { concurrency: "unbounded" },
    ),
    subscribe: Effect.forEach(entries, ([name, backend]) =>
      backend!.subscribe.pipe(
        Effect.map((stream) =>
          stream.pipe(Stream.filter((change) => owner(change.record.path) === name)),
        ),
      ),
    ).pipe(Effect.map((streams) => Stream.mergeAll(streams, { concurrency: "unbounded" }))),
  });
});

export const RoutedDurableContext = { make };
