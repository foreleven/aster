import { isDeepStrictEqual } from "node:util";
import { Effect, Schema, Stream } from "effect";
import { DurableContext, DurableContextSnapshot } from "./durable.js";
import { ContextRecoveryError } from "./errors.js";
import type { ContextRecord } from "./model.js";

export const ContextRoute = Schema.Struct({
  prefix: DurableContextSnapshot.fields.path,
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

export const normalizeContextRevision = (record: ContextRecord): ContextRecord => ({
  ...record,
  revision: record.revision ?? 0,
});

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
  const snapshots = entries.map(([name, backend]) => ({ name, snapshot: backend!.snapshot() }));
  const paths = new Set(snapshots.flatMap(({ snapshot }) => Object.keys(snapshot)));
  for (const path of paths) {
    const authoritative = selected(path).get(path);
    if (!authoritative)
      return yield* new ContextRecoveryError({
        path,
        cause:
          "Selected backend is missing a Context present in another store; migrate before activation",
      });
    for (const { name, snapshot } of snapshots) {
      const shadow = snapshot[path];
      if (name === owner(path) || !shadow) continue;
      const active = normalizeContextRevision(authoritative);
      const other = normalizeContextRevision(shadow);
      if (
        other.revision! > active.revision! ||
        (other.revision === active.revision && !isDeepStrictEqual(other, active))
      )
        return yield* new ContextRecoveryError({
          path,
          cause:
            "Selected backend is stale or diverges at the same revision; migration requires reconciliation",
        });
    }
  }
  return DurableContext.of({
    kind: "routed",
    commit: (record, options) => selected(record.path).commit(record, options),
    recover: (path, validate) => selected(path).recover(path, validate),
    get: (path) => selected(path).get(path),
    snapshot: () =>
      Object.fromEntries(
        entries.flatMap(([name, backend]) =>
          Object.entries(backend!.snapshot()).filter(([path]) => owner(path) === name),
        ),
      ),
    changes: Stream.mergeAll(
      entries.map(([name, backend]) =>
        backend!.changes.pipe(Stream.filter((change) => owner(change.path) === name)),
      ),
      { concurrency: "unbounded" },
    ),
    subscribe: Effect.forEach(entries, ([name, backend]) =>
      backend!.subscribe.pipe(
        Effect.map((stream) => stream.pipe(Stream.filter((change) => owner(change.path) === name))),
      ),
    ).pipe(Effect.map((streams) => Stream.mergeAll(streams, { concurrency: "unbounded" }))),
  });
});

export const RoutedDurableContext = { make };
