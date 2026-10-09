import { Context, Effect, Layer, Stream, type Scope } from "effect";
import type { ContextSnapshot, ContextEntry, ContextChange } from "./model.js";
import type { PublicContext } from "./contracts.js";
import { restrictedContext, type ContextViewPolicy } from "./view.js";
import { DurableContext } from "./store.js";
export type { ContextCommitOptions } from "./store.js";

export interface ContextReader {
  readonly get: (path: string) => PublicContext | undefined;
  readonly snapshot: () => Readonly<Record<string, PublicContext>>;
  readonly directory: () => readonly ContextEntry[];
  readonly subscribe: Effect.Effect<
    Stream.Stream<{ readonly record: PublicContext }>,
    never,
    Scope.Scope
  >;
}

/** Read index and public projections. Owner writes belong to scoped Context Sessions. */
export class ContextRegistry extends Context.Service<
  ContextRegistry,
  {
    readonly reader: ContextReader;
    readonly views: {
      readonly project: (record: PublicContext) => PublicContext;
      readonly set: (path: string, view: ContextViewPolicy | undefined) => Effect.Effect<void>;
      readonly register: (views: readonly ContextViewPolicy[]) => Effect.Effect<void>;
    };
    readonly get: (path: string) => ContextSnapshot | undefined;
    readonly snapshot: () => Readonly<Record<string, ContextSnapshot>>;
    readonly changes: Stream.Stream<ContextChange>;
    readonly subscribe: Effect.Effect<Stream.Stream<ContextChange>, never, Scope.Scope>;
  }
>()("context/Registry") {
  static readonly layer = Layer.effect(
    ContextRegistry,
    Effect.gen(function* () {
      return makeContextRegistryWithBackend(yield* DurableContext);
    }),
  );
}

export const makeContextRegistryWithBackend = (
  backend: DurableContext["Service"],
): ContextRegistry["Service"] => {
  const views = new Set<ContextViewPolicy>();
  const ownerViews = new Map<string, ContextViewPolicy>();
  const project = (record: PublicContext): PublicContext => {
    if (record.projection?.visibility === "restricted")
      return restrictedContext(record, record.projection.reason ?? "missing-policy");
    const view =
      ownerViews.get(record.path) ?? [...views].findLast((view) => view.matches?.(record.path));
    if (!view) return restrictedContext(record, "missing-policy");
    return structuredClone(
      view.project(structuredClone(record)) ?? restrictedContext(record, "invalid-data"),
    );
  };
  return {
    reader: {
      get: (path) => {
        const record = backend.get(path);
        return record ? project(record) : undefined;
      },
      snapshot: () =>
        Object.fromEntries(
          Object.entries(backend.snapshot()).map(([path, record]) => [path, project(record)]),
        ),
      directory: backend.directory,
      subscribe: backend.subscribe.pipe(
        Effect.map((stream) =>
          stream.pipe(Stream.map(({ record }) => ({ record: project(record) }))),
        ),
      ),
    },
    views: {
      project,
      set: (path, view) =>
        Effect.sync(() => {
          if (view) ownerViews.set(path, view);
          else ownerViews.delete(path);
        }),
      register: (policies) =>
        Effect.sync(() => {
          for (const view of policies) views.add(view);
        }),
    },
    get: backend.get,
    snapshot: backend.snapshot,
    changes: backend.changes,
    subscribe: backend.subscribe,
  };
};
