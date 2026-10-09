import {
  ContextInput,
  ContextSnapshot,
  ContextPath,
  type ContextEntry,
  type ContextChange,
} from "./model.js";
import type { PublicContext } from "./contracts.js";
import { restrictedContext, type ContextViewPolicy, type ContextDefinition } from "./definition.js";

import { Context, Effect, Layer, Schema, Stream, type Scope } from "effect";

import { ContextCommitError, ContextConflict, ContextValidationError } from "./errors.js";

import { DurableContext, type ContextCommitOptions } from "./store.js";
import type { ContextSessionStorage } from "./session-storage.js";
import { ContextRecoveryError } from "./errors.js";
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

// Domain owners enter through this schema and identity boundary. The selected
// DurableContext owns canonical storage and ordered commit notifications.
export class ContextRegistry extends Context.Service<
  ContextRegistry,
  {
    readonly acquireSession: (
      path: string,
      storage: ContextSessionStorage,
    ) => Effect.Effect<void, ContextRecoveryError, Scope.Scope>;
    readonly reader: ContextReader;
    readonly views: {
      readonly project: (record: PublicContext) => PublicContext;
      readonly register: (views: readonly ContextViewPolicy[]) => Effect.Effect<void>;
    };
    readonly register: (path: string, definition: ContextDefinition) => Effect.Effect<void>;
    readonly commit: (
      record: ContextInput,
      options: ContextCommitOptions,
    ) => Effect.Effect<
      ContextSnapshot,
      ContextConflict | ContextValidationError | ContextCommitError
    >;
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
  const definitions = new Map<string, ContextDefinition>();
  const views = new Set<ContextViewPolicy>();
  const sessionOwners = new Set<string>();
  const project = (record: PublicContext): PublicContext => {
    if (record.projection?.visibility === "restricted")
      return restrictedContext(record, record.projection.reason ?? "missing-policy");
    const view =
      definitions.get(record.path)?.view ?? [...views].find((view) => view.matches?.(record.path));
    if (!view) return restrictedContext(record, "missing-policy");
    return structuredClone(
      view.project(structuredClone(record)) ?? restrictedContext(record, "invalid-data"),
    );
  };
  const commit = Effect.fn("ContextRegistry.commit")(function* (
    input: ContextInput,
    options: ContextCommitOptions,
  ) {
    const definition = definitions.get(input.path);
    if (!definition) return yield* Effect.die(new Error(`Unregistered Context: ${input.path}`));
    const validated = yield* Effect.try({
      try: () =>
        definition.validate({
          path: input.path,
          description: input.description,
          state: input.state,
          messages: input.messages,
        }),
      catch: (cause) => new ContextValidationError({ path: input.path, cause }),
    }).pipe(
      Effect.catchTag("ContextValidationError", (error) =>
        Schema.isSchemaError(error.cause) ? Effect.fail(error) : Effect.die(error.cause),
      ),
    );
    const event =
      definition.changes === "durable-state" && options.mode !== "bootstrap"
        ? project({ ...validated, revision: options.expectedRevision + 1 })
        : undefined;
    return yield* backend.commit(validated, {
      expectedRevision: options.expectedRevision,
      mode: options.mode,
      ...(event ? { event } : {}),
    });
  });
  const publicSnapshot = () =>
    Object.fromEntries(
      Object.entries(backend.snapshot()).map(([path, record]) => [path, project(record)]),
    );
  return {
    acquireSession: (path, storage) =>
      Effect.acquireRelease(
        Effect.gen(function* () {
          if (sessionOwners.has(path))
            return yield* new ContextRecoveryError({
              path,
              cause: new Error("Context Session already has an owner"),
            });
          sessionOwners.add(path);
          yield* backend.configureSession(path, storage).pipe(
            Effect.onExit((exit) =>
              exit._tag === "Failure"
                ? Effect.sync(() => {
                    sessionOwners.delete(path);
                  })
                : Effect.void,
            ),
          );
        }),
        () =>
          Effect.sync(() => {
            sessionOwners.delete(path);
          }),
      ),
    reader: {
      get: (path) => {
        const record = backend.get(path);
        return record ? project(record) : undefined;
      },
      snapshot: publicSnapshot,
      directory: backend.directory,
      subscribe: backend.subscribe.pipe(
        Effect.map((stream) =>
          stream.pipe(Stream.map(({ record }) => ({ record: project(record) }))),
        ),
      ),
    },
    views: {
      project,
      register: (policies) =>
        Effect.sync(() => {
          for (const view of policies) views.add(view);
        }),
    },
    register: (path, definition) =>
      Effect.gen(function* () {
        if (!Schema.is(ContextPath)(path))
          return yield* Effect.die(new Error(`Invalid Context path: ${path}`));
        const previous = definitions.get(path);
        if (previous && previous !== definition)
          return yield* Effect.die(new Error(`Context implementation cannot change: ${path}`));
        yield* backend.recover(path, definition.validate).pipe(Effect.orDie);
        definitions.set(path, definition);
      }),
    commit,
    get: backend.get,
    snapshot: backend.snapshot,
    changes: backend.changes,
    subscribe: backend.subscribe,
  };
};
