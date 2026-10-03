import { coreContextViews } from "./business-view.js";
import { restrictedContext } from "./view.js";
import type { ContextViewPolicy } from "./model.js";
import { Context, Effect, Layer, Schema, type Stream, type Scope } from "effect";
import type { ContextStore } from "./storage.js";
import { ContextRecord, type ContextDefinition, type ContextChange } from "./model.js";
import { ContextCommitError, ContextConflict, ContextValidationError } from "./errors.js";

import { DurableContext, DurableContextSnapshot, type ContextCommitOptions } from "./durable.js";
import { LocalDurableContext } from "./local-durable.js";
export type { ContextCommitOptions } from "./durable.js";

// Domain owners enter through this schema and identity boundary. The selected
// DurableContext owns canonical storage and ordered commit notifications.
export class ContextRegistry extends Context.Service<
  ContextRegistry,
  {
    readonly project: (record: ContextRecord) => ContextRecord;
    readonly publicSnapshot: () => Readonly<Record<string, ContextRecord>>;
    readonly registerViews: (views: readonly ContextViewPolicy[]) => Effect.Effect<void>;
    readonly register: (path: string, definition: ContextDefinition) => Effect.Effect<void>;
    readonly definition: (path: string) => ContextDefinition | undefined;
    readonly commit: (
      record: ContextRecord,
      options: ContextCommitOptions,
    ) => Effect.Effect<
      ContextRecord,
      ContextConflict | ContextValidationError | ContextCommitError
    >;
    /** Initialize a dynamic description once, without replacing newer content. */
    readonly describe: (
      path: string,
      description: string,
      expectedRevision: number,
    ) => Effect.Effect<void, ContextConflict | ContextValidationError | ContextCommitError>;
    readonly get: (path: string) => ContextRecord | undefined;
    readonly snapshot: () => Readonly<Record<string, ContextRecord>>;
    readonly changes: Stream.Stream<ContextChange>;
    readonly subscribe: Effect.Effect<Stream.Stream<ContextChange>, never, Scope.Scope>;
  }
>()("signals/ContextRegistry") {
  static readonly layer = Layer.effect(
    ContextRegistry,
    Effect.gen(function* () {
      return makeContextRegistryWithBackend(yield* DurableContext);
    }),
  );
}

/** Standalone/test compatibility factory; production selects DurableContext through a Layer. */
export const makeContextRegistry = (store?: ContextStore) =>
  LocalDurableContext.fromStore(store).pipe(Effect.map(makeContextRegistryWithBackend));

export const makeContextRegistryWithBackend = (
  backend: DurableContext["Service"],
): ContextRegistry["Service"] => {
  const definitions = new Map<string, ContextDefinition>();
  const views = new Set<ContextViewPolicy>(coreContextViews);
  const project = (record: ContextRecord): ContextRecord => {
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
    input: ContextRecord,
    options: ContextCommitOptions,
  ) {
    const definition = definitions.get(input.path);
    if (!definition) return yield* Effect.die(new Error(`Unregistered Context: ${input.path}`));
    const previous = backend.get(input.path);
    const validated = yield* Effect.try({
      try: () =>
        definition.validate({
          path: input.path,
          description: previous?.description || input.description,
          state: input.state,
          messages: input.messages,
        }),
      catch: (cause) => new ContextValidationError({ path: input.path, cause }),
    }).pipe(
      Effect.catchTag("ContextValidationError", (error) =>
        Schema.isSchemaError(error.cause) ? Effect.fail(error) : Effect.die(error.cause),
      ),
    );
    const reaction =
      definition.signalSource && options.evaluate !== false
        ? project({ ...validated, description: validated.description || definition.identity })
        : undefined;
    return yield* backend.commit(validated, {
      expectedRevision: options.expectedRevision,
      evaluate: options.evaluate,
      ...(reaction ? { reaction } : {}),
    });
  });
  return {
    project,
    publicSnapshot: () =>
      Object.fromEntries(
        Object.entries(backend.snapshot()).map(([path, record]) => [path, project(record)]),
      ),
    registerViews: (policies) =>
      Effect.sync(() => {
        for (const view of policies) views.add(view);
      }),
    register: (path, definition) =>
      Effect.gen(function* () {
        if (!Schema.is(DurableContextSnapshot.fields.path)(path))
          return yield* Effect.die(new Error(`Invalid Context path: ${path}`));
        const previous = definitions.get(path);
        if (previous && previous !== definition)
          return yield* Effect.die(new Error(`Context implementation cannot change: ${path}`));
        yield* backend.recover(path, definition.validate).pipe(Effect.orDie);
        definitions.set(path, definition);
      }),
    definition: (path) => definitions.get(path),
    commit,
    describe: (path, description, expectedRevision) =>
      Effect.gen(function* () {
        if (!description.trim())
          return yield* Effect.die(new Error("Context description must be nonempty"));
        const current = backend.get(path);
        if (current && !current.description)
          yield* commit({ ...current, description }, { expectedRevision });
      }),
    get: backend.get,
    snapshot: backend.snapshot,
    changes: backend.changes,
    subscribe: backend.subscribe,
  };
};
