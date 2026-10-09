import { Effect, Schema } from "effect";
import { makeDurableContext } from "../context/store.js";
import {
  ContextCommitError,
  ContextRecoveryError,
  ContextValidationError,
} from "../context/errors.js";
import type { ContextInput, StoredContext } from "../context/model.js";
import { makeContextRegistryWithBackend } from "../context/registry.js";
import { coreContextViews } from "../runtime/context-views.js";

/** A synchronous fake store for deterministic tests; production drivers belong to infra. */
export interface ContextStore {
  readonly loadAll: () => readonly StoredContext[];
  readonly save: (record: StoredContext) => void;
}
export const makeContextRegistry = (store?: ContextStore) =>
  makeDurableContext({
    load: Effect.try({
      try: () => store?.loadAll() ?? [],
      catch: (cause) => new ContextRecoveryError({ path: "/", cause }),
    }),
    save: (record) =>
      Effect.try({
        try: () => store?.save(record),
        catch: (cause) => new ContextCommitError({ path: record.snapshot.path, cause }),
      }),
  }).pipe(
    Effect.flatMap((backend) => {
      const registry = makeTestContextRegistryWithBackend(backend);
      return registry.views.register(coreContextViews).pipe(Effect.as({ ...registry, backend }));
    }),
  );
export type TestContextRegistry = Effect.Success<ReturnType<typeof makeContextRegistry>>;

export { taskCapture } from "../tasks/view.js";

/** Seed owner records in tests without creating an Actor or borrowing its live Session. */
export const makeTestContextRegistryWithBackend = (
  backend: import("../context/store.js").DurableContext["Service"],
) => {
  const registry = makeContextRegistryWithBackend(backend);
  const schemas = new Map<
    string,
    {
      readonly state: Schema.ConstraintDecoder<object>;
      readonly message: Schema.ConstraintDecoder<unknown>;
      readonly view?: import("../context/view.js").ContextViewPolicy;
      readonly changes?: "none" | "durable-state";
    }
  >();
  const validate = (record: ContextInput) => {
    const schema = schemas.get(record.path);
    if (!schema) throw new Error(`Unregistered Context: ${record.path}`);
    return {
      ...record,
      state: Schema.decodeUnknownSync(schema.state)(record.state),
      messages: Schema.decodeUnknownSync(Schema.Array(schema.message))(record.messages),
    };
  };
  return {
    ...registry,
    backend,
    register: (path: string, schema: NonNullable<ReturnType<typeof schemas.get>>) =>
      Effect.gen(function* () {
        const previous = schemas.get(path);
        if (previous && previous !== schema)
          return yield* Effect.die(new Error(`Context implementation cannot change: ${path}`));
        schemas.set(path, schema);
        yield* backend.recover(path, validate).pipe(Effect.orDie);
        if (schema.view)
          yield* registry.views.register([
            { ...schema.view, matches: (candidate) => candidate === path },
          ]);
      }),
    commit: (input: ContextInput, options: import("../context/store.js").ContextCommitOptions) =>
      Effect.gen(function* () {
        const validated = yield* Effect.try({
          try: () => validate(input),
          catch: (cause) => new ContextValidationError({ path: input.path, cause }),
        });
        const event =
          schemas.get(input.path)?.changes === "durable-state" && options.mode !== "bootstrap"
            ? registry.views.project({ ...validated, revision: options.expectedRevision + 1 })
            : undefined;
        return yield* backend.commit(validated, { ...options, ...(event ? { event } : {}) });
      }),
  };
};
