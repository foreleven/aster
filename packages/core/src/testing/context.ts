import { Effect } from "effect";
import { makeDurableContext } from "../context/store.js";
import { ContextCommitError, ContextRecoveryError } from "../context/errors.js";
import type { StoredContext } from "../context/model.js";
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
      const registry = makeContextRegistryWithBackend(backend);
      return registry.views.register(coreContextViews).pipe(Effect.as({ ...registry, backend }));
    }),
  );
export type TestContextRegistry = Effect.Success<ReturnType<typeof makeContextRegistry>>;

export { taskCapture } from "../tasks/view.js";

export { initializeContextDescription } from "../reasoning/context-description.js";
