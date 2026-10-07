import { Effect } from "effect";
import { makeDurableContext, ContextCommitError, ContextRecoveryError } from "@aster/core";
import type { ContextStore } from "./file-context-store.js";

/** Adapt the synchronous file driver to core's lazy, typed persistence boundary. */
const fromStore = (store: ContextStore) =>
  makeDurableContext({
    load: Effect.try({
      try: () => store.loadAll(),
      catch: (cause) => new ContextRecoveryError({ path: "/", cause }),
    }),
    save: (record) =>
      Effect.try({
        try: () => store.save(record),
        catch: (cause) => new ContextCommitError({ path: record.snapshot.path, cause }),
      }),
  });

export const LocalDurableContext = { fromStore };
