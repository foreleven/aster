import type { StoredContext } from "@aster/core";
import { Context } from "effect";

export interface ContextStore {
  readonly loadAll: () => ReadonlyArray<StoredContext>;
  readonly save: (record: StoredContext) => void;
}

export const ContextStore = Context.Service<ContextStore>("context/Store");
