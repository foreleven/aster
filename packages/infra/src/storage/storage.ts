import type { ContextRecord } from "@aster/core";
import { Context } from "effect";

export interface ContextStore {
  readonly loadAll: () => ReadonlyArray<ContextRecord>;
  readonly save: (record: ContextRecord) => void;
}

export const ContextStore = Context.Service<ContextStore>("context/Store");
