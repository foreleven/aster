import { Effect, Layer } from "effect";
import { DurableContext } from "@aster/core";
import { makeDurableContext, type ContextPersistence } from "@aster/core";
import { ContextStore } from "./storage.js";
import { ContextCommitError, ContextRecoveryError } from "@aster/core";

export type LocalContextPersistence = ContextPersistence;

const make = (persistence: LocalContextPersistence) => makeDurableContext(persistence);

/** Native synchronous file I/O remains isolated behind the existing storage driver.
 * Effects stay lazy; both opening/recovery and writes report typed storage failures. */
const fromStore = (store?: ContextStore) =>
  make({
    load: Effect.try({
      try: () => store?.loadAll() ?? [],
      catch: (cause) => new ContextRecoveryError({ path: "/", cause }),
    }),
    save: (record) =>
      Effect.try({
        try: () => store?.save(record),
        catch: (cause) => new ContextCommitError({ path: record.snapshot.path, cause }),
      }),
  });

export const LocalDurableContext = {
  make,
  fromStore,
  layer: Layer.effect(DurableContext, Effect.flatMap(ContextStore, fromStore)),
};
