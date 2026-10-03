import { Effect, Layer } from "effect";
import { DurableContext } from "./durable.js";
import { makeDurableContext, type ContextPersistence } from "./durable-kernel.js";
import { ContextStore } from "./storage.js";
import { ContextCommitError, ContextRecoveryError } from "./errors.js";

export type LocalContextPersistence = ContextPersistence;

const make = (persistence: LocalContextPersistence) => makeDurableContext("local", persistence);

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
        catch: (cause) => new ContextCommitError({ path: record.path, cause }),
      }),
  });

export const LocalDurableContext = {
  make,
  fromStore,
  layer: Layer.effect(DurableContext, Effect.flatMap(ContextStore, fromStore)),
};
