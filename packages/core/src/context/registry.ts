import { isDeepStrictEqual } from "node:util";
import { Context, Effect, Layer, PubSub, Stream, type Scope } from "effect";
import { ContextStore } from "./storage.js";
import type { ContextRecord, ContextDefinition, ContextChange } from "./model.js";

// This is the sole writer of public Context snapshots. Actor lifecycle and
// storage adapters use the same validation, commit and notification boundary.
export class ContextRegistry extends Context.Service<
  ContextRegistry,
  {
    readonly register: (path: string, definition: ContextDefinition) => Effect.Effect<void>;
    readonly definition: (path: string) => ContextDefinition | undefined;
    readonly set: (
      record: ContextRecord,
      options?: { readonly evaluate?: boolean },
    ) => Effect.Effect<void>;
    /** Initialize a dynamic description once, without replacing newer content. */
    readonly describe: (path: string, description: string) => Effect.Effect<void>;
    readonly get: (path: string) => ContextRecord | undefined;
    readonly snapshot: () => Readonly<Record<string, ContextRecord>>;
    readonly changes: Stream.Stream<ContextChange>;
    readonly subscribe: Effect.Effect<Stream.Stream<ContextChange>, never, Scope.Scope>;
  }
>()("signals/ContextRegistry") {
  static readonly layer = Layer.effect(
    ContextRegistry,
    Effect.gen(function* () {
      return yield* makeContextRegistry(yield* ContextStore);
    }),
  );
}

export const makeContextRegistry = (store?: ContextStore) =>
  Effect.gen(function* () {
    const definitions = new Map<string, ContextDefinition>();
    const records = new Map<string, ContextRecord>(
      (store?.loadAll() ?? []).map((record) => [record.path, structuredClone(record)]),
    );
    const pubsub = yield* PubSub.unbounded<ContextChange>();
    const set = (input: ContextRecord, options?: { readonly evaluate?: boolean }) =>
      Effect.gen(function* () {
        const definition = definitions.get(input.path);
        if (!definition) return yield* Effect.die(new Error(`Unregistered Context: ${input.path}`));
        const previous = records.get(input.path);
        // Select public fields explicitly; actor handles and other extra fields never cross this boundary.
        const record = structuredClone(
          definition.validate({
            path: input.path,
            description: previous?.description || input.description,
            state: input.state,
            messages: input.messages,
          }),
        );
        if (isDeepStrictEqual(previous, record)) return;
        // The registry owns its snapshots. Storage adapters receive detached data
        // too, so retaining or mutating an adapter argument cannot bypass set().
        store?.save(structuredClone(record));
        records.set(record.path, record);
        yield* PubSub.publish(pubsub, {
          path: record.path,
          created: previous === undefined,
          stateChanged: previous === undefined || !isDeepStrictEqual(previous.state, record.state),
          record: structuredClone(record),
          ...(options?.evaluate === false ? { evaluate: false } : {}),
        });
      });
    return {
      register: (path: string, definition: ContextDefinition) =>
        Effect.gen(function* () {
          if (
            !path.startsWith("/") ||
            path
              .split("/")
              .slice(1)
              .some((s) => !s || s === "." || s === "..")
          ) {
            return yield* Effect.die(new Error(`Invalid Context path: ${path}`));
          }
          const previous = definitions.get(path);
          if (previous && previous !== definition)
            return yield* Effect.die(new Error(`Context implementation cannot change: ${path}`));
          definitions.set(path, definition);
        }),
      definition: (path: string) => definitions.get(path),
      set,
      describe: (path: string, description: string) =>
        Effect.gen(function* () {
          if (!description.trim())
            return yield* Effect.die(new Error("Context description must be nonempty"));
          const current = records.get(path);
          if (current && !current.description) yield* set({ ...current, description });
        }),
      get: (path: string) => {
        const record = records.get(path);
        return record === undefined ? undefined : structuredClone(record);
      },
      snapshot: () =>
        Object.fromEntries([...records].map(([path, record]) => [path, structuredClone(record)])),
      changes: Stream.fromPubSub(pubsub).pipe(Stream.map((change) => structuredClone(change))),
      subscribe: PubSub.subscribe(pubsub).pipe(
        Effect.map((subscription) =>
          Stream.fromEffectRepeat(PubSub.take(subscription)).pipe(
            Stream.map((change) => structuredClone(change)),
          ),
        ),
      ),
    };
  });
