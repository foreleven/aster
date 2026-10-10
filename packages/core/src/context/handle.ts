import type { Effect, Option, Schema, Stream } from "effect";
import type { ContextPartition } from "./store.js";
import type {
  ContextCommitError,
  ContextConflict,
  ContextRecoveryError,
  ContextValidationError,
  ContextClosed,
  ContextMessageConflict,
} from "./errors.js";

/** A dated owner is pinned to its partition; storage never rotates it implicitly. */
export type ContextLayout = ContextPartition;

export interface ContextOpenOptions<S extends object, M> {
  readonly path: string;
  readonly state: Schema.ConstraintDecoder<S>;
  readonly message: Schema.ConstraintDecoder<M>;
  readonly keyOf?: (message: M) => string;
  readonly serialize: (state: Readonly<S>) => string;
  readonly events?: "none" | "state";
  readonly initial?: {
    readonly state: S;
    readonly messages?: readonly M[];
    readonly description: string;
  };
  readonly persistence?: ContextLayout;
}
export interface ContextOwnerSnapshot<S, M> {
  readonly path: string;
  readonly state: S;
  readonly messages: readonly M[];
  readonly revision: number;
  readonly description: string;
}
export interface ContextMutation<S, M> {
  readonly state?: S;
  readonly messages?: { readonly append?: readonly M[]; readonly remove?: readonly string[] };
}
export interface ContextCommitOptions {
  readonly expectedRevision?: number;
  readonly mode?: "update" | "bootstrap";
  readonly description?: string;
}
export type ContextError =
  | ContextConflict
  | ContextMessageConflict
  | ContextValidationError
  | ContextCommitError
  | ContextRecoveryError
  | ContextClosed;
export interface ContextHandle<S extends object, M> {
  readonly path: string;
  readonly current: Effect.Effect<ContextOwnerSnapshot<S, M> | undefined, ContextError>;
  readonly snapshot: Effect.Effect<ContextOwnerSnapshot<S, M>, ContextError>;
  readonly commit: (
    change: (current: ContextOwnerSnapshot<S, M>) => ContextMutation<S, M>,
    options?: ContextCommitOptions,
  ) => Effect.Effect<ContextOwnerSnapshot<S, M>, ContextError>;
  readonly serializeState: Effect.Effect<string, ContextError>;
  readonly state: {
    readonly get: Effect.Effect<S, ContextError>;
    /** Also creates the first state for owners admitted by a domain command. */
    readonly set: (
      state: S,
      options?: ContextCommitOptions,
    ) => Effect.Effect<ContextOwnerSnapshot<S, M>, ContextError>;
    readonly update: (
      change: (state: S) => S,
      options?: ContextCommitOptions,
    ) => Effect.Effect<ContextOwnerSnapshot<S, M>, ContextError>;
    readonly watch: Stream.Stream<S, ContextError>;
  };
  readonly messages: {
    readonly get: (key: string) => Effect.Effect<Option.Option<M>, ContextError>;
    readonly list: Effect.Effect<readonly M[], ContextError>;
    readonly append: (
      messages: readonly M[],
      options?: ContextCommitOptions,
    ) => Effect.Effect<ContextOwnerSnapshot<S, M>, ContextError>;
    readonly remove: (
      keys: readonly string[],
      options?: ContextCommitOptions,
    ) => Effect.Effect<ContextOwnerSnapshot<S, M>, ContextError>;
    readonly watch: (key: string) => Stream.Stream<Option.Option<M>, ContextError>;
  };
}
