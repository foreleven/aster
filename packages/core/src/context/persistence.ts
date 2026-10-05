import { Context, Effect, Schema, type Scope, type Stream } from "effect";
import { ContextRecord } from "./storage-format.js";
import { ContextSnapshot, ContextInput, ContextEvent, type ContextChange } from "./model.js";
import { PublicContext } from "@aster/api-contracts";
import type {
  ContextCommitError,
  ContextConflict,
  ContextRecoveryError,
  ContextValidationError,
} from "./errors.js";

/** Drivers own serialization and atomic recovery; the kernel owns revision/publication semantics. */
export interface ContextPersistence {
  readonly load: Effect.Effect<readonly ContextRecord[], ContextRecoveryError>;
  readonly save: (record: ContextRecord) => Effect.Effect<void, ContextCommitError>;
}

export interface ContextCommitOptions {
  readonly expectedRevision: number;
  readonly mode?: "update" | "bootstrap";
}

export interface DurableCommitOptions extends ContextCommitOptions {
  /** A registry-owned public source snapshot, atomically retained on state changes. */
  readonly event?: PublicContext;
}

/** Canonical storage boundary. A commit contains the complete state, ordered
 * messages, and the owner's receipt/outbox state; these are never separate writes.
 * Agent execution is an optional consumer, not a requirement of this service. */
export class DurableContext extends Context.Service<
  DurableContext,
  {
    readonly commit: (
      record: ContextInput,
      options: DurableCommitOptions,
    ) => Effect.Effect<
      ContextSnapshot,
      ContextConflict | ContextValidationError | ContextCommitError
    >;
    /** Reconcile an uncertain commit before allowing the same owner to write again. */
    readonly recover: (
      path: string,
      validate: (record: ContextSnapshot) => ContextInput,
    ) => Effect.Effect<void, ContextRecoveryError>;
    /** Detached views of the last known committed snapshot; no file handles escape. */
    readonly get: (path: string) => ContextSnapshot | undefined;
    readonly snapshot: () => Readonly<Record<string, ContextSnapshot>>;
    /** Durable evidence is available only to consumers and infrastructure. */
    readonly journal: () => readonly ContextEvent[];
    readonly exportRecords: () => readonly ContextRecord[];
    readonly changes: Stream.Stream<ContextChange>;
    readonly subscribe: Effect.Effect<Stream.Stream<ContextChange>, never, Scope.Scope>;
  }
>()("context/DurableContext") {}

/** Canonical in-memory storage model; the flat v1 format stays at the codec boundary. */
export const StoredContext = Schema.Struct({
  snapshot: ContextSnapshot,
  events: Schema.Array(ContextEvent),
});
export type StoredContext = typeof StoredContext.Type;
export const restoreContext = (record: ContextRecord): StoredContext => ({
  snapshot: Schema.decodeUnknownSync(ContextSnapshot)({
    ...record,
    revision: record.revision ?? 0,
  }),
  events: (record.reactionEvents ?? []).map((event) => ({
    id: event.requestId,
    record: { ...event.record, revision: event.revision },
    createdAt: event.createdAt,
  })),
});
export const contextStorageRecord = (stored: StoredContext): ContextRecord => ({
  ...stored.snapshot,
  ...(stored.events.length
    ? {
        reactionEvents: stored.events.map((event) => ({
          requestId: event.id,
          causationId: event.id,
          source: event.record.path,
          target: "/system-one" as const,
          revision: event.record.revision,
          record: event.record,
          createdAt: event.createdAt,
        })),
      }
    : {}),
});
