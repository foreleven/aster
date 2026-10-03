import { Context, Effect, Schema, type Scope, type Stream } from "effect";
import { reactionEventsCheck } from "./reaction-event.js";
import type { PublicContext } from "@aster/api-contracts";
import { ContextRecord, type ContextChange } from "./model.js";
import type {
  ContextCommitError,
  ContextConflict,
  ContextRecoveryError,
  ContextValidationError,
} from "./errors.js";

/** Shared identity validation for every authoritative backend. */
export const DurableContextSnapshot = Schema.Struct({
  ...ContextRecord.fields,
  path: Schema.String.check(
    Schema.isPattern(/^\/[^/\\\0]+(?:\/[^/\\\0]+)*$/),
    Schema.isPattern(/^(?!.*(?:^|\/)\.\.?(?:\/|$))/),
  ),
}).check(reactionEventsCheck);

export interface ContextCommitOptions {
  readonly expectedRevision: number;
  readonly evaluate?: boolean;
  /** A registry-owned public source snapshot, atomically retained on state changes. */
  readonly reaction?: PublicContext;
}

/** Canonical storage boundary. A commit contains the complete state, ordered
 * messages, and the owner's receipt/outbox state; these are never separate writes.
 * Agent execution is an optional consumer, not a requirement of this service. */
export class DurableContext extends Context.Service<
  DurableContext,
  {
    readonly kind: "local" | "pi" | "routed";
    readonly commit: (
      record: ContextRecord,
      options: ContextCommitOptions,
    ) => Effect.Effect<
      ContextRecord,
      ContextConflict | ContextValidationError | ContextCommitError
    >;
    /** Reconcile an uncertain commit before allowing the same owner to write again. */
    readonly recover: (
      path: string,
      validate: (record: ContextRecord) => ContextRecord,
    ) => Effect.Effect<void, ContextRecoveryError>;
    /** Detached views of the last known committed snapshot; no file handles escape. */
    readonly get: (path: string) => ContextRecord | undefined;
    readonly snapshot: () => Readonly<Record<string, ContextRecord>>;
    readonly changes: Stream.Stream<ContextChange>;
    readonly subscribe: Effect.Effect<Stream.Stream<ContextChange>, never, Scope.Scope>;
  }
>()("context/DurableContext") {}
