import { Schema, Stream, type Scope } from "effect";
import { ContextRecoveryError } from "./errors.js";

/** A dated handle is pinned to its partition; storage never rotates it implicitly. */
export const ContextSessionLayout = Schema.Union([
  Schema.Struct({ layout: Schema.Literal("single") }),
  Schema.Struct({
    layout: Schema.Literal("daily"),
    date: Schema.String.check(Schema.isPattern(/^\d{4}-\d{2}-\d{2}$/)),
    timeZone: Schema.NonEmptyString,
  }),
]);
export type ContextSessionLayout = typeof ContextSessionLayout.Type;

/** Runtime-only routing instructions; functions never enter persisted metadata. */
export interface ContextSessionStorage {
  readonly partition: ContextSessionLayout;
  readonly messageKey: (message: unknown) => string;
}

/**
 * A storage-backed message observation. The first value is the value captured
 * when the watch was acquired; `null` means that the message was retired.
 * Persistence adapters may implement this with their native document watch.
 */
export type ContextMessageWatch = Stream.Stream<unknown | null, ContextRecoveryError, Scope.Scope>;
