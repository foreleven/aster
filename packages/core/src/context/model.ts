import { createHash } from "node:crypto";
import { PublicContext } from "@aster/api-contracts";
import { Schema } from "effect";

export const ContextPath = Schema.String.check(
  Schema.isPattern(/^\/[^/\\\0]+(?:\/[^/\\\0]+)*$/),
  Schema.isPattern(/^(?!.*(?:^|\/)\.\.?(?:\/|$))/),
);
export const ContextInput = Schema.Struct({
  path: ContextPath,
  description: Schema.String,
  state: Schema.ObjectKeyword,
  messages: Schema.Array(Schema.Unknown),
});
export type ContextInput = typeof ContextInput.Type;

/** Owner snapshots never contain public projection markers or delivery journals. */
export const ContextSnapshot = Schema.Struct({
  ...ContextInput.fields,
  revision: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
});
export type ContextSnapshot = typeof ContextSnapshot.Type;

/** Detached content of one successful commit; durable consumption uses the journal. */
export interface ContextChange {
  readonly record: ContextSnapshot;
}

export const ContextEvent = Schema.Struct({
  id: Schema.NonEmptyString,
  record: Schema.Struct({
    ...PublicContext.fields,
    revision: Schema.Int.check(Schema.isGreaterThan(0)),
  }),
  createdAt: Schema.String,
});
export type ContextEvent = typeof ContextEvent.Type;

/** Preserve the v1 identity namespace even though consumers no longer own event creation. */
export const contextEventId = (path: string, revision: number): string =>
  createHash("sha256")
    .update(JSON.stringify(["context-reaction-v1", path, revision]))
    .digest("hex");
