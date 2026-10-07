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

/** Committed owner snapshot and new source events; the journal covers missed notifications. */
export interface ContextChange {
  readonly record: ContextSnapshot;
  /** Newly committed durable source events; absent for metadata/private updates. */
  readonly events?: readonly ContextEvent[];
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

/** Stable identity for one committed source revision. */
export const contextEventId = (path: string, revision: number): string =>
  createHash("sha256")
    .update(JSON.stringify(["context-event", path, revision]))
    .digest("hex");

/** Directory reads never copy owner state or messages. */
export type ContextEntry = Pick<ContextSnapshot, "path" | "description">;

/** One atomic record: owner content and its durable public source evidence. */
export const StoredContext = Schema.Struct({
  snapshot: ContextSnapshot,
  events: Schema.Array(ContextEvent),
}).check(
  Schema.makeFilter(
    ({ snapshot, events }) => {
      let previousRevision = 0;
      for (const event of events) {
        const revision = event.record.revision;
        if (
          event.record.path !== snapshot.path ||
          revision <= previousRevision ||
          revision > snapshot.revision
        )
          return false;
        if (event.id !== contextEventId(snapshot.path, revision)) return false;
        if (!Number.isFinite(Date.parse(event.createdAt))) return false;
        previousRevision = revision;
      }
      return true;
    },
    { expected: "Ordered Context events matching their snapshot and stable identity" },
  ),
);
export type StoredContext = typeof StoredContext.Type;
