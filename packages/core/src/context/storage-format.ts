import { Schema } from "effect";
import { PublicContext } from "@aster/api-contracts";
import { ContextPath, contextEventId } from "./model.js";

// v1 disk/Pi envelopes remain readable and writable without rewriting user data.
/** Validated by storage decoding before pending records can rewrite committed files. */
export const reactionEventsCheck = Schema.makeFilter(
  (record: {
    readonly path: string;
    readonly revision?: number | undefined;
    readonly reactionEvents?: ReadonlyArray<ContextReactionEvent> | undefined;
  }) => {
    let previousRevision = 0;
    for (const event of record.reactionEvents ?? []) {
      if (event.source !== record.path || event.record.path !== record.path) return false;
      if (event.revision <= previousRevision || event.revision > (record.revision ?? 0))
        return false;
      if (event.record.revision !== event.revision) return false;
      if (event.requestId !== contextEventId(record.path, event.revision)) return false;
      if (event.causationId !== event.requestId) return false;
      if (!Number.isFinite(Date.parse(event.createdAt))) return false;
      previousRevision = event.revision;
    }
    return true;
  },
  { expected: "Ordered reaction envelopes matching their owner, snapshot and stable identity" },
);

/** Immutable source-side handoff. Only public evidence enters the reaction pipeline. */
export const ContextReactionEvent = Schema.Struct({
  requestId: Schema.NonEmptyString,
  causationId: Schema.NonEmptyString,
  source: Schema.String,
  target: Schema.Literal("/system-one"),
  revision: Schema.Int.check(Schema.isGreaterThan(0)),
  record: PublicContext,
  createdAt: Schema.String,
});
export type ContextReactionEvent = typeof ContextReactionEvent.Type;

/** Canonical recovery metadata is deliberately absent from PublicContext. */
export const ContextRecord = Schema.Struct({
  ...PublicContext.fields,
  reactionEvents: Schema.optional(Schema.Array(ContextReactionEvent)),
}).check(reactionEventsCheck);
export type ContextRecord = typeof ContextRecord.Type;

/** Shared identity validation for every authoritative backend. */
export const DurableContextSnapshot = Schema.Struct({
  ...ContextRecord.fields,
  path: ContextPath,
}).check(reactionEventsCheck);
