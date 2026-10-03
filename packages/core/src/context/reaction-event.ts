import { createHash } from "node:crypto";
import { Schema } from "effect";
import type { ContextReactionEvent } from "./model.js";

export const reactionEventId = (path: string, revision: number): string =>
  createHash("sha256")
    .update(JSON.stringify(["context-reaction-v1", path, revision]))
    .digest("hex");

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
      if (event.requestId !== reactionEventId(record.path, event.revision)) return false;
      if (event.causationId !== event.requestId) return false;
      if (!Number.isFinite(Date.parse(event.createdAt))) return false;
      previousRevision = event.revision;
    }
    return true;
  },
  { expected: "Ordered reaction envelopes matching their owner, snapshot and stable identity" },
);
