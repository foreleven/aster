import { defineDoc, defineEntry } from "@earendil-works/pi-durable";
import type { JsonValue } from "@earendil-works/chord";
import { StoredContext, ContextPath } from "@aster/core";
import { Schema } from "effect";

const PositiveId = Schema.Int.check(Schema.isGreaterThan(0));
export const PiContextMapping = Schema.Struct({
  path: ContextPath,
  conversationId: PositiveId,
  revision: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  entryId: PositiveId,
});
export const PiContextIndexSchema = Schema.Struct({
  mappingVersion: Schema.Literal(1),
  shardId: Schema.NonEmptyString,
  contexts: Schema.Array(PiContextMapping),
});
export const PiContextDocumentSchema = Schema.Struct({
  mappingVersion: Schema.Literal(1),
  entryId: PositiveId,
  record: StoredContext,
});
export const PiContextCommitSchema = Schema.Struct({
  mappingVersion: Schema.Literal(1),
  record: PiContextDocumentSchema.fields.record,
});

/** Aster bookkeeping entries never contribute model frames. A full snapshot
 * preserves message replacement/compaction as well as ordinary append. */
export const PiContextCommit = defineEntry<{ mappingVersion: 1; record: JsonValue }>(
  "app.aster.context.commit",
);
export const PiContextIndex = defineDoc<{
  mappingVersion: 1;
  shardId: string;
  contexts: Array<{ path: string; conversationId: number; revision: number; entryId: number }>;
}>({
  kind: "app.aster.context.index",
  version: 1,
  scope: "session",
  initial: () => ({ mappingVersion: 1, shardId: "", contexts: [] }),
});
export const PiContextDocument = defineDoc<{
  mappingVersion: 1;
  entryId: number;
  record: JsonValue;
}>({
  kind: "app.aster.context.snapshot",
  version: 1,
  scope: "conversation",
  history: "latest",
  fork: "initial",
  initial: () => ({ mappingVersion: 1, entryId: 0, record: null }),
});
