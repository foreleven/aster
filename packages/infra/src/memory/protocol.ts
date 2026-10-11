import { Schema } from "effect";
const NonBlank = Schema.String.check(Schema.isPattern(/\S/));
export const MemoryConnection = Schema.Struct({
  url: NonBlank,
  secret: Schema.RedactedFromValue(NonBlank),
  dataDir: NonBlank,
  project: Schema.String,
  cwd: NonBlank,
});
export type MemoryConnection = typeof MemoryConnection.Type;
export const Row = Schema.Record(Schema.String, Schema.Unknown);
export type Row = typeof Row.Type;
export const Envelope = Row;
export const Observation = Schema.Struct({ observationId: NonBlank });
export const ObservationReceipt = Schema.Struct({
  observationId: Schema.optional(NonBlank),
  deduplicated: Schema.optional(Schema.Boolean),
  sessionId: Schema.optional(NonBlank),
});
export const MemoryWriteResponse = Schema.Struct({
  success: Schema.optional(Schema.Boolean),
  memory: Schema.optional(Schema.StructWithRest(Schema.Struct({ id: NonBlank }), [Row])),
  error: Schema.optional(Schema.String),
});
export const Observations = Schema.Struct({
  observations: Schema.Array(
    Schema.Struct({ id: Schema.String, title: Schema.optional(Schema.String) }),
  ),
});
const provenanceFields = {
  sourceObservationIds: Schema.optional(Schema.Array(NonBlank)),
  sessionIds: Schema.optional(Schema.Array(NonBlank)),
};
export const MemoryDetail = Schema.StructWithRest(Schema.Struct(provenanceFields), [Row]);
const SearchHit = Schema.StructWithRest(
  Schema.Struct({
    obsId: NonBlank,
    sessionId: Schema.optional(NonBlank),
    observation: Schema.optional(MemoryDetail),
  }),
  [Row],
);
export const SearchResults = Schema.Struct({
  results: Schema.Array(SearchHit),
  truncated: Schema.optional(Schema.Boolean),
});
export const ExpandedResults = Schema.Struct({
  results: Schema.Array(
    Schema.StructWithRest(
      Schema.Struct({
        obsId: NonBlank,
        sessionId: Schema.optional(NonBlank),
        observation: MemoryDetail,
      }),
      [Row],
    ),
  ),
  truncated: Schema.optional(Schema.Boolean),
});
export const ConsolidatedMemory = Schema.Struct({
  memory: Schema.StructWithRest(Schema.Struct({ ...provenanceFields, content: NonBlank }), [Row]),
});
export interface MemoryReference {
  readonly obsId: string;
  readonly sessionId?: string;
}
