import { Schema } from "effect";
export const PublicContext = Schema.Struct({
  projection: Schema.optional(
    Schema.Struct({
      visibility: Schema.Literals(["public", "restricted"]),
      reason: Schema.optional(Schema.Literals(["missing-policy", "invalid-data"])),
    }),
  ),
  path: Schema.String,
  revision: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  description: Schema.String,
  state: Schema.ObjectKeyword,
  messages: Schema.Array(Schema.Unknown),
});
export type PublicContext = typeof PublicContext.Type;

/** Query adapters accept named scalar arguments, never shell text. */
export const ContextQueryInput = Schema.Struct({
  path: Schema.NonEmptyString,
  command: Schema.NonEmptyString,
  args: Schema.Record(Schema.String, Schema.Union([Schema.String, Schema.Number, Schema.Boolean])),
});
export type ContextQueryInput = typeof ContextQueryInput.Type;

export const ContextQueryResult = Schema.Struct({
  path: Schema.String,
  command: Schema.String,
  queriedAt: Schema.String,
  data: Schema.Json,
});
export type ContextQueryResult = typeof ContextQueryResult.Type;

export class ContextQueryError extends Schema.TaggedError<ContextQueryError>()(
  "ContextQueryError",
  {
    kind: Schema.Literals([
      "invalid-input",
      "unavailable",
      "busy",
      "failed",
      "timeout",
      "cancelled",
    ]),
    message: Schema.String,
  },
) {}
