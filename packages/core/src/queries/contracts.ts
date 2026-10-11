import { Schema } from "effect";
/** Query adapters accept named scalar arguments, never shell text. */
export const ContextQueryInput = Schema.Struct({
  path: Schema.NonEmptyString,
  command: Schema.NonEmptyString,
  args: Schema.Record(Schema.String, Schema.Union([Schema.String, Schema.Number, Schema.Boolean])),
});
export type ContextQueryInput = typeof ContextQueryInput.Type;

export class ContextQueryError extends Schema.TaggedError<ContextQueryError>()(
  "ContextQueryError",
  {
    kind: Schema.Literals([
      "invalid-input",
      "unavailable",
      "busy",
      "duplicate",
      "failed",
      "timeout",
      "cancelled",
    ]),
    message: Schema.String,
  },
) {}

export const QueryPage = Schema.Struct({
  resultId: Schema.Int,
  path: Schema.String,
  content: Schema.String,
  totalCharacters: Schema.Int,
  nextOffset: Schema.NullOr(Schema.Int),
});
export type QueryPage = typeof QueryPage.Type;
