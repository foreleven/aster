import { Schema } from "effect";
/** Generic evidence contains serialized State only; business APIs own structured data. */
export const ContextText = Schema.Struct({
  path: Schema.String,
  revision: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  description: Schema.String,
  text: Schema.String,
});
export type ContextText = typeof ContextText.Type;

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
      "failed",
      "timeout",
      "cancelled",
    ]),
    message: Schema.String,
  },
) {}
