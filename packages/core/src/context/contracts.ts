import { Schema } from "effect";
/** Generic evidence contains serialized State only; business APIs own structured data. */
export const ContextText = Schema.Struct({
  path: Schema.String,
  revision: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  description: Schema.String,
  text: Schema.String,
});
export type ContextText = typeof ContextText.Type;
