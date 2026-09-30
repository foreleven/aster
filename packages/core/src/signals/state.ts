import { Schema } from "effect";
import { SignalDefinition } from "../config/schema.js";
import { ContextRecord } from "../context/model.js";

const Occurrence = Schema.Struct({
  id: Schema.String,
  text: Schema.String,
  delivered: Schema.Boolean,
  source: ContextRecord,
});

/** Delivery flags and timer revisions are executable recovery state, not arbitrary metadata. */
export const SignalState = Schema.Struct({
  ...SignalDefinition.fields,
  goal: Schema.optional(Schema.String),
  active: Schema.optional(Schema.Boolean),
  deleted: Schema.optional(Schema.Boolean),
  revision: Schema.optional(Schema.Int),
  seenSources: Schema.optional(Schema.Array(Schema.String)),
  nextDue: Schema.optional(Schema.Number.check(Schema.isFinite())),
  timerDone: Schema.optional(Schema.Boolean),
  occurrences: Schema.optional(Schema.Array(Occurrence)),
});
export type SignalState = typeof SignalState.Type;
