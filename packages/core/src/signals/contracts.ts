import { Schema } from "effect";
export const SignalTime = Schema.String.check(
  Schema.isPattern(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/),
  Schema.makeFilter((value) => Number.isFinite(Date.parse(value)), {
    expected: "An ISO timestamp with a timezone offset",
  }),
);
export const SignalSchedule = Schema.Union([
  Schema.Struct({ type: Schema.Literal("once"), at: SignalTime }),
  Schema.Struct({
    type: Schema.Literal("cron"),
    expression: Schema.String,
    timeZone: Schema.String,
  }),
]);
export type SignalSchedule = typeof SignalSchedule.Type;
export const SignalTrigger = Schema.TaggedUnion({
  Context: { when: Schema.NonEmptyString },
  Schedule: { schedule: SignalSchedule },
});
export type SignalTrigger = typeof SignalTrigger.Type;
