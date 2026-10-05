import { Schema } from "effect";
export const SignalSchedule = Schema.Union([
  Schema.Struct({ type: Schema.Literal("once"), at: Schema.String }),
  Schema.Struct({
    type: Schema.Literal("cron"),
    expression: Schema.String,
    timeZone: Schema.String,
  }),
]);
export type SignalSchedule = typeof SignalSchedule.Type;
export const SignalTrigger = Schema.Union([
  Schema.TaggedStruct("Context", { when: Schema.NonEmptyString }),
  Schema.TaggedStruct("Schedule", { schedule: SignalSchedule }),
]);
export type SignalTrigger = typeof SignalTrigger.Type;
