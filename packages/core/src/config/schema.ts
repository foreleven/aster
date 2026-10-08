import { Cron, Schema } from "effect";
import { SignalSchedule, SignalTrigger } from "../signals/contracts.js";
import { Task } from "../tasks/contracts.js";
export { SignalSchedule } from "../signals/contracts.js";

export const validateSignalTime = (signal: { schedule: SignalSchedule }) => {
  const absolute = (value: string) => {
    if (!/(Z|[+-]\d{2}:\d{2})$/.test(value) || !Number.isFinite(Date.parse(value)))
      throw new Error("Time requires a valid absolute ISO timestamp with offset");
  };
  if (signal.schedule?.type === "once") absolute(signal.schedule.at);
  if (signal.schedule?.type === "cron")
    Cron.parseUnsafe(signal.schedule.expression, signal.schedule.timeZone);
};

export const SignalEntry = Schema.Struct({
  trigger: SignalTrigger,
  task: Task,
});

export const GoalTitle = Schema.String.check(Schema.isPattern(/\S/));

export const SignalDefinition = Schema.Struct({ slug: Schema.String, ...SignalEntry.fields });
export type SignalDefinition = typeof SignalDefinition.Type;

export const GoalDefinition = Schema.Struct({
  slug: Schema.String,
  title: Schema.optional(GoalTitle),
  description: Schema.NonEmptyString,
});
export type GoalDefinition = typeof GoalDefinition.Type;
