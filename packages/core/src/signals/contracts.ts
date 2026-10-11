import { GoalPath } from "../goals/contracts.js";
import { InternalTaskInput, ExternalTaskInput, Task } from "../tasks/contracts.js";
import { Cron, Result, Schema } from "effect";
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
  }).check(
    Schema.makeFilter((value) => Result.isSuccess(Cron.parse(value.expression, value.timeZone)), {
      expected: "A valid cron expression and timezone",
    }),
  ),
]);
export type SignalSchedule = typeof SignalSchedule.Type;
export const SignalTrigger = Schema.TaggedUnion({
  Context: { when: Schema.NonEmptyString },
  Schedule: { schedule: SignalSchedule },
});
export type SignalTrigger = typeof SignalTrigger.Type;

export const SignalAction = Schema.TaggedUnion({
  StartTask: { task: Task },
  SendGoalMessage: { goalPath: GoalPath, text: Schema.NonEmptyString },
});
export type SignalAction = typeof SignalAction.Type;
export const SignalEntry = Schema.Struct({ trigger: SignalTrigger, action: SignalAction });
export const SignalDefinition = Schema.Struct({ slug: Schema.String, ...SignalEntry.fields });
export type SignalDefinition = typeof SignalDefinition.Type;

/** Model inputs omit the Signal namespace and all delivery ownership metadata. */
export const SignalRuleId = Schema.String.check(Schema.isPattern(/^[a-z0-9][a-z0-9-]*$/));
export const SignalRuleVersion = Schema.Int.check(Schema.isGreaterThan(0));
export const SignalTriggerInput = Schema.Union([
  Schema.Struct({ type: Schema.Literal("context"), when: Schema.NonEmptyString }),
  Schema.Struct({ type: Schema.Literal("once"), at: SignalTime }),
  Schema.Struct({
    type: Schema.Literal("cron"),
    expression: Schema.String,
    timeZone: Schema.String,
  }).check(
    Schema.makeFilter((value) => Result.isSuccess(Cron.parse(value.expression, value.timeZone)), {
      expected: "A valid cron expression and timezone",
    }),
  ),
]);
export type SignalTriggerInput = typeof SignalTriggerInput.Type;
export const SignalActionInput = Schema.Union([
  Schema.Struct({ type: Schema.Literal("start_task"), ...InternalTaskInput.fields }),
  Schema.Struct({ type: Schema.Literal("delegate_task"), ...ExternalTaskInput.fields }),
  Schema.Struct({
    type: Schema.Literal("send_goal_message"),
    goalPath: GoalPath,
    text: Schema.NonEmptyString,
  }),
]);
export type SignalActionInput = typeof SignalActionInput.Type;
export const CreateSignalInput = Schema.Struct({
  id: SignalRuleId,
  trigger: SignalTriggerInput,
  action: SignalActionInput,
});
export type CreateSignalInput = typeof CreateSignalInput.Type;
export const UpdateSignalInput = Schema.Struct({
  ...CreateSignalInput.fields,
  version: SignalRuleVersion,
});
export type UpdateSignalInput = typeof UpdateSignalInput.Type;
export const ControlSignalInput = Schema.Struct({
  id: SignalRuleId,
  version: SignalRuleVersion,
  operation: Schema.Literals(["pause", "resume", "delete"]),
});
export type ControlSignalInput = typeof ControlSignalInput.Type;
