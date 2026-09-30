import { Match, Schema } from "effect";

/** Business outcomes survive transport and parent reattachment without becoming generic errors. */
export const ExecutionOutcome = Schema.Union([
  Schema.TaggedStruct("Completed", { text: Schema.String }),
  Schema.TaggedStruct("Failed", { text: Schema.String }),
  Schema.TaggedStruct("Cancelled", { text: Schema.String }),
  Schema.TaggedStruct("Uncertain", { text: Schema.String }),
]);
export type ExecutionOutcome = typeof ExecutionOutcome.Type;

export const outcomeStatus = (outcome: ExecutionOutcome) =>
  Match.value(outcome).pipe(
    Match.tag("Completed", () => "completed" as const),
    Match.tag("Failed", () => "failed" as const),
    Match.tag("Cancelled", () => "cancelled" as const),
    Match.tag("Uncertain", () => "uncertain" as const),
    Match.exhaustive,
  );
