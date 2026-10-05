import {
  BusinessNotification,
  CausalChain,
  TaskAdmission,
  RunResumption,
  WritebackOperation,
} from "@aster/api-contracts";
import { Match, Option, Schema } from "effect";
import { PublicContext as ContextRecord } from "@aster/api-contracts";
import { SignalDefinition } from "../config/schema.js";
import { Task } from "./model.js";

const fields = {
  writeback: Schema.optional(WritebackOperation),
  causal: Schema.optional(CausalChain),
  businessOutbox: Schema.optional(Schema.Array(BusinessNotification)),
  resumptions: Schema.optional(Schema.Array(RunResumption)),
  admission: Schema.optional(TaskAdmission),
  outcomeText: Schema.optional(Schema.String),
  signalSlug: Schema.String,
  sourcePath: Schema.String,
  definition: SignalDefinition,
  source: ContextRecord,
  approvals: Schema.optional(Schema.Array(Schema.String)),
  // The durable session belongs to Delegation. A recovered completion can arrive
  // without a new Submitted notification, so the Run's display ID is optional.
  sessionId: Schema.optional(Schema.String),
};

/** Preserve the persisted status discriminator while validating each phase's prerequisites. */
export const RunState = Schema.Union([
  Schema.Struct({
    ...fields,
    status: Schema.Literals(["preparing", "preparation-failed", "cancelled", "failed"]),
    task: Schema.optional(Task),
  }),
  Schema.Struct({
    ...fields,
    status: Schema.Literals([
      "checking",
      "ready",
      "awaiting-confirmation",
      "blocked",
      "rejected",
      "submitting",
      "running",
      "waiting_input",
      "completed",
      "uncertain",
    ]),
    task: Task,
  }),
]);
export type RunState = typeof RunState.Type;

const TerminalEvent = Schema.Struct({ type: Schema.String, text: Schema.optional(Schema.String) });
const terminalEvent = Schema.decodeUnknownOption(TerminalEvent);

/** Legacy records keep their result in messages; new records carry a stable replay payload. */
export const terminalRunText = (
  state: RunState,
  messages: readonly unknown[],
  includeUncertain = false,
): string | undefined => {
  const eventTypes: readonly string[] = Match.value(state.status).pipe(
    Match.when("completed", () => ["Completed"]),
    Match.when("failed", () => ["Failed", "Error"]),
    Match.when("cancelled", () => ["Cancelled"]),
    Match.when("blocked", () => ["NotExecutable"]),
    Match.when("rejected", () => ["ConfirmationResolved"]),
    Match.when("preparation-failed", () => ["PreparationFailed", "RecoveryFailed"]),
    Match.when("uncertain", () => (includeUncertain ? ["Uncertain", "Error"] : [])),
    Match.orElse(() => []),
  );
  if (!eventTypes.length) return undefined;
  if (state.outcomeText !== undefined) return state.outcomeText;
  const event = messages
    .map((message) => terminalEvent(message))
    .filter(Option.isSome)
    .map((value) => value.value)
    .findLast((value) => eventTypes.includes(value.type));
  return event?.text ?? `Execution ${state.status}`;
};
