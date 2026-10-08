import { Schema } from "effect";
import type { AgentOptionsBase, AgentError } from "../shared/contracts.js";
import type { AssistantMessage } from "@earendil-works/pi-ai";
import type { InputSubmissionDraft, SubmissionRecord } from "@earendil-works/pi-durable";
import type { Effect, Option } from "effect";

export const DurableContextBudget = Schema.Struct({
  contextTokens: Schema.Int.check(Schema.isGreaterThan(0)),
  reserveTokens: Schema.Int.check(Schema.isGreaterThan(0)),
}).check(
  Schema.makeFilter((budget) => budget.reserveTokens < budget.contextTokens, {
    expected: "Output reserve smaller than the effective model context window",
  }),
);

/** Configuration and callbacks live for the whole conversation scope. */
export interface HarnessOptions extends AgentOptionsBase {
  readonly owner: string;
  readonly instructions: string;
  readonly extensionName?: string;
  readonly contextBudget?: typeof DurableContextBudget.Type;
}
export type HarnessInput = Omit<InputSubmissionDraft, "type" | "requestId"> & {
  readonly requestId: string;
};
export interface HarnessSubmission {
  readonly status: Effect.Effect<SubmissionRecord, AgentError>;
  /** Waiting is interruptible; cancelling a wait does not abort native work. */
  readonly wait: Effect.Effect<AssistantMessage | undefined, AgentError>;
}
export interface HarnessConversation {
  readonly submit: (input: HarnessInput) => Effect.Effect<HarnessSubmission, AgentError>;
  readonly submission: (
    requestId: string,
  ) => Effect.Effect<Option.Option<HarnessSubmission>, AgentError>;
  readonly abort: Effect.Effect<void, AgentError>;
}
