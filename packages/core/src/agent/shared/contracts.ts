import type { AgentMessage, AgentTool } from "@earendil-works/pi-agent-core";
import type { AssistantMessage } from "@earendil-works/pi-ai";
import { Schema } from "effect";

/** A tool's accepted operation cannot safely be followed by more model actions. */
export class ToolInvocationFailure extends Schema.TaggedError<ToolInvocationFailure>()(
  "ToolInvocationFailure",
  {
    message: Schema.String,
    outcome: Schema.Literals(["unknown", "failed"]),
    operationId: Schema.optional(Schema.String),
  },
) {}

export class AgentError extends Error {
  readonly _tag = "AgentError";
  readonly outcome?: "failed" | "unknown";
  readonly operationId?: string;
  constructor(
    message: string,
    readonly messages: readonly AgentMessage[] = [],
    options?: ErrorOptions & {
      readonly outcome?: "failed" | "unknown";
      readonly operationId?: string;
    },
  ) {
    super(message, options);
    this.outcome = options?.outcome;
    this.operationId = options?.operationId;
  }
}
export interface AgentMessages {
  readonly messages: readonly AgentMessage[];
}
export interface AgentResult {
  readonly messages: readonly AgentMessage[];
}

export interface AgentOptionsBase {
  readonly name: string;
  readonly tools?: readonly AgentTool[];
  /** Live provider responses before tool execution; observational, not a durable acknowledgement. */
  readonly onResponse?: (message: AssistantMessage, signal?: AbortSignal) => Promise<void> | void;
  /** Internal callback lifetime fence; the adapter owns and aborts this signal. */
  readonly invocationSignal?: AbortSignal;
}
