import type { AgentMessage, AgentTool } from "@earendil-works/pi-agent-core";
import type { AssistantMessage } from "@earendil-works/pi-ai";

export class AgentError extends Error {
  readonly _tag = "AgentError";
  readonly outcome?: "failed" | "unknown";
  constructor(
    message: string,
    readonly messages: readonly AgentMessage[] = [],
    options?: ErrorOptions & { readonly outcome?: "failed" | "unknown" },
  ) {
    super(message, options);
    this.outcome = options?.outcome;
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
}
