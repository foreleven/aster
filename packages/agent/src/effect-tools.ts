import type { AgentMessage, AgentTool, AgentToolResult } from "@earendil-works/pi-agent-core";
import type { AssistantMessage, TSchema } from "@earendil-works/pi-ai";
import { Effect } from "effect";
import type { AgentInvocation } from "./index.js";
import type { AgentCallbackInvoker } from "./agent-callbacks.js";

/** Domain tools describe Effects; the runner owns their SDK callback lifetime. */
export interface EffectTool<T extends TSchema = TSchema, E = never, R = never> extends Omit<
  AgentTool<T>,
  "execute"
> {
  execute(
    callId: string,
    args: Parameters<AgentTool<T>["execute"]>[1],
  ): Effect.Effect<AgentToolResult<unknown>, E, R>;
}

export type AgentRequest<E = never, R = never> = Omit<
  AgentInvocation,
  "tools" | "onMessage" | "onResponse" | "transformContext"
> & {
  readonly tools?: readonly EffectTool<TSchema, E, R>[];
  readonly onMessage?: (message: AgentMessage) => Effect.Effect<void, E, R>;
  readonly onResponse?: (message: AssistantMessage) => Effect.Effect<void, E, R>;
  readonly transformContext?: (messages: AgentMessage[]) => Effect.Effect<AgentMessage[], E, R>;
};

/** The only Effect-to-Promise adaptation for runner tools and observational callbacks. */
export const nativeInvocation = <E, R>(
  request: AgentRequest<E, R>,
  invoke: AgentCallbackInvoker<R>,
): AgentInvocation => {
  const { tools, onMessage, onResponse, transformContext, ...options } = request;
  return {
    ...options,
    tools: tools?.map(({ execute, ...definition }) => ({
      ...definition,
      execute: (id, args, signal) =>
        invoke(
          Effect.suspend(() => execute(id, args)),
          signal,
        ),
    })),
    onMessage: onMessage
      ? (message) => invoke(Effect.suspend(() => onMessage(message)))
      : undefined,
    onResponse: onResponse
      ? (message, signal) =>
          invoke(
            Effect.suspend(() => onResponse(message)),
            signal,
          )
      : undefined,
    transformContext: transformContext
      ? (messages, signal) =>
          invoke(
            Effect.suspend(() => transformContext(messages)),
            signal,
          )
      : undefined,
  };
};
