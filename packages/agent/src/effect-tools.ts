import type { AgentTool, AgentToolResult } from "@earendil-works/pi-agent-core";
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

type EffectInvocation<I, E, R> = I extends unknown
  ? Omit<I, "tools" | "onResponse"> & {
      readonly tools?: readonly EffectTool<TSchema, E, R>[];
      readonly onResponse?: (message: AssistantMessage) => Effect.Effect<void, E, R>;
    }
  : never;
export type AgentRequest<E = never, R = never> = EffectInvocation<AgentInvocation, E, R>;

/** The only Effect-to-Promise adaptation for runner tools and observational callbacks. */
export const nativeInvocation = <E, R>(
  request: AgentRequest<E, R>,
  invoke: AgentCallbackInvoker<R>,
): AgentInvocation => {
  const { tools, onResponse, ...options } = request;
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
    onResponse: onResponse
      ? (message, signal) =>
          invoke(
            Effect.suspend(() => onResponse(message)),
            signal,
          )
      : undefined,
  };
};
