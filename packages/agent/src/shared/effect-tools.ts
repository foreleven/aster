import type { AgentTool, AgentToolResult } from "@earendil-works/pi-agent-core";
import type { AssistantMessage, TSchema } from "@earendil-works/pi-ai";
import { Effect } from "effect";
import type { AgentOptionsBase } from "./contracts.js";
import type { AgentCallbackInvoker } from "./callbacks.js";

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

export interface EffectCallbacks<E = never, R = never> {
  readonly tools?: readonly EffectTool<TSchema, E, R>[];
  readonly onResponse?: (message: AssistantMessage) => Effect.Effect<void, E, R>;
}
export type EffectInvocation<I, E, R> = Omit<I, "tools" | "onResponse"> & EffectCallbacks<E, R>;

/** The only Effect-to-Promise adaptation for runner tools and observational callbacks. */
export const nativeCallbacks = <E, R>(
  request: EffectCallbacks<E, R>,
  invoke: AgentCallbackInvoker<R>,
): { tools: AgentOptionsBase["tools"]; onResponse: AgentOptionsBase["onResponse"] } => {
  const { tools, onResponse } = request;
  return {
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
