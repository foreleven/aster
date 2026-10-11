import type { AgentTool, AgentToolResult } from "@earendil-works/pi-agent-core";
import type { AssistantMessage, TSchema } from "@earendil-works/pi-ai";
import { Effect, Schema } from "effect";
import type { AgentOptionsBase } from "./contracts.js";
import type { AgentCallbackInvoker } from "./callbacks.js";
import { toolValidationFeedback } from "./tool-validation.js";

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

/** JSON Schema is preliminary SDK validation; Effect decoding remains authoritative. */
export const schemaTool = <A, E = never, R = never>(definition: {
  readonly name: string;
  readonly label: string;
  readonly description: string;
  readonly replay: "safe" | "never";
  readonly parameters: Schema.Decoder<A>;
  readonly execute: (callId: string, args: A) => Effect.Effect<AgentToolResult<unknown>, E, R>;
}): EffectTool<TSchema, E, R> => {
  const document = Schema.toJsonSchemaDocument(definition.parameters, {
    onExcessProperty: "error",
  });
  const decode = Schema.decodeUnknownEffect(Schema.toCodecJson(definition.parameters), {
    onExcessProperty: "error",
    reportInput: false,
    errors: "all",
  });
  const parameters = { ...document.schema, $defs: document.definitions };
  const feedback = toolValidationFeedback(parameters);
  return {
    name: definition.name,
    label: definition.label,
    description: definition.description,
    replay: definition.replay,
    parameters,
    execute: (callId, args) =>
      decode(args).pipe(
        Effect.matchEffect({
          onFailure: (error) => Effect.succeed(feedback(error)),
          onSuccess: (decoded) => Effect.suspend(() => definition.execute(callId, decoded)),
        }),
      ),
  };
};

export interface EffectCallbacks<E = never, R = never> {
  readonly tools?: readonly EffectTool<TSchema, E, R>[];
  readonly onResponse?: (message: AssistantMessage) => Effect.Effect<void, E, R>;
}
export type EffectInvocation<I, E, R> = Omit<I, "tools" | "onResponse"> & EffectCallbacks<E, R>;

/** The only Effect-to-Promise adaptation for runner tools and observational callbacks. */
export const nativeCallbacks = <E, R>(
  request: EffectCallbacks<E, R>,
  invoke: AgentCallbackInvoker<R>,
): {
  tools: AgentOptionsBase["tools"];
  onResponse: AgentOptionsBase["onResponse"];
  invocationSignal: AbortSignal;
} => {
  const { tools, onResponse } = request;
  return {
    invocationSignal: invoke.signal,
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
