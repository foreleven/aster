import { Context, Effect, Layer } from "effect";
import { Agent, type AgentOptions } from "./ordinary.js";
import { AgentModels } from "../models.js";
import type { AgentError, AgentMessages, AgentResult } from "../shared/contracts.js";
import { withAgentCallbacks } from "../shared/callbacks.js";
import { nativeCallbacks, type EffectInvocation } from "../shared/effect-tools.js";

export type AgentInvocation = AgentOptions & AgentMessages;
export type AgentRequest<E = never, R = never> = EffectInvocation<AgentInvocation, E, R>;

/** Isolated execution: each invocation owns a fresh Agent and callback scope. */
export class AgentRuntime extends Context.Service<
  AgentRuntime,
  {
    readonly run: <E = never, R = never>(
      request: AgentRequest<E, R>,
    ) => Effect.Effect<AgentResult, AgentError, R>;
  }
>()("agent/Runtime") {
  static readonly make = (
    execute: (invocation: AgentInvocation) => Effect.Effect<AgentResult, AgentError>,
  ): AgentRuntime["Service"] => ({
    run: <E = never, R = never>(request: AgentRequest<E, R>) =>
      withAgentCallbacks<AgentResult, AgentError, R>((invoke) =>
        execute({ ...request, ...nativeCallbacks(request, invoke) }),
      ),
  });

  static readonly layer = Layer.effect(
    AgentRuntime,
    Effect.gen(function* () {
      const models = yield* AgentModels;
      return AgentRuntime.make(
        Effect.fn("AgentRuntime.run")(function* ({ messages, ...options }) {
          const agent = yield* Agent.make(options).pipe(Effect.provideService(AgentModels, models));
          return yield* agent.run({ messages });
        }),
      );
    }),
  );
}
