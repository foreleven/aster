import { Context, Effect, Layer } from "effect";
import { Agent, type AgentOptions } from "./agent.js";
import { Models } from "../models.js";
import type { AgentError, AgentMessages, AgentResult } from "../shared/contracts.js";
import { withAgentCallbacks } from "../shared/callbacks.js";
import { nativeCallbacks, type EffectInvocation } from "../shared/effect-tools.js";

export type AgentInvocation = AgentOptions & AgentMessages;
export type AgentRequest<E = never, R = never> = EffectInvocation<AgentInvocation, E, R>;

/** Isolated execution: each invocation owns a fresh Agent and callback scope. */
export class AgentRunner extends Context.Service<
  AgentRunner,
  {
    readonly run: <E = never, R = never>(
      request: AgentRequest<E, R>,
    ) => Effect.Effect<AgentResult, AgentError, R>;
  }
>()("agent/Runner") {
  static readonly make = (
    execute: (invocation: AgentInvocation) => Effect.Effect<AgentResult, AgentError>,
  ): AgentRunner["Service"] => ({
    run: <E = never, R = never>(request: AgentRequest<E, R>) =>
      withAgentCallbacks<AgentResult, AgentError, R>((invoke) =>
        execute({ ...request, ...nativeCallbacks(request, invoke) }),
      ),
  });

  static readonly layer = Layer.effect(
    AgentRunner,
    Effect.gen(function* () {
      const models = yield* Models;
      return AgentRunner.make(
        Effect.fn("AgentRunner.run")(function* ({ messages, ...options }) {
          const agent = yield* Agent.make(options).pipe(Effect.provideService(Models, models));
          return yield* agent.run({ messages });
        }),
      );
    }),
  );
}
