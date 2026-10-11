import { Context, Effect, Layer, type Scope } from "effect";
import { AgentModels } from "../models.js";
import { AgentConversations } from "./conversations.js";
import { openSession } from "./session-runtime.js";
import type { HarnessConversation, HarnessOptions } from "./session-contracts.js";
import type { AgentError } from "../shared/contracts.js";
import { withAgentCallbacks } from "../shared/callbacks.js";
import { nativeCallbacks, type EffectInvocation } from "../shared/effect-tools.js";

export type HarnessRequest<E = never, R = never> = EffectInvocation<HarnessOptions, E, R>;

export class AgentSession extends Context.Service<
  AgentSession,
  {
    readonly withConversation: <A, E, R, TE = never, TR = never>(
      options: HarnessRequest<TE, TR>,
      use: (conversation: HarnessConversation) => Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | AgentError, R | TR>;
  }
>()("agent/AgentSession") {
  static readonly make = (
    open: (options: HarnessOptions) => Effect.Effect<HarnessConversation, AgentError, Scope.Scope>,
  ): AgentSession["Service"] => ({
    withConversation: (options, use) =>
      withAgentCallbacks((invoke) =>
        Effect.scoped(
          Effect.flatMap(
            open({
              ...options,
              ...nativeCallbacks(options, invoke),
            }),
            use,
          ),
        ),
      ),
  });
  static readonly layer = Layer.effect(
    AgentSession,
    Effect.gen(function* () {
      const models = yield* AgentModels;
      const conversations = yield* AgentConversations;
      return AgentSession.make((options) =>
        openSession(options).pipe(
          Effect.provideService(AgentModels, models),
          Effect.provideService(AgentConversations, conversations),
        ),
      );
    }),
  );
}
