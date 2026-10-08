import { Context, Effect, Layer, type Scope } from "effect";
import { Models } from "../models.js";
import { AgentConversations } from "./conversations.js";
import { openConversation } from "./execution.js";
import type { HarnessConversation, HarnessOptions } from "./contracts.js";
import type { AgentError } from "../shared/contracts.js";
import { withAgentCallbacks } from "../shared/callbacks.js";
import { nativeCallbacks, type EffectInvocation } from "../shared/effect-tools.js";

export type HarnessRequest<E = never, R = never> = EffectInvocation<HarnessOptions, E, R>;

export class DurableHarness extends Context.Service<
  DurableHarness,
  {
    readonly withConversation: <A, E, R, TE = never, TR = never>(
      options: HarnessRequest<TE, TR>,
      use: (conversation: HarnessConversation) => Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | AgentError, R | TR>;
  }
>()("agent/DurableHarness") {
  static readonly make = (
    open: (options: HarnessOptions) => Effect.Effect<HarnessConversation, AgentError, Scope.Scope>,
  ): DurableHarness["Service"] => ({
    withConversation: (options, use) =>
      withAgentCallbacks((invoke) =>
        Effect.scoped(
          Effect.flatMap(open({ ...options, ...nativeCallbacks(options, invoke) }), use),
        ),
      ),
  });
  static readonly layer = Layer.effect(
    DurableHarness,
    Effect.gen(function* () {
      const models = yield* Models;
      const conversations = yield* AgentConversations;
      return DurableHarness.make((options) =>
        openConversation(options).pipe(
          Effect.provideService(Models, models),
          Effect.provideService(AgentConversations, conversations),
        ),
      );
    }),
  );
}
