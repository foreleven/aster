import { after } from "node:test";
import { AgentConversations } from "@aster/core/agent/session";
import { Effect, Exit, Scope } from "effect";

/** Native Pi memory storage, with writer lifetime owned by the enclosing node:test case. */
export const testConversations = (_root?: string) => {
  const scope = Effect.runSync(Scope.make());
  after(() => Effect.runPromise(Effect.scoped(Scope.close(scope, Exit.void))));
  return Effect.runSync(
    AgentConversations.makeMemory().pipe(Effect.provideService(Scope.Scope, scope)),
  );
};
