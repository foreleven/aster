import { after } from "node:test";
import { AgentConversations } from "@aster/agent/harness";
import { Effect, Exit, Scope } from "effect";

/** Native Pi memory storage, with writer lifetime owned by the enclosing node:test case. */
export const testConversations = (root?: string) => {
  const scope = Effect.runSync(Scope.make());
  after(() => Effect.runPromise(Scope.close(scope, Exit.void)));
  return Effect.runSync(
    (root ? AgentConversations.make({ root }) : AgentConversations.makeMemory()).pipe(
      Effect.provideService(Scope.Scope, scope),
    ),
  );
};
