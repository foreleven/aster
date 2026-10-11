import { after } from "node:test";
import { createHash } from "node:crypto";
import { AgentConversations } from "../src/agent/runtime/session.js";
import { openNodeJsonlStorage } from "@earendil-works/pi-durable/storage/jsonl/node";
import { Effect, Exit, Scope } from "effect";

/** Native Pi memory storage, with writer lifetime owned by the enclosing node:test case. */
export const testConversations = (root?: string) => {
  const scope = Effect.runSync(Scope.make());
  after(() => Effect.runPromise(Effect.scoped(Scope.close(scope, Exit.void))));
  return Effect.runSync(
    (root
      ? AgentConversations.make({
          openStorage: (owner, context) =>
            openNodeJsonlStorage(
              `${root}/${createHash("sha256").update(owner).digest("hex")}`,
              context,
              { fsync: true },
            ),
        })
      : AgentConversations.makeMemory()
    ).pipe(Effect.provideService(Scope.Scope, scope)),
  );
};
