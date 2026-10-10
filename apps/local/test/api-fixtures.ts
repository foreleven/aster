import { makeTestContext, type TestContext } from "@aster/core/testing";
import type { TestContext as NodeTestContext } from "node:test";
import { DurableContext } from "@aster/core";
import { ActorNotFound, type ActorSystem } from "@aster/actor";
import { AgentConversations } from "@aster/agent/harness";
import { AsterRuntime, ContextQueries } from "@aster/core";
import { NodeFileSystem } from "@effect/platform-node";
import { Effect, Exit, Layer, Scope } from "effect";
import { makeHttpApi } from "../src/http-api.js";
import { testConversations } from "./conversation-fixtures.js";

export interface ApiFixture {
  readonly registry: TestContext;
  readonly actors?: Pick<ActorSystem, "select">;
  readonly conversations?: AgentConversations["Service"];
  readonly inspect?: AsterRuntime["Service"]["inspect"];
  readonly queries?: ContextQueries["Service"];
}
export const apiServices = (options: ApiFixture) =>
  Layer.mergeAll(
    Layer.succeed(AsterRuntime, {
      actors: options.actors ?? {
        select: (path) => ({ path, resolve: () => Effect.fail(new ActorNotFound(path)) }),
      },
      ready: Effect.void,
      inspect: options.inspect ?? Effect.succeed({ phase: "ready", actors: [], events: [] }),
    }),
    Layer.mergeAll(Layer.succeed(DurableContext, options.registry.backend)),
    Layer.succeed(AgentConversations, options.conversations ?? testConversations()),
    options.queries ? Layer.succeed(ContextQueries, options.queries) : ContextQueries.layer,
  );

/** Promise boundary only for node:test HTTP callers. The fixture owns the server Scope. */
export const startTestHttp = async (options: ApiFixture & { port?: number; webDir?: string }) => {
  const scope = await Effect.runPromise(Effect.scoped(Scope.make()));
  try {
    const server = await Effect.runPromise(
      Effect.scoped(
        makeHttpApi({ port: options.port ?? 0, webDir: options.webDir }).pipe(
          Effect.provide(apiServices(options)),
          Effect.provide(NodeFileSystem.layer),
          Effect.provideService(Scope.Scope, scope),
        ),
      ),
    );
    return {
      ...server,
      close: () => Effect.runPromise(Effect.scoped(Scope.close(scope, Exit.void))),
    };
  } catch (cause) {
    await Effect.runPromise(Effect.scoped(Scope.close(scope, Exit.void)));
    throw cause;
  }
};
export const rpcRequest = (tag: string, payload: unknown = null, id = "1") => ({
  method: "POST",
  headers: { "content-type": "application/ndjson" },
  body: JSON.stringify({ _tag: "Request", id, tag, payload, headers: [] }) + "\n",
});

/** Keep subscriptions alive for the entire HTTP test. */
export const createTestContext = async (test: NodeTestContext) => {
  const scope = await Effect.runPromise(Scope.make());
  test.after(() => Effect.runPromise(Scope.close(scope, Exit.void)));
  return Effect.runPromise(makeTestContext().pipe(Effect.provideService(Scope.Scope, scope)));
};
