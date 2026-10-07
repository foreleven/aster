import { Config, Context, Effect, FileSystem, Layer, Scope } from "effect";
import { NodeFileSystem, NodeHttpServer } from "@effect/platform-node";
import { HttpRouter } from "effect/http";
import { NetAddress } from "effect/net";
import { RpcSerialization, RpcServer } from "effect/rpc";
import * as ApiServer from "@aster/api/server";
import { AgentConversations } from "@aster/agent";
import { createServer } from "node:http";
import { resolve } from "node:path";
import { AsterRuntime, ConfigLocation, ContextRegistry, ContextQueries } from "@aster/core";
import { withHttpPolicy } from "./http-policy.js";
import { staticAssets } from "./static-assets.js";
import { sourceAssets } from "./source-assets.js";

interface BoundHttpApi {
  readonly url: string;
}
interface ApiOptions {
  readonly port?: number;
  readonly webDir?: string;
  readonly webSourceDir?: string;
}

/** Host owns transport scopes. The API package owns all business handlers. */
export const makeHttpApi = Effect.fn("LocalHttpApi.make")(function* (
  options: ApiOptions,
): Effect.fn.Return<
  BoundHttpApi,
  unknown,
  | Scope.Scope
  | FileSystem.FileSystem
  | AsterRuntime
  | ContextRegistry
  | ContextQueries
  | AgentConversations
> {
  const nodeServer = createServer();
  const server = yield* NodeHttpServer.make(() => nodeServer, {
    host: "127.0.0.1",
    port: options.port ?? 4317,
    // Interrupt requests and drain their finalizers before awaiting the socket server's close.
    disablePreemptiveShutdown: true,
  });
  // platform-node removes its handler before draining request scopes. A reconnect
  // in that window has no request Fiber to end its response. After those scopes
  // drain, stop accepting connections and close remaining sockets before waiting
  // for Node's close callback; otherwise later runtime finalizers never run.
  yield* Effect.addFinalizer(() =>
    Effect.callback<void>((resume) => {
      nodeServer.close((error) => resume(error ? Effect.die(error) : Effect.void));
      nodeServer.closeAllConnections();
    }),
  );
  const url = NetAddress.formatUrlUnsafe(server.address);
  const rpc = ApiServer.layer.pipe(
    Layer.provide(RpcServer.layerProtocolHttp({ path: "/api/rpc", streamBufferSize: 64 })),
    Layer.provide(RpcSerialization.layerNdjson),
  );
  const routes = Layer.mergeAll(
    rpc,
    options.webSourceDir
      ? yield* sourceAssets(options.webSourceDir)
      : staticAssets(options.webDir, url),
  );
  const handler = yield* HttpRouter.toHttpEffect(routes);
  yield* server.serve(withHttpPolicy(url, handler));
  return { url };
});

export class LocalHttpApi extends Context.Service<LocalHttpApi, { readonly url: string }>()(
  "local/HttpApi",
) {
  static readonly layer = (options: { readonly source?: boolean } = {}) =>
    Layer.effect(
      LocalHttpApi,
      Effect.gen(function* () {
        const { projectRoot } = yield* ConfigLocation;
        const port = yield* Config.Port("port").pipe(
          Config.withDefault(4317),
          Config.nested("http"),
        );
        return yield* makeHttpApi({
          port,
          webDir: resolve(projectRoot, "apps/web/dist"),
          ...(options.source ? { webSourceDir: resolve(projectRoot, "apps/web") } : {}),
        });
      }),
    ).pipe(Layer.provide(NodeFileSystem.layer));
}
