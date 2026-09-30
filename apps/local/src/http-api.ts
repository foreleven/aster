import { Config, Context, Effect, Exit, Layer, Scope } from "effect";
import { NodeFileSystem, NodeHttpServer } from "@effect/platform-node";
import { HttpRouter } from "effect/unstable/http";
import { NetAddress } from "effect/unstable/net";
import { RpcSerialization, RpcServer } from "effect/unstable/rpc";
import { ApplicationRpcs } from "@aster/api-contracts";
import { createServer } from "node:http";
import { resolve } from "node:path";
import { AsterRuntime, ConfigLocation, type ApplicationApi } from "@aster/core";
import { applicationRpcHandlers } from "./rpc-api.js";
import { eventResponse } from "./http-events.js";
import { withHttpPolicy } from "./http-policy.js";
import { legacyRest } from "./legacy-rest.js";
import { staticAssets } from "./static-assets.js";

export interface GoalApi {
  readonly url: string;
  close(): Promise<void>;
}
interface ApiOptions {
  readonly application: ApplicationApi;
  readonly port?: number;
  readonly webDir?: string;
}

/** Host owns transport scopes. RPC and legacy REST share the same application operations. */
export const makeGoalApi = Effect.fn("LocalHttpApi.make")(function* (options: ApiOptions) {
  const { application } = options;
  const server = yield* NodeHttpServer.make(createServer, {
    host: "127.0.0.1",
    port: options.port ?? 4317,
    // Interrupt requests and drain their finalizers before awaiting the socket server's close.
    disablePreemptiveShutdown: true,
  });
  const url = NetAddress.formatUrlUnsafe(server.address);
  const rpc = RpcServer.layerHttp({
    group: ApplicationRpcs,
    path: "/api/rpc",
    protocol: "http",
  }).pipe(
    Layer.provide(applicationRpcHandlers(application)),
    Layer.provide(RpcSerialization.layerNdjson),
  );
  const routes = Layer.mergeAll(
    rpc,
    legacyRest(application, url),
    staticAssets(options.webDir, url),
    HttpRouter.addAll([HttpRouter.route("GET", "/api/events", eventResponse(application))]),
  );
  const handler = yield* HttpRouter.toHttpEffect(routes);
  yield* server.serve(withHttpPolicy(url, handler));
  return { url };
});

/** Promise entry point for embedders/tests. Production composes makeGoalApi directly into its Scope. */
export const startGoalApi = async (options: ApiOptions): Promise<GoalApi> => {
  const scope = await Effect.runPromise(Scope.make());
  try {
    const api = await Effect.runPromise(
      makeGoalApi(options).pipe(
        Effect.provideService(Scope.Scope, scope),
        Effect.provide(NodeFileSystem.layer),
      ),
    );
    return { ...api, close: () => Effect.runPromise(Scope.close(scope, Exit.void)) };
  } catch (error) {
    await Effect.runPromise(Scope.close(scope, Exit.void));
    throw error;
  }
};
export class LocalHttpApi extends Context.Service<LocalHttpApi, { readonly url: string }>()(
  "local/HttpApi",
) {
  static readonly layer = Layer.effect(
    LocalHttpApi,
    Effect.gen(function* () {
      const runtime = yield* AsterRuntime;
      const { projectRoot } = yield* ConfigLocation;
      const port = yield* Config.Port("port").pipe(Config.withDefault(4317), Config.nested("http"));
      return yield* makeGoalApi({
        application: runtime.api,
        port,
        webDir: resolve(projectRoot, "apps/web/dist"),
      });
    }),
  ).pipe(Layer.provide(NodeFileSystem.layer));
}
