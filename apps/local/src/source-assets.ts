import { Data, Effect, Layer, Scope } from "effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/unstable/http";
import { NodeHttpServerRequest } from "@effect/platform-node";
import { resolve } from "node:path";
import { json } from "./http-policy.js";

class SourceAssetsError extends Data.TaggedError("SourceAssetsError")<{
  readonly cause: unknown;
}> {}

/** Vite owns transforms/watchers; the HTTP Scope releases them before the listener closes. */
export const sourceAssets: (
  root: string,
) => Effect.Effect<
  Layer.Layer<never, SourceAssetsError, HttpRouter.HttpRouter | Scope.Scope>,
  SourceAssetsError,
  Scope.Scope
> = Effect.fn("LocalHttpApi.sourceAssets")(function* (root: string) {
  const vite = yield* Effect.acquireRelease(
    Effect.tryPromise({
      // Vite has no AbortSignal API. Uninterruptible acquisition pairs a late server with close().
      try: async () => {
        const { createServer } = await import("vite");
        return createServer({
          root,
          configFile: resolve(root, "vite.config.js"),
          server: { middlewareMode: true, hmr: false },
        });
      },
      catch: (cause) => new SourceAssetsError({ cause }),
    }),
    (server) => Effect.promise(() => server.close()),
  );
  return HttpRouter.addAll([
    HttpRouter.route(
      "GET",
      "/*",
      Effect.gen(function* () {
        const request = yield* HttpServerRequest.HttpServerRequest;
        if (request.url.split("?")[0]!.startsWith("/api/"))
          return json({ error: "Not found" }, 404);
        // Connect requires native Node request/response objects. Effect retains request ownership;
        // platform-node skips writing a second response after Vite has finished it.
        const incoming = NodeHttpServerRequest.toIncomingMessage(request);
        const response = NodeHttpServerRequest.toServerResponse(request);
        return yield* Effect.callback<HttpServerResponse.HttpServerResponse, SourceAssetsError>(
          (resume) => {
            const finished = () => resume(Effect.succeed(HttpServerResponse.empty()));
            const closed = () => resume(Effect.interrupt);
            response.once("finish", finished);
            response.once("close", closed);
            vite.middlewares(incoming, response, (cause?: unknown) => {
              resume(
                cause === undefined
                  ? Effect.succeed(json({ error: "Not found" }, 404))
                  : Effect.fail(new SourceAssetsError({ cause })),
              );
            });
            return Effect.sync(() => {
              response.off("finish", finished);
              response.off("close", closed);
            });
          },
        ).pipe(
          Effect.catchTag("SourceAssetsError", () =>
            Effect.succeed(json({ error: "Source transform failed" }, 500)),
          ),
        );
      }),
    ),
  ]);
});
