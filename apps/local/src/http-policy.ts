import { ByteSize, Effect } from "effect";
import { HttpIncomingMessage, HttpServerRequest, HttpServerResponse } from "effect/http";

export const json = (body: unknown, status = 200) =>
  HttpServerResponse.jsonUnsafe(body, {
    status,
    headers: { "cache-control": "no-store" },
  });
/** One policy wraps RPC, REST, SSE and static files. Platform reads also enforce the streaming body limit. */
export const withHttpPolicy = <E, R>(
  url: string,
  handler: Effect.Effect<HttpServerResponse.HttpServerResponse, E, R>,
) => {
  const host = new URL(url).host;
  return Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    if (request.headers.host !== host) return json({ error: "Invalid host" }, 403);
    if (request.headers.origin && request.headers.origin !== url)
      return json({ error: "Invalid origin" }, 403);
    if (Number(request.headers["content-length"]) > 32 * 1024)
      return json({ error: "Request body too large" }, 413);
    return yield* handler;
  }).pipe(Effect.provideService(HttpIncomingMessage.MaxBodySize, ByteSize.kibibytes(32)));
};
