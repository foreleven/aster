import { ContextQueryError } from "@aster/core";
import { Context, Effect, Layer, Redacted, Schema } from "effect";
import { HttpClient, HttpClientRequest } from "effect/http";
import {
  TavilySearchPayload,
  TavilySearchResponse,
  type TavilySearchResponse as SearchResponse,
} from "./commands.js";
import { WebSearchSettings } from "./settings.js";

const requestBody = (request: typeof TavilySearchPayload.Type, apiKey: string) => ({
  api_key: apiKey,
  query: request.query,
  ...(request.maxResults === undefined ? {} : { max_results: request.maxResults }),
  ...(request.searchDepth === undefined ? {} : { search_depth: request.searchDepth }),
  ...(request.topic === undefined ? {} : { topic: request.topic }),
  ...(request.timeRange === undefined ? {} : { time_range: request.timeRange }),
});

export class WebSearchClient extends Context.Service<
  WebSearchClient,
  {
    readonly search: (
      request: typeof TavilySearchPayload.Type,
    ) => Effect.Effect<SearchResponse, ContextQueryError>;
  }
>()("web-search/Client") {
  static readonly layer = Layer.effect(
    WebSearchClient,
    Effect.gen(function* () {
      const settings = yield* WebSearchSettings;
      if (!settings)
        return {
          search: () =>
            Effect.fail(
              new ContextQueryError({
                kind: "unavailable",
                message: "Web search is not configured",
              }),
            ),
        };
      const client = yield* HttpClient.HttpClient;
      return {
        search: Effect.fn("Tavily.search")(function* (request: typeof TavilySearchPayload.Type) {
          const response = yield* Effect.scoped(
            Effect.gen(function* () {
              const input = HttpClientRequest.post(settings.endpoint).pipe(
                HttpClientRequest.bodyJsonUnsafe(
                  requestBody(request, Redacted.value(settings.apiKey)),
                ),
                HttpClientRequest.setHeader("Accept", "application/json"),
              );
              const response = yield* client.execute(input);
              if (response.status < 200 || response.status >= 300)
                return yield* new ContextQueryError({
                  kind: response.status === 429 ? "busy" : "failed",
                  message:
                    response.status === 401 || response.status === 403
                      ? "Tavily rejected the configured API key"
                      : `Tavily returned HTTP ${response.status}`,
                });
              const body = yield* response.json;
              return yield* Schema.decodeUnknownEffect(TavilySearchResponse)(body).pipe(
                Effect.mapError(
                  () =>
                    new ContextQueryError({
                      kind: "failed",
                      message: "Tavily returned an invalid search response",
                    }),
                ),
              );
            }),
          ).pipe(
            Effect.timeout("30 seconds"),
            Effect.catchTag("TimeoutError", () =>
              Effect.fail(
                new ContextQueryError({
                  kind: "timeout",
                  message: "Tavily search timed out after 30 seconds",
                }),
              ),
            ),
            Effect.catchTag("HttpClientError", () =>
              Effect.fail(
                new ContextQueryError({
                  kind: "unavailable",
                  message: "Tavily search transport is unavailable",
                }),
              ),
            ),
          );
          return response;
        }),
      };
    }),
  );
}
