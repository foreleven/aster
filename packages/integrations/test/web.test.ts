import assert from "node:assert/strict";
import { test } from "node:test";
import { ActorSystem } from "@aster/actor";
import {
  ContextQueries,
  ContextQueryError,
  ProcessEnvironment,
  RuntimeIntegrations,
} from "@aster/core";
import { Effect, Layer, Redacted, ConfigProvider, Schema } from "effect";
import { FetchHttpClient } from "effect/http";
import { WebSearchClient } from "../src/web/client.js";
import { TavilySearchPayload, TavilySearchResponse } from "../src/web/commands.js";
import { WebSearchIntegration } from "../src/web/integration.js";
import { WebSearchSettings } from "../src/web/settings.js";

const settings = {
  description: "Public web",
  endpoint: "https://api.tavily.com/search",
  apiKey: Redacted.make("private-tavily-key"),
} satisfies NonNullable<typeof WebSearchSettings.Service>;

const response = {
  query: "Effect 4 release",
  answer: null,
  results: [
    {
      title: "Effect",
      url: "https://effect.website",
      content: "A TypeScript library for building robust applications.",
      score: 0.99,
      raw_content: null,
      published_date: null,
    },
  ],
};

const httpLayer = (fetch: typeof globalThis.fetch) =>
  FetchHttpClient.layer.pipe(Layer.provide(Layer.succeed(FetchHttpClient.Fetch, fetch)));

test("Tavily client sends the documented search payload and decodes source results", async () => {
  let request: Request | undefined;
  const client = await Effect.runPromise(
    WebSearchClient.pipe(
      Effect.provide(
        WebSearchClient.layer.pipe(
          Layer.provide(Layer.succeed(WebSearchSettings, settings)),
          Layer.provide(
            httpLayer(async (input, init) => {
              request = new Request(input, init);
              return Response.json(response);
            }),
          ),
        ),
      ),
    ),
  );
  const result = await Effect.runPromise(
    client.search({ query: "Effect 4 release", maxResults: 3, topic: "general" }),
  );
  assert.deepEqual(result, response);
  assert.equal(request?.url, settings.endpoint);
  assert.deepEqual(await request?.json(), {
    api_key: "private-tavily-key",
    query: "Effect 4 release",
    max_results: 3,
    topic: "general",
  });
});

test("Tavily integration exposes a discoverable read-only /web Context", async () => {
  const calls: unknown[] = [];
  const client: WebSearchClient["Service"] = {
    search: (input) =>
      Effect.sync(() => {
        calls.push(input);
        return response;
      }),
  };
  await Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        yield* Layer.build(WebSearchIntegration.installation);
        const modules = yield* RuntimeIntegrations;
        const module = modules.installed()[0]!;
        const system = yield* ActorSystem.make().pipe(
          ActorSystem.provide(Layer.succeedContext(module.services)),
        );
        const handle = yield* module.activate(system);
        const queries = yield* ContextQueries;
        assert.equal((yield* queries.describe("/web")).commands[0]?.command, "search");
        const queryResult = yield* queries
          .query({
            path: "/web",
            command: "search",
            args: { query: "Effect 4", maxResults: 2 },
          })
          .pipe(
            Effect.match({
              onFailure: (error) => ({ failure: error }),
              onSuccess: (value) => ({ success: value }),
            }),
          );
        assert.deepEqual(queryResult, { success: response });
        assert.deepEqual(calls, [{ query: "Effect 4", maxResults: 2 }]);
        const invalid = yield* queries
          .query({ path: "/web", command: "search", args: { query: "", maxResults: 2 } })
          .pipe(Effect.flip);
        assert.equal(Schema.decodeUnknownSync(ContextQueryError)(invalid).kind, "invalid-input");
        assert.equal(calls.length, 1);
        yield* handle.stop;
        const unavailable = yield* queries
          .query({ path: "/web", command: "search", args: { query: "Effect" } })
          .pipe(Effect.flip);
        assert.equal(Schema.decodeUnknownSync(ContextQueryError)(unavailable).kind, "unavailable");
      }),
    ).pipe(
      Effect.provide(
        Layer.mergeAll(
          RuntimeIntegrations.layer,
          ContextQueries.layer,
          Layer.succeed(WebSearchSettings, settings),
          Layer.succeed(WebSearchClient, client),
        ),
      ),
    ),
  );
});

test("web settings resolve the captured API key environment variable", async () => {
  const layer = WebSearchSettings.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        ConfigProvider.layer(
          ConfigProvider.fromUnknown(
            { contexts: { "/web": { config: { apiKeyEnv: "TAVILY_TEST_KEY" } } } },
            { preserveEmptyStrings: true },
          ),
        ),
        Layer.succeed(ProcessEnvironment, {
          values: Redacted.make({ TAVILY_TEST_KEY: "private-tavily-key" }),
          privateKeys: ["TAVILY_TEST_KEY"],
        }),
      ),
    ),
  );
  const result = await Effect.runPromise(WebSearchSettings.pipe(Effect.provide(layer)));
  assert.equal(result?.description, "Search the public web with Tavily");
  assert.equal(Redacted.value(result!.apiKey), "private-tavily-key");
});

test("Tavily response schema rejects malformed results", () => {
  assert.throws(() =>
    Schema.decodeUnknownSync(TavilySearchResponse)({ results: [{ title: "missing" }] }),
  );
  assert.deepEqual(
    Schema.decodeUnknownSync(TavilySearchPayload)({ query: "current news", maxResults: 20 }),
    { query: "current news", maxResults: 20 },
  );
});
