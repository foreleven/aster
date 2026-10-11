import { Context, Effect, Exit, Layer, Schema, Scope } from "effect";
import {
  ContextQueries,
  ContextQueryError,
  RuntimeIntegrations,
  defineIntegration,
} from "@aster/core";
import { TavilySearch, TavilySearchPayload } from "./commands.js";
import { WebSearchClient } from "./client.js";
import { WebSearchSettings } from "./settings.js";

const installation = Layer.effectDiscard(
  Effect.gen(function* () {
    const settings = yield* WebSearchSettings;
    if (!settings) return;
    const client = yield* WebSearchClient;
    const queries = yield* ContextQueries;
    const modules = yield* RuntimeIntegrations;
    const services = Context.make(WebSearchSettings, settings).pipe(
      Context.add(WebSearchClient, client),
      Context.add(ContextQueries, queries),
    );
    yield* modules.register(
      defineIntegration({
        name: "web-search",
        phase: "source",
        services,
        activate: () =>
          Effect.gen(function* () {
            const routeScope = yield* Effect.acquireRelease(Scope.make(), (scope, exit) =>
              Scope.close(scope, exit),
            );
            yield* queries
              .register(
                "/web",
                {
                  description: settings.description,
                  commands: {
                    search: {
                      description: TavilySearch.description,
                      schema: TavilySearchPayload,
                      success: TavilySearch.successSchema,
                      error: TavilySearch.errorSchema,
                      text: (value) => JSON.stringify(value) ?? "",
                    },
                  },
                },
                (input) =>
                  Schema.decodeUnknownEffect(TavilySearchPayload)(input.args).pipe(
                    Effect.mapError(
                      () =>
                        new ContextQueryError({
                          kind: "invalid-input",
                          message: "Invalid Tavily search arguments",
                        }),
                    ),
                    Effect.flatMap((request) => client.search(request)),
                  ),
              )
              .pipe(Effect.provideService(Scope.Scope, routeScope));
            return { stop: Scope.close(routeScope, Exit.void) };
          }),
      }),
    );
  }),
);

const services = Layer.unwrap(
  Effect.gen(function* () {
    const settings = yield* WebSearchSettings;
    return Layer.merge(
      Layer.succeed(WebSearchSettings, settings),
      WebSearchClient.layer.pipe(Layer.provide(Layer.succeed(WebSearchSettings, settings))),
    );
  }).pipe(Effect.provide(WebSearchSettings.layer)),
);

export const WebSearchIntegration = {
  services,
  installation,
  layer: installation.pipe(Layer.provide(services)),
};
