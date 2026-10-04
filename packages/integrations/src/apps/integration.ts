import { Context, Effect, Layer } from "effect";
import {
  ContextQueries,
  ContextRegistry,
  RuntimeIntegrations,
  defineIntegration,
} from "@aster/core";
import { AppsSettings } from "./config.js";
import { OpenCli } from "./client.js";
import { AppsRootActor } from "./actors.js";
import { appsView, appView } from "./contexts.js";

const installation = Layer.effectDiscard(
  Effect.gen(function* () {
    const settings = yield* AppsSettings;
    const registry = yield* ContextRegistry;
    yield* registry.registerViews([appsView, appView]);
    if (settings.apps.length === 0) return;
    const modules = yield* RuntimeIntegrations;
    const services = Context.pick(
      AppsSettings,
      OpenCli,
      ContextQueries,
      ContextRegistry,
    )(yield* Effect.context<AppsSettings | OpenCli | ContextQueries | ContextRegistry>());
    yield* modules.register(
      defineIntegration({
        name: "apps",
        phase: "source",
        services,
        activate: (system) =>
          Effect.gen(function* () {
            const actor = yield* system.spawn("apps", AppsRootActor);
            return {
              ready: actor.ask<void>((replyTo) => ({ _tag: "Ready", replyTo })),
              stop: system.stop(actor),
            };
          }),
      }),
    );
  }),
);
const services = Layer.merge(AppsSettings.layer, OpenCli.layer);
export const AppsIntegration = {
  installation,
  services,
  layer: installation.pipe(Layer.provide(services)),
};
