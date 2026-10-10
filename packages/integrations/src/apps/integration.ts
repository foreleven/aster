import { Context, Effect, Layer } from "effect";
import {
  ContextQueries,
  DurableContext,
  RuntimeIntegrations,
  defineIntegration,
} from "@aster/core";
import { AppsSettings } from "./config.js";
import { OpenCli } from "./client.js";
import { AppsRootActor } from "./actors.js";

const installation = Layer.effectDiscard(
  Effect.gen(function* () {
    const settings = yield* AppsSettings;

    if (settings.apps.length === 0) return;
    const modules = yield* RuntimeIntegrations;
    const services = Context.pick(
      AppsSettings,
      OpenCli,
      ContextQueries,
      DurableContext,
    )(yield* Effect.context<AppsSettings | OpenCli | ContextQueries | DurableContext>());
    yield* modules.register(
      defineIntegration({
        name: "apps",
        phase: "source",
        services,
        activate: (system) =>
          Effect.gen(function* () {
            const actor = yield* system.spawn("apps", AppsRootActor);
            yield* actor.awaitStarted;
            return { stop: system.stop(actor) };
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
