import { Context, Effect, Layer } from "effect";
import {
  ContextQueries,
  DurableContext,
  RuntimeIntegrations,
  defineIntegration,
} from "@aster/core";
import { MailSettings } from "./settings.js";
import { MailFetcher, mailFetcherLayer } from "./client.js";
import { MailRootActor } from "./actors.js";

const services = mailFetcherLayer().pipe(Layer.provideMerge(MailSettings.layer));

/** Installation is separate from transport acquisition so hosts and tests can supply a fetcher. */
const installation = Layer.effectDiscard(
  Effect.gen(function* () {
    const settings = yield* MailSettings;
    if (settings.mailboxes.length === 0) return;
    const modules = yield* RuntimeIntegrations;
    const dependencies = Context.pick(
      MailSettings,
      MailFetcher,
      ContextQueries,
      DurableContext,
    )(yield* Effect.context<MailSettings | MailFetcher | DurableContext | ContextQueries>());
    yield* modules.register(
      defineIntegration({
        name: "mail",
        phase: "source",
        services: dependencies,
        activate: (system) =>
          Effect.gen(function* () {
            const actor = yield* system.spawn("mail", MailRootActor);
            yield* actor.awaitStarted;
            return { stop: system.stop(actor) };
          }),
      }),
    );
  }),
);

export const MailIntegration = {
  services,
  installation,
  layer: installation.pipe(Layer.provide(services)),
};

export type MailIntegrationServices = typeof MailFetcher.Service;
