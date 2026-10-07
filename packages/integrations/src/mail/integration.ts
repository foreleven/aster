import { Context, Deferred, Effect, Fiber, Layer, Schema, Stream } from "effect";
import { ContextRegistry, RuntimeIntegrations, defineIntegration } from "@aster/core";
import { MailSettings } from "./config.js";
import { MailFetcher, mailFetcherLayer } from "./client.js";
import { MailRootActor } from "./actors.js";
import { mailContextViews, mailboxPath, MailboxState } from "./contexts.js";

const services = Layer.unwrap(
  Effect.gen(function* () {
    return mailFetcherLayer((yield* MailSettings).mailboxes);
  }),
).pipe(Layer.provideMerge(MailSettings.layer));

/** Installation is separate from transport acquisition so hosts and tests can supply a fetcher. */
const installation = Layer.effectDiscard(
  Effect.gen(function* () {
    const settings = yield* MailSettings;
    const registry = yield* ContextRegistry;
    yield* registry.views.register(mailContextViews);
    if (settings.mailboxes.length === 0) return;
    const modules = yield* RuntimeIntegrations;
    const dependencies = Context.pick(
      MailSettings,
      MailFetcher,
      ContextRegistry,
    )(yield* Effect.context<MailSettings | MailFetcher | ContextRegistry>());
    yield* modules.register(
      defineIntegration({
        name: "mail",
        phase: "source",
        services: dependencies,
        activate: (system) =>
          Effect.gen(function* () {
            const ready = yield* Deferred.make<void>();
            const pending = new Set(settings.mailboxes.map((mailbox) => mailboxPath(mailbox.id)));
            // Only this activation's commits count; a persisted ready flag cannot satisfy startup.
            const changes = yield* registry.subscribe;
            const observer = yield* Stream.runForEach(changes, (change) =>
              Effect.gen(function* () {
                if (
                  pending.has(change.record.path) &&
                  Schema.is(MailboxState)(change.record.state) &&
                  change.record.state.status === "ready"
                ) {
                  pending.delete(change.record.path);
                  if (pending.size === 0) yield* Deferred.succeed(ready, undefined);
                }
              }),
            ).pipe(Effect.forkScoped);
            const actor = yield* system.spawn("mail", MailRootActor);
            return {
              ready: Deferred.await(ready),
              stop: system.stop(actor).pipe(Effect.ensuring(Fiber.interrupt(observer))),
            };
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
