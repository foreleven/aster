import { Effect, Layer } from "effect";
import { MailSettings } from "./config.js";
import { MailFetcher, mailFetcherLayer } from "./client.js";

/** Configuration and transport services for the generic IMAP/POP3 mail source. */
const services = Layer.unwrap(
  Effect.gen(function* () {
    const settings = yield* MailSettings;
    return mailFetcherLayer(settings.mailboxes).pipe(Layer.provide(MailSettings.layer));
  }),
);

export const MailIntegration = {
  services,
  layer: services.pipe(Layer.provide(MailSettings.layer), Layer.merge(MailSettings.layer)),
};

export type MailIntegrationServices = typeof MailFetcher.Service;
