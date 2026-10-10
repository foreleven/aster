import { Config, ConfigProvider, Context, Effect, Layer, Schema } from "effect";
import { RuntimeConfigurationError } from "@aster/core";
import { MailConfig } from "./model.js";

export class MailSettings extends Context.Service<
  MailSettings,
  typeof MailConfig.Type & { readonly description: string; readonly pollIntervalMs: number }
>()("mail/Config") {
  static readonly layer = Layer.effect(
    MailSettings,
    Effect.gen(function* () {
      const provider = yield* ConfigProvider.ConfigProvider;
      const configured = yield* provider.load(["contexts", "/mail", "config"]);
      const description = yield* Config.schema(
        // Description remains separate from credentials and transport configuration.
        Schema.String,
        ["contexts", "/mail", "description"],
      ).pipe(Config.withDefault("Connected mailboxes"));
      if (!configured) return { mailboxes: [], description, pollIntervalMs: 30_000 };
      const config = yield* Config.schema(MailConfig, ["contexts", "/mail", "config"]);
      if (config.mailboxes.length === 0)
        return yield* new RuntimeConfigurationError({
          message: "Mail requires at least one mailbox",
        });
      if (new Set(config.mailboxes.map((mailbox) => mailbox.id)).size !== config.mailboxes.length)
        return yield* new RuntimeConfigurationError({ message: "Mail mailbox IDs must be unique" });
      return { ...config, description, pollIntervalMs: config.pollIntervalMs ?? 30_000 };
    }),
  );
}
