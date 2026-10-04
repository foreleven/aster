import { Config, Context, Effect, Layer, Option, Schema } from "effect";
import { RuntimeConfigurationError } from "@aster/core";
import { MailConfig } from "./model.js";

export class MailSettings extends Context.Service<
  MailSettings,
  typeof MailConfig.Type & { readonly description?: string }
>()("mail/Config") {
  static readonly layer = Layer.effect(
    MailSettings,
    Effect.gen(function* () {
      // Read the config directly: an optional enclosing object can hide invalid nested input.
      const entry = yield* Config.schema(MailConfig, ["contexts", "/mail", "config"]).pipe(
        Config.option,
      );
      const description = yield* Config.schema(
        // Description remains separate from credentials and transport configuration.
        Schema.String,
        ["contexts", "/mail", "description"],
      ).pipe(Config.withDefault("Connected mailboxes"));
      if (Option.isNone(entry)) return { mailboxes: [], description };
      const config = entry.value;
      if (config.mailboxes.length === 0)
        return yield* new RuntimeConfigurationError({
          message: "Mail requires at least one mailbox",
        });
      if (new Set(config.mailboxes.map((mailbox) => mailbox.id)).size !== config.mailboxes.length)
        return yield* new RuntimeConfigurationError({ message: "Mail mailbox IDs must be unique" });
      return { ...config, description };
    }),
  );
}
