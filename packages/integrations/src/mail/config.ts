import { Config, Context, Effect, Layer, Schema } from "effect";
import { validateConfig } from "@aster/core";
import { MailConfig } from "./model.js";

const MailContextEntry = Schema.Struct({
  description: Schema.optional(Schema.String),
  config: Schema.optional(MailConfig),
});

export class MailSettings extends Context.Service<MailSettings, typeof MailConfig.Type>()(
  "mail/Config",
) {
  static readonly layer = Layer.effect(
    MailSettings,
    Effect.gen(function* () {
      const entry = yield* Config.schema(MailContextEntry, ["contexts", "/mail"]).pipe(
        Config.withDefault({ config: undefined }),
      );
      const config = entry.config ?? { mailboxes: [] };
      return yield* validateConfig("Mail", () => {
        if (config.mailboxes.length === 0) throw new Error("Mail requires at least one mailbox");
        return config;
      });
    }),
  );
}
