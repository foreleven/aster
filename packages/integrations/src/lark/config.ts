import { Config, ConfigProvider, Context, Effect, Layer, Schema } from "effect";
import { validateConfig } from "@aster/core";
import { parseImPolicy } from "./im/policy.js";
export interface LarkIntegrationConfig {
  readonly im?: typeof LarkImEntry.Type;
  readonly profile?: string;
  readonly description: string;
  readonly mail: {
    readonly mailbox: string;
    readonly description: string;
    readonly pollIntervalMs: number;
  };
}

const LarkRootEntry = Schema.Struct({
  description: Schema.optional(Schema.String),
  config: Schema.optionalKey(Schema.Struct({ profile: Schema.optional(Schema.String) })),
  children: Schema.optional(Schema.Record(Schema.String, Schema.Unknown)),
});

const LarkEmailEntry = Schema.Struct({
  description: Schema.optional(Schema.String),
  config: Schema.optionalKey(
    Schema.Struct({
      mailbox: Schema.optional(Schema.String),
      pollIntervalMs: Schema.optional(Schema.Number),
    }),
  ),
});

const LarkImEntry = Schema.Struct({
  description: Schema.optional(Schema.String),
  config: Schema.optionalKey(
    Schema.Struct({
      pollIntervalMs: Schema.optional(Schema.Int),
      catchUpWindowMs: Schema.optional(Schema.Int),
      summary: Schema.optionalKey(
        Schema.Struct({
          model: Schema.optional(Schema.NonEmptyString),
          agentStartIntervalMs: Schema.optional(Schema.Int),
          agentConcurrency: Schema.optional(Schema.Int),
        }),
      ),
    }),
  ),
});
const LarkEntry = Schema.Struct({
  description: Schema.optional(Schema.String),
  config: Schema.optionalKey(Schema.Struct({ profile: Schema.optional(Schema.String) })),
  children: Schema.optionalKey(
    Schema.Struct({
      "/mail": Schema.optionalKey(LarkEmailEntry),
      "/im": Schema.optionalKey(LarkImEntry),
    }),
  ),
});

export const parseLarkConfig = (entry: unknown): LarkIntegrationConfig => {
  const root = Schema.decodeUnknownSync(LarkRootEntry)(entry ?? {});
  const children = root.children ?? {};
  if (Object.keys(children).some((path) => path !== "/mail" && path !== "/im")) {
    throw new Error(
      `Unsupported Lark child: ${Object.keys(children).find((path) => path !== "/mail" && path !== "/im")}`,
    );
  }
  const mail = Schema.decodeUnknownSync(LarkEmailEntry)(children["/mail"] ?? {});
  const description = root.description ?? "My Lark account";
  const mailDescription = mail.description ?? "My Lark mailbox";
  const mailbox = mail.config?.mailbox ?? "me";
  if (!description.trim() || !mailDescription.trim())
    throw new Error("Lark Context descriptions must be nonempty");
  if (!mailbox.trim() || root.config?.profile === "")
    throw new Error("Lark profile and mailbox must be nonempty");
  const pollIntervalMs = mail.config?.pollIntervalMs ?? 30_000;
  if (!Number.isSafeInteger(pollIntervalMs) || pollIntervalMs <= 0) {
    throw new Error("Lark email pollIntervalMs must be a positive integer");
  }
  const im =
    children["/im"] === undefined
      ? undefined
      : Schema.decodeUnknownSync(LarkImEntry)(children["/im"]);
  if (im) parseImPolicy(im);
  return {
    ...(im === undefined ? {} : { im }),
    profile: root.config?.profile,
    description,
    mail: { mailbox, description: mailDescription, pollIntervalMs },
  };
};

export class LarkConfig extends Context.Service<LarkConfig, LarkIntegrationConfig>()(
  "lark/Config",
) {
  static readonly layer = Layer.effect(
    LarkConfig,
    Effect.gen(function* () {
      const provider = yield* ConfigProvider.ConfigProvider;
      const children = yield* provider.load(["contexts", "/lark", "children"]);
      if (children?._tag === "Record")
        yield* validateConfig("Lark", () => {
          for (const key of children.keys)
            if (key !== "/mail" && key !== "/im") throw new Error(`Unsupported Lark child: ${key}`);
        });
      const entry = yield* Config.schema(LarkEntry, ["contexts", "/lark"]).pipe(
        Config.withDefault({}),
      );
      return yield* validateConfig("Lark", () => parseLarkConfig(entry));
    }),
  );
}
