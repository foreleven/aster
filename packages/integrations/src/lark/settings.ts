import { Config, ConfigProvider, Context, Effect, Layer, Schema } from "effect";
import { LarkId } from "./paths.js";
import { RuntimeConfigurationError } from "@aster/core";

const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0));
const NonBlank = Schema.String.check(Schema.makeFilter((value) => value.trim().length > 0));
const ChildName = Schema.Literals(["/mail", "/im"]);

export const ChatPollingConfig = Schema.Struct({
  pollIntervalMs: PositiveInt.pipe(Schema.withDecodingDefaultKey(Effect.succeed(15 * 60_000))),
  catchUpWindowMs: PositiveInt.pipe(Schema.withDecodingDefaultKey(Effect.succeed(60 * 60_000))),
});

export const AgentAdmissionConfig = Schema.Struct({
  agentStartIntervalMs: PositiveInt.pipe(Schema.withDecodingDefaultKey(Effect.succeed(10_000))),
  agentConcurrency: PositiveInt.pipe(Schema.withDecodingDefaultKey(Effect.succeed(2))),
});

export const ChatSummaryBatchConfig = Schema.Struct({
  maxMessages: PositiveInt.pipe(Schema.withDecodingDefaultKey(Effect.succeed(200))),
});

export const ChatSummaryConfig = Schema.Struct({
  ...AgentAdmissionConfig.fields,
  ...ChatSummaryBatchConfig.fields,
  model: NonBlank,
});

export const ChatConfigEntry = Schema.Struct({
  description: Schema.optional(NonBlank),
  config: Schema.Struct({
    ...ChatPollingConfig.fields,
    summary: ChatSummaryConfig,
  }),
});

export interface LarkSettingsValue {
  readonly im?: typeof ChatConfigEntry.Type;
  readonly profile?: string;
  readonly description: string;
  readonly mail?: {
    readonly mailbox: string;
    readonly description: string;
    readonly pollIntervalMs: number;
  };
}

const LarkRootEntry = Schema.Struct({
  description: Schema.optional(NonBlank),
  config: Schema.optionalKey(Schema.Struct({ profile: Schema.optional(NonBlank) })),
  children: Schema.optional(Schema.Record(Schema.String, Schema.Unknown)),
});

const LarkEmailEntry = Schema.Struct({
  description: Schema.optional(NonBlank),
  config: Schema.optionalKey(
    Schema.Struct({
      mailbox: Schema.optional(LarkId),
      pollIntervalMs: Schema.optional(PositiveInt),
    }),
  ),
});

const LarkEntry = Schema.Struct({
  description: Schema.optional(NonBlank),
  config: Schema.optionalKey(Schema.Struct({ profile: Schema.optional(NonBlank) })),
  children: Schema.optionalKey(
    Schema.Struct({
      "/mail": Schema.optionalKey(LarkEmailEntry),
      "/im": Schema.optionalKey(ChatConfigEntry),
    }),
  ),
});

const normalize = (root: typeof LarkEntry.Type): LarkSettingsValue => {
  const mail = root.children?.["/mail"];
  const im = root.children?.["/im"];
  return {
    description: root.description ?? "My Lark account",
    profile: root.config?.profile,
    ...(im ? { im } : {}),
    ...(mail
      ? {
          mail: {
            mailbox: mail.config?.mailbox ?? "me",
            description: mail.description ?? "My Lark mailbox",
            pollIntervalMs: mail.config?.pollIntervalMs ?? 30_000,
          },
        }
      : {}),
  };
};
export const decodeLarkSettings = Effect.fn("LarkSettings.decode")(function* (input: unknown) {
  const root = yield* Schema.decodeUnknownEffect(LarkRootEntry)(input ?? {}).pipe(
    Effect.mapError(
      (error) =>
        new RuntimeConfigurationError({ message: `Invalid Lark settings: ${error.message}` }),
    ),
  );
  yield* Schema.decodeUnknownEffect(Schema.Array(ChildName))(Object.keys(root.children ?? {})).pipe(
    Effect.mapError(() => new RuntimeConfigurationError({ message: "Unsupported Lark child" })),
  );
  const entry = yield* Schema.decodeUnknownEffect(LarkEntry)(root).pipe(
    Effect.mapError(
      (error) =>
        new RuntimeConfigurationError({ message: `Invalid Lark settings: ${error.message}` }),
    ),
  );
  return normalize(entry);
});

export class LarkSettings extends Context.Service<LarkSettings, LarkSettingsValue>()(
  "lark/Settings",
) {
  static readonly layer = Layer.effect(
    LarkSettings,
    Effect.gen(function* () {
      const provider = yield* ConfigProvider.ConfigProvider;
      const children = yield* provider.load(["contexts", "/lark", "children"]);
      if (children?._tag === "Record")
        yield* Schema.decodeUnknownEffect(Schema.Array(ChildName))([...children.keys]).pipe(
          Effect.mapError(
            () => new RuntimeConfigurationError({ message: "Unsupported Lark child" }),
          ),
        );
      const entry = yield* Config.schema(LarkEntry, ["contexts", "/lark"]).pipe(
        Config.withDefault({}),
      );
      return normalize(entry);
    }),
  );
}
