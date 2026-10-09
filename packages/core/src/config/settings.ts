import { Config, ConfigProvider, Context, Effect, Layer, Schema } from "effect";
import { configuredContexts, type ConfiguredContext } from "./context-directory.js";
import {
  GoalTitle,
  SignalEntry,
  validateSignalTime,
  type GoalDefinition,
  type SignalDefinition,
} from "./schema.js";

/** Bootstrap metadata, not a bag of domain configuration. */
export class ConfigLocation extends Context.Service<
  ConfigLocation,
  {
    readonly baseDir: string;
    readonly projectRoot: string;
    readonly envPath: string;
  }
>()("config/Location") {}

/** Captured launch environment. It is never part of a public Context. */
export class ProcessEnvironment extends Context.Service<
  ProcessEnvironment,
  {
    readonly values: NodeJS.ProcessEnv;
    readonly privateKeys: readonly string[];
  }
>()("config/ProcessEnvironment") {}

export const validateConfig = <A>(label: string, run: () => A) =>
  Effect.try({
    try: run,
    catch: (cause) =>
      new Config.ConfigError(
        new ConfigProvider.SourceError({
          message: `${label}: ${cause instanceof Error ? cause.message : String(cause)}`,
        }),
      ),
  });

const personalGoal: GoalDefinition = {
  slug: "personal",
  title: "Personal assistant",
  description:
    "Help the user handle everyday matters, answer questions, coordinate work and follow up on relevant changes. Use Contexts and memory as evidence, create Tasks and Signals when useful, and communicate clearly with the user.",
};

const GoalEntry = Schema.Struct({
  title: Schema.optional(GoalTitle),
  description: Schema.NonEmptyString,
});
const GoalOptions = Schema.Struct({
  model: Schema.NonEmptyString,
  contextTokens: Schema.optional(Schema.Int),
  reserveTokens: Schema.optional(Schema.Int),
});

export class GoalSettings extends Context.Service<
  GoalSettings,
  {
    readonly definitions: readonly GoalDefinition[];
    readonly reasoning?: typeof GoalOptions.Type;
    readonly contexts?: readonly ConfiguredContext[];
  }
>()("goals/Settings") {
  static readonly layer = Layer.effect(
    GoalSettings,
    Effect.gen(function* () {
      const contexts = yield* configuredContexts;
      const entries = yield* Config.schema(Schema.Record(Schema.String, GoalEntry), "goals").pipe(
        Config.withDefault({}),
      );
      const configuredReasoning = yield* Config.schema(Schema.optional(GoalOptions), [
        "config",
        "goals",
      ]);
      const reasoning = configuredReasoning ?? {
        model: yield* Config.NonEmptyString("model").pipe(
          Config.nested("agent"),
          Config.nested("config"),
        ),
      };
      return yield* validateConfig("Goals", () => {
        const definitions = Object.entries(entries).map(([slug, definition]) => {
          if (!/^[a-z0-9][a-z0-9-]*$/.test(slug) || !definition.description.trim())
            throw new Error(`Invalid Goal: ${slug}`);
          return { slug, ...definition };
        });
        if (definitions.length && !reasoning) throw new Error("config.goals.model is required");
        if (reasoning?.contextTokens !== undefined && reasoning.contextTokens < 32000)
          throw new Error("contextTokens must be >= 32000");
        if (
          reasoning?.reserveTokens !== undefined &&
          (reasoning.reserveTokens < 1024 ||
            reasoning.reserveTokens >= (reasoning.contextTokens ?? 200000) / 2)
        )
          throw new Error("Invalid reserveTokens");
        return {
          definitions: [personalGoal, ...definitions.filter((goal) => goal.slug !== "personal")],
          reasoning,
          contexts,
        };
      });
    }),
  );
}

export const signalSettings = Config.schema(
  Schema.Record(Schema.String, SignalEntry),
  "signals",
).pipe(
  Config.withDefault({}),
  Config.mapEffect((entries) =>
    validateConfig("Signals", (): readonly SignalDefinition[] =>
      Object.entries(entries).map(([slug, definition]) => {
        if (!/^[a-z0-9][a-z0-9-]*$/.test(slug)) throw new Error(`Invalid Signal: ${slug}`);
        if (definition.trigger._tag === "Schedule") validateSignalTime(definition.trigger);
        return { slug, ...definition };
      }),
    ),
  ),
);
