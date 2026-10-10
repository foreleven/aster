import { Config, ConfigProvider, Context, Effect, Layer, Schema } from "effect";
import { GoalTitle, type GoalDefinition } from "./contracts.js";

const Slug = Schema.String.check(Schema.isPattern(/^[a-z0-9][a-z0-9-]*$/));
const GoalEntry = Schema.Struct({
  title: Schema.optional(GoalTitle),
  description: Schema.String.check(Schema.isPattern(/\S/)),
});
export const GoalReasoningConfig = Schema.Struct({
  model: Schema.NonEmptyString,
  contextTokens: Schema.Int.check(Schema.isGreaterThanOrEqualTo(32000)).pipe(
    Schema.withDecodingDefaultKey(Effect.succeed(200000)),
  ),
  reserveTokens: Schema.Int.check(Schema.isGreaterThanOrEqualTo(1024)).pipe(
    Schema.withDecodingDefaultKey(Effect.succeed(8192)),
  ),
}).check(
  Schema.makeFilter((value) => value.reserveTokens < value.contextTokens / 2, {
    expected: "reserveTokens must be less than half of contextTokens",
  }),
);
const personalGoal: GoalDefinition = {
  slug: "personal",
  title: "Personal assistant",
  description:
    "Help the user handle everyday matters, answer questions, coordinate work and follow up on relevant changes. Use Contexts and memory as evidence, create Tasks and Signals when useful, and communicate clearly with the user.",
};
export class GoalSettings extends Context.Service<
  GoalSettings,
  {
    readonly definitions: readonly GoalDefinition[];
    readonly reasoning: typeof GoalReasoningConfig.Type;
  }
>()("goals/Settings") {
  static readonly layer = Layer.effect(
    GoalSettings,
    Effect.gen(function* () {
      const entries = yield* Config.schema(Schema.Record(Slug, GoalEntry), "goals").pipe(
        Config.withDefault({}),
      );
      const provider = yield* ConfigProvider.ConfigProvider;
      const reasoning = (yield* provider.load(["config", "goals"]))
        ? yield* Config.schema(GoalReasoningConfig, ["config", "goals"])
        : {
            model: yield* Config.NonEmptyString("model").pipe(
              Config.nested("agent"),
              Config.nested("config"),
            ),
            contextTokens: 200000,
            reserveTokens: 8192,
          };
      const definitions = Object.entries(entries).map(([slug, entry]) => ({ slug, ...entry }));
      return {
        definitions: [personalGoal, ...definitions.filter((goal) => goal.slug !== "personal")],
        reasoning,
      };
    }),
  );
}
