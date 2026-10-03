import { Cron, Schema } from "effect";
import { SystemOneConfig } from "../decisions/system-one.js";

export const ModelConfig = Schema.Struct({
  name: Schema.String,
  provider: Schema.Literals(["minimax", "anthropic", "openai"]),
  model: Schema.String,
  url: Schema.String,
  apiKey: Schema.String,
});
export type ModelConfig = typeof ModelConfig.Type;

import { SignalSchedule, SignalAction } from "@aster/api-contracts";
export { SignalSchedule } from "@aster/api-contracts";
export const validateSignalTime = (signal: { schedule?: SignalSchedule; notBefore?: string }) => {
  const absolute = (value: string) => {
    if (!/(Z|[+-]\d{2}:\d{2})$/.test(value) || !Number.isFinite(Date.parse(value)))
      throw new Error("Time requires a valid absolute ISO timestamp with offset");
  };
  if (signal.notBefore) absolute(signal.notBefore);
  if (signal.schedule?.type === "once") absolute(signal.schedule.at);
  if (signal.schedule?.type === "cron")
    Cron.parseUnsafe(signal.schedule.expression, signal.schedule.timeZone);
};

export const SignalEntry = Schema.Struct({
  action: Schema.optional(SignalAction),
  when: Schema.String,
  schedule: Schema.optional(SignalSchedule),
  notBefore: Schema.optional(Schema.String),
  taskId: Schema.optional(Schema.String),
  task: Schema.String,
  agent: Schema.String,
  mode: Schema.Union([Schema.Literal("auto"), Schema.Literal("confirm")]),
});

export const GoalTitle = Schema.String.check(Schema.isPattern(/\S/));

const ConfigFile = Schema.Struct({
  config: Schema.optional(
    Schema.Struct({
      "system-one": Schema.optional(SystemOneConfig),
      models: Schema.optional(Schema.Array(ModelConfig)),
      agent: Schema.optional(Schema.Struct({ model: Schema.String })),
      goals: Schema.optional(
        Schema.Struct({
          model: Schema.String,
          contextTokens: Schema.optional(Schema.Number),
          reserveTokens: Schema.optional(Schema.Number),
        }),
      ),
    }),
  ),
  agents: Schema.optional(
    Schema.Struct({
      doubao: Schema.optional(Schema.Struct({ prompt: Schema.String })),
      pi: Schema.optional(
        Schema.Struct({
          model: Schema.NonEmptyString,
          storageDirectory: Schema.optional(Schema.NonEmptyString),
        }),
      ),
    }),
  ),
  contexts: Schema.optional(Schema.Record(Schema.String, Schema.Unknown)),
  signals: Schema.optional(Schema.Record(Schema.String, SignalEntry)),
  goals: Schema.optional(
    Schema.Record(
      Schema.String,
      Schema.Struct({
        title: Schema.optional(GoalTitle),
        description: Schema.String,
        completionCriteria: Schema.optional(Schema.String),
      }),
    ),
  ),
});

export const SignalDefinition = Schema.Struct({ slug: Schema.String, ...SignalEntry.fields });
export type SignalDefinition = typeof SignalDefinition.Type;

export interface CoreConfig {
  readonly config: NonNullable<typeof ConfigFile.Type.config>;
  readonly contexts: Readonly<Record<string, unknown>>;
  readonly agents?: typeof ConfigFile.Type.agents;
  readonly baseDir: string;
  readonly signals: ReadonlyArray<SignalDefinition>;
  readonly goals: ReadonlyArray<GoalDefinition>;
}

export interface GoalDefinition {
  readonly slug: string;
  readonly title?: string;
  readonly description: string;
  readonly completionCriteria?: string;
}

export const parseConfig = (input: unknown, baseDir: string): CoreConfig => {
  const parsed = Schema.decodeUnknownSync(ConfigFile)(input);
  const contexts = parsed.contexts ?? {};
  for (const root of Object.keys(contexts)) {
    if (!/^\/[^/]+$/.test(root) || root === "/." || root === "/..")
      throw new Error(`Invalid Context root: ${root}`);
  }
  const signals = Object.entries(parsed.signals ?? {}).map(([slug, signal]) => {
    if (!/^[a-z0-9][a-z0-9-]*$/.test(slug)) throw new Error(`Invalid Signal slug: ${slug}`);
    if (!signal.when.trim() || !signal.task.trim() || !signal.agent.trim()) {
      throw new Error(`Signal ${slug} has an empty field`);
    }
    validateSignalTime(signal);
    return { slug, ...signal };
  });
  const goals = Object.entries(parsed.goals ?? {}).map(([slug, goal]) => {
    if (!/^[a-z0-9][a-z0-9-]*$/.test(slug) || !goal.description.trim())
      throw new Error(`Invalid Goal: ${slug}`);
    return { slug, ...goal };
  });
  const models = parsed.config?.models ?? [];
  if (new Set(models.map((model) => model.name)).size !== models.length)
    throw new Error("Model names must be unique");
  if (parsed.config?.agent && !models.some((model) => model.name === parsed.config?.agent?.model))
    throw new Error("config.agent.model must reference a configured model");
  if (goals.length && !models.some((model) => model.name === parsed.config?.goals?.model))
    throw new Error("config.goals.model must reference a configured model");
  const budget = parsed.config?.goals;
  if (
    budget?.contextTokens !== undefined &&
    (!Number.isInteger(budget.contextTokens) || budget.contextTokens < 32000)
  )
    throw new Error("contextTokens must be an integer >= 32000");
  if (
    budget?.reserveTokens !== undefined &&
    (!Number.isInteger(budget.reserveTokens) ||
      budget.reserveTokens < 1024 ||
      budget.reserveTokens >= (budget.contextTokens ?? 48000) / 2)
  )
    throw new Error("Invalid reserveTokens");
  return { config: parsed.config ?? {}, agents: parsed.agents, contexts, baseDir, signals, goals };
};
