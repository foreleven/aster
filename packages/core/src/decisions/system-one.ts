import { Context, Data, Effect, Schema } from "effect";

export class DecisionError extends Data.TaggedError("DecisionError")<{
  readonly message: string;
  readonly cause?: unknown;
}> {}

export const SystemOneConfig = Schema.Struct({
  url: Schema.String,
  model: Schema.String,
  apiKey: Schema.String,
});
export type SystemOneConfig = typeof SystemOneConfig.Type;
export type DecisionQuestion = DecisionChoiceQuestion | DecisionScoreQuestion;
export interface DecisionChoiceQuestion {
  readonly type: "choice";
  readonly instructions: string;
  readonly criteria: Readonly<Record<string, string>>;
}
export interface DecisionScoreQuestion {
  readonly type: "score";
  readonly instructions: string;
  readonly criteria: readonly [string, string, ...string[]];
}
export const choice = (
  instructions: string,
  criteria: Readonly<Record<string, string>>,
): DecisionQuestion => ({ type: "choice", instructions, criteria });
export const score = (
  instructions: string,
  criteria: readonly [string, string, ...string[]],
): DecisionQuestion => ({ type: "score", instructions, criteria });
/** Adapters translate SDK failures and forward fiber interruption to the transport. */
export interface SystemOneClient {
  readonly configured?: boolean;
  readonly systemOne: (request: {
    readonly state: string | Readonly<Record<string, unknown>>;
    readonly questions: Readonly<Record<string, DecisionQuestion>>;
  }) => Effect.Effect<
    {
      readonly answers: Readonly<
        Record<
          string,
          {
            readonly type: string;
            readonly choice?: string;
            readonly score?: number;
            readonly confidence?: number;
            readonly legend?: Readonly<Record<string, unknown>>;
          }
        >
      >;
    },
    DecisionError
  >;
}

/** Global decision transport; each domain owns its questions and policy. */
export const SystemOneClient = Context.Service<SystemOneClient>("decisions/SystemOneClient");
