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
export interface DecisionQuestion {
  readonly type: "choice";
  readonly instructions: string;
  readonly criteria: Readonly<Record<string, string>>;
}
export const choice = (
  instructions: string,
  criteria: Readonly<Record<string, string>>,
): DecisionQuestion => ({ type: "choice", instructions, criteria });
/** Adapters translate SDK failures and forward fiber interruption to the transport. */
export interface SystemOneClient {
  readonly configured?: boolean;
  readonly systemOne: (request: {
    readonly state: string | Readonly<Record<string, unknown>>;
    readonly questions: Readonly<Record<string, DecisionQuestion>>;
  }) => Effect.Effect<
    {
      readonly answers: Readonly<
        Record<string, { readonly type: string; readonly choice?: string }>
      >;
    },
    DecisionError
  >;
}

/** Global decision transport; each domain owns its questions and policy. */
export const SystemOneClient = Context.Service<SystemOneClient>("decisions/SystemOneClient");
