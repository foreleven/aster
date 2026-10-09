import { Effect, Schema } from "effect";

const PositiveInt = Schema.Int.check(Schema.isGreaterThan(0));

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
  model: Schema.NonEmptyString,
});

export const ChatConfigEntry = Schema.Struct({
  description: Schema.optional(Schema.String),
  config: Schema.optionalKey(
    Schema.Struct({
      ...ChatPollingConfig.fields,
      summary: Schema.optionalKey(
        Schema.Struct({
          ...ChatSummaryConfig.fields,
          // The summarizer requires a model when acquired; keep incomplete entries visible here.
          model: Schema.optional(ChatSummaryConfig.fields.model),
        }),
      ),
    }),
  ),
});
