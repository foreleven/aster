import { Match, Schema } from "effect";
import { type ExecutionStatus, type InputRequest } from "@aster/core";

const RawOption = Schema.Struct({
  option_id: Schema.String,
  text: Schema.optional(Schema.String),
  title: Schema.optional(Schema.String),
});
const RawQuestion = Schema.Struct({
  question_id: Schema.String,
  title: Schema.optional(Schema.String),
  question: Schema.optional(Schema.String),
  type: Schema.optional(Schema.Number),
  options: Schema.optional(Schema.Array(RawOption)),
});
const RawPending = Schema.Struct({
  threadId: Schema.optional(Schema.String),
  messageId: Schema.String,
  blockId: Schema.optional(Schema.String),
  clarifyId: Schema.optional(Schema.String),
  kind: Schema.Literals(["approval", "input"]),
  items: Schema.optional(Schema.Unknown),
  questions: Schema.optional(Schema.Array(RawQuestion)),
});
const RawStatus = Schema.Struct({
  status: Schema.String,
  reply: Schema.optional(Schema.Struct({ text: Schema.optional(Schema.String) })),
  // Keep native metadata intact, including nested fields outside our display contract.
  pending: Schema.optional(Schema.Array(Schema.Record(Schema.String, Schema.Unknown))),
  error: Schema.optional(Schema.Unknown),
});
export const decodeDoubaoReceipt = Schema.decodeUnknownSync(
  Schema.Struct({
    conversationId: Schema.NonEmptyString,
    runId: Schema.NonEmptyString,
  }),
);

const question = (value: typeof RawQuestion.Type) => ({
  id: value.question_id,
  prompt: value.title ?? value.question ?? "Additional information",
  ...Match.value(value.type).pipe(
    Match.when(1, () => ({ allowOther: false, multiple: false })),
    Match.when(2, () => ({ allowOther: false, multiple: true })),
    Match.orElse(() => ({})),
  ),
  ...(value.options
    ? { options: value.options.map((option) => option.text ?? option.title ?? option.option_id) }
    : {}),
});
const request = (metadata: Record<string, unknown>): InputRequest => {
  const value = Schema.decodeUnknownSync(RawPending)(metadata);
  const base = {
    id: [value.threadId ?? "main", value.messageId, value.blockId ?? value.clarifyId].join(":"),
    metadata,
  };
  return Match.value(value.kind).pipe(
    Match.when("approval", () => ({
      ...base,
      kind: "approval" as const,
      prompt: JSON.stringify(value.items ?? []),
    })),
    Match.when("input", () => ({
      ...base,
      kind: "input" as const,
      prompt: JSON.stringify(value.questions ?? []),
      ...(value.questions ? { questions: value.questions.map(question) } : {}),
    })),
    Match.exhaustive,
  );
};

/** Decode once at the CLI boundary; unsupported status values remain explicitly unknown. */
export const doubaoStatus = (input: unknown): ExecutionStatus => {
  const value = Schema.decodeUnknownSync(RawStatus)(input);
  const status: ExecutionStatus = Match.value(value.status).pipe(
    Match.when("completed", () => ({
      state: "completed" as const,
      ...(value.reply?.text !== undefined ? { result: { text: value.reply.text } } : {}),
    })),
    Match.when("waiting_input", () => ({
      state: "waiting_input" as const,
      requests: (value.pending ?? []).map(request),
    })),
    Match.whenOr("running", "failed", "cancelled", "unknown", (state) => ({ state })),
    Match.orElse(() => ({ state: "unknown" as const })),
  );
  return value.error ? { ...status, error: String(value.error) } : status;
};
