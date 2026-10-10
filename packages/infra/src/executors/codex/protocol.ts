import { Effect, Schema, Match } from "effect";
import { ExternalAgentError, type InputRequest, type ApprovalResponse } from "@aster/core";
export const Row = Schema.Record(Schema.String, Schema.Unknown);
export const Frame = Schema.Struct({
  id: Schema.optional(Schema.Union([Schema.String, Schema.Number])),
  method: Schema.optional(Schema.String),
  params: Schema.optional(Row),
  result: Schema.optional(Schema.Unknown),
  error: Schema.optional(Row),
});
export type Frame = typeof Frame.Type;
export const ThreadStart = Schema.Struct({ thread: Schema.Struct({ id: Schema.NonEmptyString }) });
export const TurnStart = Schema.Struct({ turn: Schema.Struct({ id: Schema.NonEmptyString }) });
export const ThreadRead = Schema.Struct({
  thread: Schema.Struct({
    turns: Schema.optional(
      Schema.Array(
        Schema.Struct({
          id: Schema.String,
          status: Schema.String,
          items: Schema.Array(
            Schema.Struct({ type: Schema.String, text: Schema.optional(Schema.String) }),
          ),
          error: Schema.optional(Row),
        }),
      ),
    ),
  }),
});
const Questions = Schema.Array(
  Schema.Struct({
    id: Schema.String,
    question: Schema.String,
    options: Schema.optional(Schema.Array(Schema.Struct({ label: Schema.String }))),
  }),
);
export const decodeControl = Effect.fnUntraced(function* (
  id: string,
  method: string,
  params: typeof Row.Type,
) {
  const questions = yield* Schema.decodeUnknownEffect(Schema.optional(Questions))(params.questions);
  const input = method.includes("requestUserInput") || method.includes("elicitation");
  return {
    id,
    kind: input ? ("input" as const) : ("approval" as const),
    prompt: JSON.stringify(params),
    ...(questions
      ? {
          questions: questions.map((question) => ({
            id: question.id,
            prompt: question.question,
            allowOther: true,
            ...(question.options
              ? { options: question.options.map((option) => option.label) }
              : {}),
          })),
        }
      : {}),
    metadata: { method, params },
  } satisfies InputRequest;
});
export const controlResponse = Effect.fnUntraced(function* (
  method: string,
  params: typeof Row.Type,
  response: ApprovalResponse,
) {
  return yield* Match.value(method).pipe(
    Match.when("item/tool/requestUserInput", () =>
      Effect.gen(function* () {
        const questions = yield* Schema.decodeUnknownEffect(Questions)(params.questions);
        const answers =
          response.answers ??
          (questions.length === 1 && response.text ? { [questions[0]!.id]: [response.text] } : {});
        if (questions.some((question) => !answers[question.id]?.length))
          return yield* new ExternalAgentError({
            operation: "respond",
            outcome: "rejected",
            message: "Every Codex question requires an answer keyed by question ID",
          });
        return {
          answers: Object.fromEntries(
            Object.entries(answers).map(([id, value]) => [id, { answers: value }]),
          ),
        };
      }),
    ),
    Match.when(
      (value) =>
        [
          "item/commandExecution/requestApproval",
          "item/fileChange/requestApproval",
          "item/permissions/requestApproval",
        ].includes(value),
      () =>
        Effect.gen(function* () {
          if (!response.decision)
            return yield* new ExternalAgentError({
              operation: "respond",
              outcome: "rejected",
              message: "An approval decision is required",
            });
          return method === "item/permissions/requestApproval"
            ? {
                permissions: response.decision === "approve" ? params.permissions : {},
                scope: "turn",
              }
            : { decision: response.decision === "approve" ? "accept" : "decline" };
        }),
    ),
    Match.orElse(() =>
      Effect.fail(
        new ExternalAgentError({
          operation: "respond",
          outcome: "rejected",
          message: `Unsupported Codex control: ${method}`,
        }),
      ),
    ),
  );
});
