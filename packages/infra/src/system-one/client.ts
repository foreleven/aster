import { DecisionError, type SystemOneClient } from "@aster/core";
import {
  Clock,
  Data,
  Duration,
  Effect,
  Predicate,
  Random,
  Redacted,
  Schedule,
  Schema,
} from "effect";
import { HttpClient, HttpClientRequest } from "effect/http";
import { randomUUID } from "node:crypto";
import type { SystemOneSettings } from "./settings.js";

class RequestFailure extends Data.TaggedError("SystemOneRequestFailure")<{
  readonly retryable: boolean;
  readonly message: string;
  readonly cause?: unknown;
  readonly retryAfterMs?: number;
}> {}
const Answer = Schema.Struct({
  type: Schema.String,
  choice: Schema.optional(Schema.String),
  score: Schema.optional(Schema.Finite),
  confidence: Schema.optional(Schema.Finite),
  legend: Schema.optional(Schema.Record(Schema.String, Schema.Unknown)),
});
const Response = Schema.Struct({ answers: Schema.Record(Schema.String, Answer) });
const networkCodes = new Set([
  "ECONNREFUSED",
  "ECONNRESET",
  "ENOTFOUND",
  "EAI_AGAIN",
  "ETIMEDOUT",
  "EPIPE",
  "ENETUNREACH",
  "EHOSTUNREACH",
  "UND_ERR_CONNECT_TIMEOUT",
  "UND_ERR_HEADERS_TIMEOUT",
  "UND_ERR_BODY_TIMEOUT",
  "UND_ERR_SOCKET",
]);
/** Only known network codes leave a cause; native messages can contain credentials. */
const transportCodes = (cause: unknown, depth = 0): string[] => {
  if (Redacted.isRedacted(cause)) return transportCodes(Redacted.value(cause), depth + 1);
  if (depth > 4 || !Predicate.isObject(cause)) return [];
  const code = Predicate.hasProperty(cause, "code") ? cause.code : undefined;
  return [
    ...(typeof code === "string" && networkCodes.has(code) ? [code] : []),
    ...(Predicate.hasProperty(cause, "cause") ? transportCodes(cause.cause, depth + 1) : []),
    ...(cause instanceof AggregateError
      ? cause.errors.slice(0, 8).flatMap((error: unknown) => transportCodes(error, depth + 1))
      : []),
  ];
};

/** All retry decisions concern this read-only model decision protocol, never external task submission. */
export const makeSystemOneClient = Effect.fn("SystemOne.make")(function* (
  settings: SystemOneSettings,
) {
  const client = yield* HttpClient.HttpClient;
  const endpoint = Redacted.value(settings.endpoint);
  const origin = new URL(endpoint).origin;
  return {
    configured: true,
    systemOne: Effect.fn("SystemOne.request")(function* (request) {
      for (const question of Object.values(request.questions)) {
        const size =
          question.type === "score"
            ? question.criteria.length
            : Object.keys(question.criteria).length;
        if (
          !question.instructions.trim() ||
          size < (question.type === "score" ? 2 : 1) ||
          (question.type === "score" && size > 10)
        )
          return yield* new DecisionError({ message: "Invalid System One question criteria" });
      }
      const requestId = yield* Effect.sync(randomUUID);
      const started = yield* Clock.currentTimeMillis;
      let attempt = 0;
      const log = (event: string, fields: Record<string, unknown> = {}) =>
        Effect.logInfo({
          event,
          requestId,
          model: settings.model,
          endpoint: origin,
          attempt,
          ...fields,
        });
      yield* log("system-one.request.started", {
        timeoutMs: 10000,
        maxRetries: 2,
        questionCount: Object.keys(request.questions).length,
      });
      const execute = Effect.scoped(
        Effect.gen(function* () {
          attempt++;
          yield* log("system-one.request.attempt.started");
          let input = HttpClientRequest.post(endpoint).pipe(
            HttpClientRequest.bodyJsonUnsafe({
              ...request,
              model: settings.model,
              state:
                typeof request.state === "string" ? request.state : JSON.stringify(request.state),
            }),
          );
          if (settings.apiKey !== undefined)
            input = input.pipe(
              HttpClientRequest.setHeader(
                "Authorization",
                `Bearer ${Redacted.value(settings.apiKey)}`,
              ),
            );
          const response = yield* client.execute(input);
          yield* log("system-one.request.headers", { status: response.status });
          if (response.status < 200 || response.status >= 300) {
            const rawDelay = response.headers["retry-after-ms"];
            const rawSeconds = response.headers["retry-after"];
            const now = yield* Clock.currentTimeMillis;
            const delay =
              rawDelay !== undefined
                ? Number(rawDelay)
                : rawSeconds === undefined
                  ? NaN
                  : /^\d+(?:\.\d+)?$/.test(rawSeconds)
                    ? Number(rawSeconds) * 1000
                    : Date.parse(rawSeconds) - now;
            return yield* new RequestFailure({
              message: `System One returned HTTP ${response.status}`,
              retryable:
                response.status === 408 || response.status === 429 || response.status >= 500,
              ...(Number.isFinite(delay) && delay >= 0 && delay <= 60000
                ? { retryAfterMs: delay }
                : {}),
            });
          }
          const body = yield* response.json;
          const value = yield* Schema.decodeUnknownEffect(Response)(body).pipe(
            Effect.mapError(
              (cause) =>
                new RequestFailure({
                  message: "System One returned an invalid response",
                  retryable: false,
                  cause: Redacted.make(cause),
                }),
            ),
          );
          for (const [key, question] of Object.entries(request.questions)) {
            const answer = value.answers[key];
            if (
              !answer ||
              answer.type !== question.type ||
              (question.type === "choice" &&
                (answer.choice === undefined ||
                  !Object.hasOwn(question.criteria, answer.choice))) ||
              (question.type === "score" &&
                (answer.score === undefined ||
                  answer.score < 0 ||
                  answer.score > question.criteria.length - 1))
            )
              return yield* new RequestFailure({
                message: "System One returned an invalid answer",
                retryable: false,
              });
          }
          return value;
        }),
      ).pipe(
        Effect.timeout(10000),
        Effect.catchTag("TimeoutError", (cause) =>
          Effect.fail(
            new RequestFailure({
              message: "System One request timed out",
              retryable: true,
              cause: Redacted.make(cause),
            }),
          ),
        ),
        Effect.catchTag("HttpClientError", (cause) => {
          const codes = [...new Set(transportCodes(cause.reason.cause))];
          return Effect.fail(
            new RequestFailure({
              message: codes.length
                ? `System One transport failed (${codes.join(", ")})`
                : "System One transport failed",
              retryable: cause.reason._tag === "TransportError",
              cause: Redacted.make(cause),
            }),
          );
        }),
      );
      const schedule = Schedule.exponential(500).pipe(
        Schedule.modifyDelay(({ duration, input }) => {
          const failure = input instanceof RequestFailure ? input : undefined;
          return failure?.retryAfterMs !== undefined
            ? Effect.succeed(failure.retryAfterMs)
            : Random.next.pipe(
                Effect.map(
                  (random) => Math.min(Duration.toMillis(duration), 5000) * (0.75 + random * 0.5),
                ),
              );
        }),
      );
      return yield* execute.pipe(
        Effect.retry({ times: 2, while: (failure) => failure.retryable, schedule }),
        Effect.mapError(
          (cause) => new DecisionError({ message: cause.message, cause: Redacted.make(cause) }),
        ),
        Effect.tap(() =>
          Clock.currentTimeMillis.pipe(
            Effect.flatMap((now) =>
              log("system-one.request.completed", { elapsedMs: now - started }),
            ),
          ),
        ),
        Effect.tapError((error) =>
          Effect.logError({
            event: "system-one.request.failed",
            requestId,
            endpoint: origin,
            attempt,
            error: error.message,
          }),
        ),
        Effect.onInterrupt(() => log("system-one.request.interrupted")),
      );
    }),
  } satisfies SystemOneClient;
});
