import { Data, Effect, Schema } from "effect";
import { HttpRouter, HttpServerRequest } from "effect/unstable/http";
import { ApplicationError, ApprovalResponse, type ApplicationApi } from "@aster/core";
import { json } from "./http-policy.js";

const statusCodes = {
  "not-found": 404,
  "invalid-input": 400,
  unavailable: 503,
  conflict: 409,
} as const;
class HttpInputError extends Data.TaggedError("HttpInputError")<{
  readonly status: number;
  readonly message: string;
}> {}
const rest = <A, E, R>(effect: Effect.Effect<A, E, R>, status = 200) =>
  effect.pipe(
    Effect.map((value) => json(value, status)),
    Effect.catch((error) => {
      if (error instanceof HttpInputError)
        return Effect.succeed(json({ error: error.message }, error.status));
      if (error instanceof ApplicationError)
        return Effect.succeed(json({ error: error.message }, statusCodes[error.kind]));
      return Effect.succeed(json({ error: "Request failed" }, 500));
    }),
  );
const accepted = <E, R>(effect: Effect.Effect<void, E, R>) =>
  rest(effect.pipe(Effect.as({ accepted: true })), 202);
const body = <A>(schema: Schema.ConstraintDecoder<A>) =>
  Effect.gen(function* () {
    const request = yield* HttpServerRequest.HttpServerRequest;
    if (!request.headers["content-type"]?.startsWith("application/json"))
      return yield* new HttpInputError({
        status: 415,
        message: "Expected application/json",
      });
    return yield* request.json.pipe(
      Effect.flatMap(Schema.decodeUnknownEffect(schema)),
      Effect.mapError(
        () => new ApplicationError({ kind: "invalid-input", message: "Invalid JSON request" }),
      ),
    );
  });

/** Compatibility routes share the same application operations as RPC. */
export const legacyRest = (application: ApplicationApi, url: string) =>
  HttpRouter.addAll([
    HttpRouter.route("GET", "/api/dashboard", rest(application.dashboard)),
    HttpRouter.route("GET", "/api/goals", rest(application.goals.list)),
    HttpRouter.route(
      "GET",
      "/api/context",
      rest(
        Effect.gen(function* () {
          const request = yield* HttpServerRequest.HttpServerRequest;
          return yield* application.context(
            new URL(request.url, url).searchParams.get("path") ?? "",
          );
        }),
      ),
    ),
    HttpRouter.route(
      "GET",
      "/api/goals/:slug/history",
      rest(
        Effect.gen(function* () {
          const { slug } = yield* HttpRouter.params;
          const request = yield* HttpServerRequest.HttpServerRequest;
          const params = new URL(request.url, url).searchParams;
          return yield* application.goals.history(slug!, {
            ...(params.has("before") ? { before: Number(params.get("before")) } : {}),
            ...(params.has("limit") ? { limit: Number(params.get("limit")) } : {}),
          });
        }),
      ),
    ),
    HttpRouter.route("GET", "/api/approvals", rest(application.approvals.list)),
    HttpRouter.route(
      "POST",
      "/api/approvals/respond",
      accepted(
        Effect.gen(function* () {
          const input = yield* body(
            Schema.Struct({ id: Schema.String, response: ApprovalResponse }),
          );
          yield* application.approvals.respond(input.id, input.response);
        }),
      ),
    ),
    HttpRouter.route(
      "POST",
      "/api/goals/:slug/messages",
      accepted(
        Effect.gen(function* () {
          const { slug } = yield* HttpRouter.params;
          const { text } = yield* body(Schema.Struct({ text: Schema.String }));
          yield* application.goals.sendMessage(slug!, text);
        }),
      ),
    ),
    HttpRouter.route(
      "POST",
      "/api/goals/:slug/end",
      accepted(
        Effect.gen(function* () {
          const { slug } = yield* HttpRouter.params;
          yield* body(Schema.Struct({}));
          yield* application.goals.end(slug!);
        }),
      ),
    ),
  ]);
