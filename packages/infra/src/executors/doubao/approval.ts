import { ExternalAgentError, type ApprovalResponse } from "@aster/core";
import { Deferred, Effect, Redacted, Schema, Stream } from "effect";
import { HttpClient, HttpClientRequest } from "effect/http";
import { Socket } from "effect/socket";
import { respondInRenderer } from "./renderer.js";
const Targets = Schema.Array(
  Schema.Struct({
    url: Schema.String,
    type: Schema.String,
    webSocketDebuggerUrl: Schema.optional(Schema.String),
  }),
);
const Result = Schema.Struct({
  id: Schema.optional(Schema.Number),
  error: Schema.optional(Schema.Unknown),
  result: Schema.optional(Schema.Struct({ exceptionDetails: Schema.optional(Schema.Unknown) })),
});

export const respondDoubaoNative = Effect.fn("Doubao.nativeResponse")(function* (
  request: unknown,
  response: ApprovalResponse,
  endpoint: string,
) {
  return yield* Effect.scoped(
    Effect.gen(function* () {
      const http = yield* HttpClient.HttpClient;
      const targets = yield* http
        .execute(HttpClientRequest.get(endpoint.replace(/\/$/, "") + "/json/list"))
        .pipe(
          Effect.flatMap((response) => response.json),
          Effect.flatMap(Schema.decodeUnknownEffect(Targets)),
          Effect.timeout(5000),
        );
      const target = targets.find((target) => {
        try {
          const url = new URL(target.url);
          return (
            target.type === "page" &&
            ["doubaowork:", "chrome:"].includes(url.protocol) &&
            url.hostname === "doubaowork-chat" &&
            /^\/chat(?:\/|$)/.test(url.pathname)
          );
        } catch {
          return false;
        }
      });
      if (!target?.webSocketDebuggerUrl)
        return yield* new ExternalAgentError({
          operation: "respond",
          message: "Doubao Work renderer is unavailable",
        });
      const socket = yield* Socket.makeWebSocket(target.webSocketDebuggerUrl).pipe(
        Effect.provide(Socket.layerWebSocketConstructorGlobal),
      );
      const completed = yield* Deferred.make<void, ExternalAgentError>();
      const reader = yield* socket.reader;
      yield* Stream.fromPull(Effect.succeed(reader.pull)).pipe(
        Stream.map((frame) =>
          typeof frame === "string" ? frame : new TextDecoder().decode(frame),
        ),
        Stream.runForEach((text) =>
          Effect.gen(function* () {
            const value = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Result))(text);
            if (value.id !== 1) return;
            if (value.error || value.result?.exceptionDetails)
              yield* Deferred.fail(
                completed,
                new ExternalAgentError({
                  operation: "respond",
                  message: "Native approval response failed; outcome may be unknown",
                }),
              );
            else yield* Deferred.succeed(completed, undefined);
          }),
        ),
        Effect.mapError(
          (cause) =>
            new ExternalAgentError({
              operation: "respond",
              message: "Native approval transport failed; outcome may be unknown",
              cause: Redacted.make(cause),
            }),
        ),
        Effect.onExit((exit) =>
          exit._tag === "Failure"
            ? Deferred.done(completed, exit)
            : Deferred.fail(
                completed,
                new ExternalAgentError({
                  operation: "respond",
                  message: "Native approval connection closed before acknowledgement",
                }),
              ),
        ),
        Effect.catchTag("ExternalAgentError", () => Effect.void),
        Effect.forkScoped,
      );
      const writer = yield* socket.writer;
      yield* writer.write(
        JSON.stringify({
          id: 1,
          method: "Runtime.evaluate",
          params: {
            expression: `(${respondInRenderer.toString()})(${JSON.stringify({ request, response })})`,
            awaitPromise: true,
            returnByValue: true,
          },
        }),
      );
      yield* Deferred.await(completed);
    }),
  ).pipe(
    Effect.timeout(30000),
    Effect.mapError((cause) =>
      cause instanceof ExternalAgentError
        ? cause
        : new ExternalAgentError({
            operation: "respond",
            message: "Native approval transport failed; outcome may be unknown",
            cause: Redacted.make(cause),
          }),
    ),
  );
});
