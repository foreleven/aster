import { isDeepStrictEqual } from "node:util";
import { Effect, Layer, Schema } from "effect";
import {
  ChannelWrites,
  ChannelWriteError,
  ContextRegistry,
  WritebackOperation,
  WritebackRequest,
  WritebackAuthorization,
} from "@aster/core";
import { LarkConfig } from "../config.js";
import { runLarkCli } from "../shared/cli.js";

const Published = Schema.Struct({
  ok: Schema.Literal(true),
  identity: Schema.Literals(["user", "bot"]),
  data: Schema.Struct({ message_id: Schema.NonEmptyString, chat_id: Schema.NonEmptyString }),
});
const Admission = Schema.Struct({ writeback: WritebackOperation });
const Target = Schema.Struct({ chat: Schema.Struct({ id: Schema.NonEmptyString }) });
const Input = Schema.Struct({ request: WritebackRequest, authorization: WritebackAuthorization });
const TextContent = Schema.fromJsonString(Schema.Struct({ text: Schema.String }));

/** A receipt is authoritative. While an acknowledgement is unknown, an exact
 * payload observed in the same chat after submission is conservatively treated
 * as a possible echo, never as fresh automatic authorization. */
export const isLarkWritebackEcho = (
  registry: ContextRegistry["Service"],
  channelPath: string,
  message: { readonly id: string; readonly at: string; readonly content: string },
): boolean => {
  const parsed = Schema.decodeUnknownResult(TextContent)(message.content);
  const text = parsed._tag === "Success" ? parsed.success.text : message.content;
  return Object.values(registry.snapshot()).some((record) => {
    if (!/^\/(?:runs\/[^/]+|(?:goals|signals)\/[^/]+\/runs\/[^/]+)$/.test(record.path))
      return false;
    const decoded = Schema.decodeUnknownResult(Admission)(record.state);
    if (decoded._tag === "Failure") return false;
    const operation = decoded.success.writeback;
    if (operation.request.action.channelPath !== channelPath) return false;
    if (operation.status === "published" && operation.externalId === message.id) return true;
    return (
      ["sending", "unknown"].includes(operation.status) &&
      operation.submittedAt !== undefined &&
      Date.parse(message.at) >= Date.parse(operation.submittedAt) &&
      text === operation.request.content
    );
  });
};

/** The transport accepts only an already committed, approved Run publication.
 * JSON content avoids @file expansion and automatic Markdown asset downloads. */
export const makeLarkChannelWrites = (
  registry: ContextRegistry["Service"],
  execute: (args: readonly string[]) => Effect.Effect<string, ChannelWriteError>,
): ChannelWrites["Service"] => ({
  publish: Effect.fn("Lark.publishResult")(function* (raw, grant) {
    const invalid = () =>
      new ChannelWriteError({
        outcome: "rejected",
        message: "Publication is not an approved, committed Lark Channel action",
      });
    const { request, authorization } = yield* Schema.decodeUnknownEffect(Input)({
      request: raw,
      authorization: grant,
    }).pipe(Effect.mapError(invalid));
    const owner = registry.get(request.source);
    const saved = yield* Schema.decodeUnknownEffect(Admission)(owner?.state).pipe(
      Effect.mapError(invalid),
    );
    if (
      saved.writeback.status !== "sending" ||
      !isDeepStrictEqual(saved.writeback.request, request) ||
      !isDeepStrictEqual(saved.writeback.authorization, authorization)
    )
      return yield* invalid();
    const match = /^\/lark\/im\/chats\/(oc_[a-zA-Z0-9_-]+)$/.exec(request.action.channelPath);
    const target = yield* Schema.decodeUnknownEffect(Target)(
      registry.get(request.action.channelPath)?.state,
    ).pipe(Effect.mapError(invalid));
    if (!match || match[1] !== target.chat.id || request.requestId.length > 50)
      return yield* invalid();
    const stdout = yield* execute([
      "im",
      "+messages-send",
      "--as",
      request.action.identity,
      "--chat-id",
      target.chat.id,
      "--msg-type",
      "text",
      "--content",
      JSON.stringify({ text: request.content }),
      "--idempotency-key",
      request.requestId,
      "--format",
      "json",
    ]);
    const result = yield* Effect.try({
      try: () => Schema.decodeUnknownSync(Published)(JSON.parse(stdout)),
      catch: (cause) =>
        new ChannelWriteError({
          outcome: "unknown",
          cause,
          message: "Lark publication response could not be verified; do not resend",
        }),
    });
    if (result.identity !== request.action.identity || result.data.chat_id !== target.chat.id)
      return yield* new ChannelWriteError({
        outcome: "unknown",
        message: "Lark returned a different publication identity or destination; do not resend",
      });
    return { externalId: result.data.message_id };
  }),
});

export const larkChannelWritesLayer = Layer.effect(
  ChannelWrites,
  Effect.gen(function* () {
    const registry = yield* ContextRegistry;
    const config = yield* LarkConfig;
    return makeLarkChannelWrites(registry, (args) =>
      Effect.tryPromise({
        try: (signal) => runLarkCli(args, config.profile, signal),
        catch: (cause) =>
          new ChannelWriteError({
            outcome: "unknown",
            cause,
            message:
              "Lark publication acknowledgement is unavailable; inspect the retained operation before further action",
          }),
      }),
    );
  }),
);
