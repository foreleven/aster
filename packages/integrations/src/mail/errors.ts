import { Match, Option, Schema } from "effect";

const MailErrorCode = Schema.Literals([
  "UIDVALIDITY_CHANGED",
  "ECONNRESET",
  "ECONNREFUSED",
  "ECONNABORTED",
  "EPIPE",
  "ENOTFOUND",
  "EAI_AGAIN",
  "ETIMEDOUT",
  "CONNECTIONTIMEOUT",
  "SOCKETTIMEOUT",
  "NOCONNECTION",
  "CONNECTIONCLOSED",
  "CERT_HAS_EXPIRED",
  "DEPTH_ZERO_SELF_SIGNED_CERT",
  "SELF_SIGNED_CERT_IN_CHAIN",
  "UNABLE_TO_VERIFY_LEAF_SIGNATURE",
  "ERR_TLS_CERT_ALTNAME_INVALID",
  "ERR_SSL_WRONG_VERSION_NUMBER",
  "AUTHENTICATIONFAILED",
  "AUTHORIZATIONFAILED",
  "PRIVACYREQUIRED",
  "EXPIRED",
  "UNAVAILABLE",
  "LIMIT",
  "NONEXISTENT",
  "NOPERM",
  "INUSE",
  "SERVERBUG",
  "CLIENTBUG",
]);
export const MailFailureStage = Schema.Literals([
  "connect",
  "authenticate",
  "open-mailbox",
  "fetch",
  "parse",
  "validate",
  "publish",
]);
export type MailFailureStage = typeof MailFailureStage.Type;
export const MailFailureDetails = Schema.Struct({
  stage: MailFailureStage,
  reason: Schema.Literals([
    "authentication",
    "network",
    "timeout",
    "tls",
    "mailbox",
    "invalid-response",
    "storage",
    "server",
    "protocol",
  ]),
  code: Schema.optional(MailErrorCode),
  responseStatus: Schema.optional(Schema.Literals(["NO", "BAD", "BYE"])),
});
export type MailFailureDetails = typeof MailFailureDetails.Type;

const TransportFailure = Schema.Struct({
  code: Schema.optional(Schema.Unknown),
  serverResponseCode: Schema.optional(Schema.Unknown),
  responseStatus: Schema.optional(Schema.Unknown),
  authenticationFailed: Schema.optional(Schema.Unknown),
});

/** Export only allowlisted metadata; SDK messages, commands and responses may contain credentials. */
export const mailFailureDetails = (cause: unknown, stage: MailFailureStage): MailFailureDetails => {
  const error: typeof TransportFailure.Type = Option.getOrElse(
    Schema.decodeUnknownOption(TransportFailure)(cause),
    () => ({}),
  );
  const decodeCode = (value: unknown) =>
    Option.getOrUndefined(
      Schema.decodeUnknownOption(MailErrorCode)(
        typeof value === "string" ? value.toUpperCase() : value,
      ),
    );
  const code = decodeCode(error.serverResponseCode) ?? decodeCode(error.code);
  const authentication =
    error.authenticationFailed === true ||
    code === "AUTHENTICATIONFAILED" ||
    code === "AUTHORIZATIONFAILED" ||
    code === "EXPIRED";
  const failureStage = authentication ? "authenticate" : stage;
  const reason = Match.value({ stage: failureStage, code, authentication }).pipe(
    Match.when(
      { code: Match.is("ETIMEDOUT", "CONNECTIONTIMEOUT", "SOCKETTIMEOUT") },
      () => "timeout" as const,
    ),
    Match.when(
      {
        code: Match.is(
          "CERT_HAS_EXPIRED",
          "DEPTH_ZERO_SELF_SIGNED_CERT",
          "SELF_SIGNED_CERT_IN_CHAIN",
          "UNABLE_TO_VERIFY_LEAF_SIGNATURE",
          "ERR_TLS_CERT_ALTNAME_INVALID",
          "ERR_SSL_WRONG_VERSION_NUMBER",
          "PRIVACYREQUIRED",
        ),
      },
      () => "tls" as const,
    ),
    Match.when(
      {
        code: Match.is(
          "ECONNRESET",
          "ECONNREFUSED",
          "ECONNABORTED",
          "EPIPE",
          "ENOTFOUND",
          "EAI_AGAIN",
          "NOCONNECTION",
          "CONNECTIONCLOSED",
        ),
      },
      () => "network" as const,
    ),
    Match.when(
      { code: Match.is("UNAVAILABLE", "LIMIT", "INUSE", "SERVERBUG") },
      () => "server" as const,
    ),
    Match.when({ authentication: true }, () => "authentication" as const),
    Match.when({ stage: "authenticate" }, () => "authentication" as const),
    Match.when({ stage: "open-mailbox" }, () => "mailbox" as const),
    Match.when({ stage: Match.is("parse", "validate") }, () => "invalid-response" as const),
    Match.when({ stage: "publish" }, () => "storage" as const),
    Match.orElse(() => "protocol" as const),
  );
  const responseStatus = Option.getOrUndefined(
    Schema.decodeUnknownOption(MailFailureDetails.fields.responseStatus)(error.responseStatus),
  );
  return {
    stage: failureStage,
    reason,
    ...(code ? { code } : {}),
    ...(responseStatus ? { responseStatus } : {}),
  };
};

export const mailFailureSummary = (details: MailFailureDetails, retryInMs: number) =>
  `Mail ${details.stage} failed: ${details.reason}${details.code ? ` (${details.code})` : ""}; retrying in ${Math.ceil(retryInMs / 1000)}s.`;

export class MailFetchError extends Schema.TaggedError<MailFetchError>()("MailFetchError", {
  mailbox: Schema.String,
  message: Schema.String,
  cause: Schema.Unknown,
  details: Schema.optional(MailFailureDetails),
}) {}
