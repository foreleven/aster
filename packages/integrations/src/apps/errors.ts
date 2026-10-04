import { stripVTControlCharacters } from "node:util";
import { Match, Option, Result, Schema } from "effect";
import { Yaml } from "effect/unstable/encoding";
import { ContextQueryError } from "@aster/core";

const ErrorEnvelope = Schema.Struct({
  error: Schema.Struct({
    code: Schema.String.check(Schema.isPattern(/^[A-Z][A-Z0-9_]{0,63}$/)),
    message: Schema.String,
    help: Schema.optional(Schema.String),
  }),
});

const decodeDiagnostic = (text: string) => {
  const clean = stripVTControlCharacters(text).trim();
  return Schema.decodeUnknownOption(Schema.fromJsonString(ErrorEnvelope))(clean).pipe(
    Option.orElse(() =>
      Result.try(() => Yaml.parse(clean)).pipe(
        Result.flatMap(Schema.decodeUnknownResult(ErrorEnvelope)),
        Result.getSuccess,
      ),
    ),
  );
};

/** Export selected diagnostic fields only, never provider payloads, traces or cause chains. */
const diagnosticText = (text: string, privateValues: readonly string[]) => {
  let clean = stripVTControlCharacters(text);
  for (const value of privateValues) {
    if (value) clean = clean.split(value).join("[redacted]");
  }
  return clean
    .replace(/https?:\/\/[^\s<>"']+/gi, "[url]")
    .replace(/\b(?:authorization|cookie|set-cookie)\s*[:=][^\r\n]*/gi, "[redacted header]")
    .replace(/\b(?:bearer|basic)\s+\S+/gi, "[redacted authorization]")
    .replace(
      /\b(?:[\w-]*(?:token|secret|password|api[_-]?key))\s*[:=]\s*(?:"[^"]*"|'[^']*'|[^\s,;]+)/gi,
      "[redacted credential]",
    )
    .replace(/\s+/g, " ")
    .trim()
    .slice(0, 1500);
};

export const openCliFailure = (
  exitCode: number,
  stdout: string,
  stderr: string,
  privateValues: readonly string[],
): ContextQueryError => {
  // OpenCLI 1.8.8 emits YAML errors to stderr even when success output uses -f json.
  const diagnostic = decodeDiagnostic(stderr).pipe(Option.orElse(() => decodeDiagnostic(stdout)));
  if (Option.isNone(diagnostic))
    return new ContextQueryError({
      kind: "failed",
      message: `OpenCLI exited with code ${exitCode}; no structured diagnostic was returned. Run opencli doctor and inspect the command locally.`,
    });
  const { code, message, help } = diagnostic.value.error;
  const kind = Match.value(code).pipe(
    Match.when("ARGUMENT", () => "invalid-input" as const),
    Match.when(Match.is("BROWSER_CONNECT", "CONFIG", "ADAPTER_LOAD"), () => "unavailable" as const),
    Match.when("TIMEOUT", () => "timeout" as const),
    Match.when("SESSION_BUSY", () => "busy" as const),
    Match.orElse(() => "failed" as const),
  );
  const summary = diagnosticText(message, privateValues);
  const hint = help ? diagnosticText(help, privateValues) : "";
  return new ContextQueryError({
    kind,
    message: `OpenCLI exited with code ${exitCode} [${code}]: ${summary}${hint ? ` Hint: ${hint}` : ""}`,
  });
};
