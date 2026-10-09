import { Match, Predicate } from "effect";
import { ChatSummaryError } from "../../shared/errors.js";

/** Provider boundaries expose inconsistent diagnostics; unknown failures stay non-retryable. */
export const chatSummaryError = (cause: unknown): ChatSummaryError => {
  if (cause instanceof ChatSummaryError) return cause;
  const diagnostics: string[] = [];
  const visited = new Set<unknown>();
  for (
    let current = cause;
    Predicate.isObject(current) && !visited.has(current);
    current = "cause" in current ? current.cause : undefined
  ) {
    visited.add(current);
    for (const key of ["message", "code", "status", "statusCode", "_tag"])
      if (key in current) diagnostics.push(String(current[key]));
  }
  const text = diagnostics.join(" ");
  const kind = Match.value(text).pipe(
    Match.when(
      (value) =>
        /context[_ ]length[_ ]exceeded|maximum context length|context (?:window|length).*(?:exceed|too (?:long|large))|(?:prompt|input|request).*(?:too (?:long|large)|exceeds)|too many tokens|\b413\b/i.test(
          value,
        ),
      () => "capacity" as const,
    ),
    Match.when(
      (value) =>
        /\b(?:401|403)\b|unauthorized|forbidden|invalid.*(?:api.?key|credential)|not configured|unknown model/i.test(
          value,
        ),
      () => "permanent" as const,
    ),
    Match.when(
      (value) =>
        /\b(?:408|429|500|502|503|504|ECONNRESET|ECONNREFUSED|ETIMEDOUT|EAI_AGAIN)\b|TimeoutError|rate.?limit|temporarily unavailable|overloaded|network (?:error|failure)|fetch failed|failed to fetch|timed? ?out/i.test(
          value,
        ),
      () => "transient" as const,
    ),
    Match.orElse(() => "permanent" as const),
  );
  return new ChatSummaryError({
    cause,
    kind,
    message: cause instanceof Error ? cause.message : "Chat summary request failed",
  });
};
