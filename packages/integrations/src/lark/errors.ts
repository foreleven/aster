import { Data, Match } from "effect";
import { ContextQueryError } from "@aster/core";

export class LarkCliError extends Data.TaggedError("LarkCliError")<{
  readonly message: string;
  readonly cause: unknown;
  readonly kind?: "unavailable" | "timeout" | "failed";
}> {}
export class LarkAccountError extends Data.TaggedError("LarkAccountError")<{
  readonly message: string;
  readonly cause: unknown;
}> {}
export class ChatPollError extends Data.TaggedError("ChatPollError")<{
  readonly message: string;
  readonly cause: unknown;
}> {}

/** Only stable diagnostics cross the public query boundary; provider causes stay internal. */
export const queryError = (error: { readonly _tag: string; readonly kind?: string }) =>
  Match.value({ tag: error._tag, kind: error.kind }).pipe(
    Match.when(
      { kind: "invalid-input" },
      () =>
        new ContextQueryError({ kind: "invalid-input", message: "Invalid Lark query arguments" }),
    ),
    Match.when(
      { kind: "timeout" },
      () => new ContextQueryError({ kind: "timeout", message: "Lark query timed out" }),
    ),
    Match.when(
      { kind: "unavailable" },
      () => new ContextQueryError({ kind: "unavailable", message: "Lark data is unavailable" }),
    ),
    Match.when(
      { tag: "ContextValidationError" },
      () => new ContextQueryError({ kind: "failed", message: "Retained Lark data is invalid" }),
    ),
    Match.orElse(
      () => new ContextQueryError({ kind: "failed", message: "Unable to read Lark data" }),
    ),
  );
