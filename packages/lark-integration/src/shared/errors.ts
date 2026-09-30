import { Data } from "effect";

export class LarkCliError extends Data.TaggedError("LarkCliError")<{
  readonly message: string;
  readonly cause: unknown;
}> {}

export class LarkAccountError extends Data.TaggedError("LarkAccountError")<{
  readonly message: string;
  readonly cause: unknown;
}> {}

export class ImPollError extends Data.TaggedError("ImPollError")<{
  readonly message: string;
  readonly cause: unknown;
}> {}

export class ImSummaryError extends Data.TaggedError("ImSummaryError")<{
  readonly message: string;
  readonly cause?: unknown;
}> {}
