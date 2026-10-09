import { Data } from "effect";

export class LarkCliError extends Data.TaggedError("LarkCliError")<{
  readonly message: string;
  readonly cause: unknown;
}> {}

export class LarkAccountError extends Data.TaggedError("LarkAccountError")<{
  readonly message: string;
  readonly cause: unknown;
}> {}

export class ChatPollError extends Data.TaggedError("ChatPollError")<{
  readonly message: string;
  readonly cause: unknown;
}> {}

export class ChatSummaryError extends Data.TaggedError("ChatSummaryError")<{
  readonly message: string;
  readonly cause?: unknown;
  readonly kind?: "capacity" | "transient" | "permanent";
}> {}
