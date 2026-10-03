import { Data } from "effect";

/** Native SDK boundary: thrown only after a terminal failure is durably committed. */
export class DurableAgentFailure extends Data.TaggedError("DurableAgentFailure")<{
  readonly message: string;
}> {}
