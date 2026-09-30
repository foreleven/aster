import { Cause } from "effect";
import type { FailureSummary } from "../actor.js";

export const failureSummary = (cause: Cause.Cause<unknown>): FailureSummary => {
  const error = Cause.squash(cause);
  return error instanceof Error
    ? { message: error.message, stack: error.stack }
    : { message: String(error) };
};

export const commandTag = (command: unknown): string | undefined =>
  typeof command === "object" &&
  command !== null &&
  "_tag" in command &&
  typeof command._tag === "string"
    ? command._tag
    : undefined;
