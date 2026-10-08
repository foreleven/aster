import { DurableHarness, type HarnessOptions, type HarnessSubmission } from "@aster/agent/harness";
import type { AgentError, AgentResult } from "@aster/agent";
import { Effect, Option } from "effect";

/** A fake SDK invocation used by business tests. Native persistence is tested separately. */
export interface HarnessCall extends HarnessOptions {
  readonly content: string;
  readonly requestId: string;
}
export const makeHarness = (
  execute: (invocation: HarnessCall) => Effect.Effect<AgentResult, AgentError>,
) =>
  DurableHarness.make((options) =>
    Effect.sync(() => {
      const submissions = new Map<string, HarnessSubmission>();
      return {
        submit: (input) =>
          Effect.sync(() => {
            const invocation: HarnessCall = {
              ...options,
              content:
                typeof input.content === "string" ? input.content : JSON.stringify(input.content),
              requestId: input.requestId,
            };
            const submission: HarnessSubmission = {
              status: Effect.die("Fake SDK status is not configured"),
              wait: execute(invocation).pipe(
                Effect.map((result) =>
                  result.messages.findLast((message) => message.role === "assistant"),
                ),
              ),
            };
            submissions.set(input.requestId, submission);
            return submission;
          }),
        submission: (id) => Effect.sync(() => Option.fromUndefinedOr(submissions.get(id))),
        abort: Effect.void,
      };
    }),
  );
