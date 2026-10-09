import { Effect } from "effect";
import { ChatSummaryError, type ChatSummaryInput, type ChatSummary } from "@aster/integrations";

// Adapt existing asynchronous test doubles at the fixture boundary; production services stay Effect-native.
const failure = (cause: unknown) =>
  new ChatSummaryError({ cause, message: cause instanceof Error ? cause.message : String(cause) });
export const summaryStub =
  (run: (input: ChatSummaryInput) => Promise<ChatSummary>) => (input: ChatSummaryInput) =>
    Effect.tryPromise({ try: () => run(input), catch: failure });
export const gateStub =
  (run: (input: ChatSummaryInput) => Promise<boolean>) => (input: ChatSummaryInput) =>
    Effect.tryPromise({ try: () => run(input), catch: failure });
