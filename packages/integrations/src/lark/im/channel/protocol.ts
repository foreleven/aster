import { Schema } from "effect";
import { ChatPollError } from "../../shared/errors.js";
import { ImPollResult } from "./snapshot.js";
export const ImInternal = Schema.TaggedUnion({
  Poll: {},
  HandedOff: {
    value: ImPollResult,
    result: Schema.TaggedUnion({
      Success: { value: Schema.Void },
      Failure: { error: Schema.instanceOf(ChatPollError) },
    }),
  },
  Polled: {
    result: Schema.TaggedUnion({
      Success: { value: ImPollResult },
      Failure: { error: Schema.instanceOf(ChatPollError) },
    }),
  },
});
// Context queries are intercepted before reaching the channel mailbox.
export type ImCommand = typeof ImInternal.Type;
