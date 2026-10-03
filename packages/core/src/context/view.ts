import { Option, Schema } from "effect";
import type { ContextRecord, ContextViewPolicy } from "./model.js";

/** Views are explicit read contracts. Unknown fields are never copied from canonical storage. */
export const contextView = <State extends object, Message>(options: {
  readonly matches?: (path: string) => boolean;
  readonly state: Schema.ConstraintDecoder<State>;
  readonly message?: Schema.ConstraintDecoder<Message>;
  readonly projectMessage?: (message: unknown) => unknown | undefined;
}): ContextViewPolicy => ({
  matches: options.matches,
  project: (record) => {
    const state = Schema.decodeUnknownOption(options.state)(record.state);
    if (Option.isNone(state)) return undefined;
    const messages = record.messages.flatMap((message) => {
      if (options.projectMessage) {
        const projected = options.projectMessage(message);
        return projected === undefined ? [] : [projected];
      }
      if (!options.message) return [];
      const decoded = Schema.decodeUnknownOption(options.message)(message);
      return Option.isSome(decoded) ? [decoded.value] : [];
    });
    return {
      path: record.path,
      revision: record.revision,
      description: record.description,
      state: state.value,
      messages,
      projection: { version: 1, visibility: "public" },
    };
  },
});

export const restrictedContext = (
  record: ContextRecord,
  reason: "missing-policy" | "invalid-data",
): ContextRecord => ({
  path: record.path,
  revision: record.revision,
  description: record.description,
  state: {},
  messages: [],
  projection: { version: 1, visibility: "restricted", reason },
});
