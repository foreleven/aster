import { Option, Schema } from "effect";
import type { PublicContext } from "@aster/api-contracts";
import type { ContextInput } from "./model.js";
export interface ContextViewPolicy {
  readonly matches?: ((path: string) => boolean) | undefined;
  readonly project: (
    record: ContextInput & { readonly revision?: number },
  ) => PublicContext | undefined;
}

/** Private behavior supplied by the Actor implementation, never serialized. */
export interface ContextDefinition {
  readonly view?: ContextViewPolicy;
  readonly validate: (record: ContextInput) => ContextInput;
  readonly changes?: "none" | "durable-state";
}

export const defineContext = <State extends object, Message>(options: {
  readonly view?: ContextViewPolicy;
  readonly state: Schema.ConstraintDecoder<State>;
  readonly message: Schema.ConstraintDecoder<Message>;
  readonly changes?: "none" | "durable-state";
}): ContextDefinition => {
  const validate = (record: ContextInput) => ({
    path: record.path,
    description: record.description,
    state: Schema.decodeUnknownSync(options.state)(record.state),
    messages: Schema.decodeUnknownSync(Schema.Array(options.message))(record.messages),
  });
  return {
    validate,
    changes: options.changes,
    ...(options.view ? { view: options.view } : {}),
  };
};

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
      revision: record.revision ?? 0,
      description: record.description,
      state: state.value,
      messages,
      projection: { version: 1, visibility: "public" },
    };
  },
});

export const restrictedContext = (
  record: PublicContext,
  reason: "missing-policy" | "invalid-data",
): PublicContext => ({
  path: record.path,
  revision: record.revision ?? 0,
  description: record.description,
  state: {},
  messages: [],
  projection: { version: 1, visibility: "restricted", reason },
});
