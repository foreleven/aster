import { Schema } from "effect";
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
