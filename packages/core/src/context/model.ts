import { Schema } from "effect";

import { PublicContext as ContextRecord } from "@aster/api-contracts";
export { PublicContext as ContextRecord } from "@aster/api-contracts";

export interface ContextChange {
  readonly path: string;
  readonly created: boolean;
  readonly stateChanged: boolean;
  /** Detached snapshot at this change, never a reference to mutable actor state. */
  readonly record: ContextRecord;
  /** History bootstrap is readable by planners but does not replay old source events. */
  readonly evaluate?: boolean;
}

export interface ContextCapture {
  readonly sessionId: string;
  readonly records: ReadonlyArray<ContextRecord>;
}

/** Private behavior supplied by the Actor implementation, never serialized. */
export interface ContextDefinition {
  readonly identity: string;
  readonly validate: (record: ContextRecord) => ContextRecord;
  readonly signalSource?: boolean;
  readonly capture?: (record: ContextRecord) => ContextCapture | undefined;
}

export const defineContext = <State extends object, Message>(options: {
  readonly identity: string;
  readonly state: Schema.ConstraintDecoder<State>;
  readonly message: Schema.ConstraintDecoder<Message>;
  readonly signalSource?: boolean;
  readonly capture?: (
    record: Omit<ContextRecord, "state" | "messages"> & {
      readonly state: State;
      readonly messages: ReadonlyArray<Message>;
    },
  ) => ContextCapture | undefined;
}): ContextDefinition => {
  const validate = (record: ContextRecord) => ({
    path: record.path,
    description: record.description,
    state: Schema.decodeUnknownSync(options.state)(record.state),
    messages: Schema.decodeUnknownSync(Schema.Array(options.message))(record.messages),
  });
  return {
    identity: options.identity,
    validate,
    signalSource: options.signalSource,
    ...(options.capture
      ? { capture: (record: ContextRecord) => options.capture!(validate(record)) }
      : {}),
  };
};
