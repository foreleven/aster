import { Schema } from "effect";
import { reactionEventsCheck } from "./reaction-event.js";

import { PublicContext } from "@aster/api-contracts";

/** Immutable source-side handoff. Only public evidence enters the reaction pipeline. */
export const ContextReactionEvent = Schema.Struct({
  requestId: Schema.NonEmptyString,
  causationId: Schema.NonEmptyString,
  source: Schema.String,
  target: Schema.Literal("/system-one"),
  revision: Schema.Int.check(Schema.isGreaterThan(0)),
  record: PublicContext,
  createdAt: Schema.String,
});
export type ContextReactionEvent = typeof ContextReactionEvent.Type;

/** Canonical recovery metadata is deliberately absent from PublicContext. */
export const ContextRecord = Schema.Struct({
  ...PublicContext.fields,
  reactionEvents: Schema.optional(Schema.Array(ContextReactionEvent)),
}).check(reactionEventsCheck);
export type ContextRecord = typeof ContextRecord.Type;

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

export interface ContextViewPolicy {
  readonly matches?: ((path: string) => boolean) | undefined;
  readonly project: (record: ContextRecord) => ContextRecord | undefined;
}

/** Private behavior supplied by the Actor implementation, never serialized. */
export interface ContextDefinition {
  readonly view?: ContextViewPolicy;
  readonly identity: string;
  readonly validate: (record: ContextRecord) => ContextRecord;
  readonly signalSource?: boolean;
  readonly capture?: (record: ContextRecord) => ContextCapture | undefined;
}

export const defineContext = <State extends object, Message>(options: {
  readonly view?: ContextViewPolicy;
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
    ...(record.revision === undefined ? {} : { revision: record.revision }),
    description: record.description,
    state: Schema.decodeUnknownSync(options.state)(record.state),
    messages: Schema.decodeUnknownSync(Schema.Array(options.message))(record.messages),
  });
  return {
    identity: options.identity,
    validate,
    signalSource: options.signalSource,
    ...(options.view ? { view: options.view } : {}),
    ...(options.capture
      ? { capture: (record: ContextRecord) => options.capture!(validate(record)) }
      : {}),
  };
};
