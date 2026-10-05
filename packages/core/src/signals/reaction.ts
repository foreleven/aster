import {
  ApplicationError,
  CausalChain,
  CommandReceipt,
  PublicContext,
  TaskMessage,
} from "@aster/api-contracts";
import { Clock, Effect, Schema } from "effect";
import { isDeepStrictEqual } from "node:util";
import type { ContextRegistry } from "../context/registry.js";
import { SignalDefinition } from "../config/schema.js";
import { sourceSignalEligible } from "./policy.js";

export const SignalReactionInput = Schema.Struct({
  requestId: Schema.NonEmptyString,
  causationId: Schema.NonEmptyString,
  source: Schema.Literal("/system-one"),
  target: Schema.String.check(Schema.isPattern(/^\/signals\/[a-z0-9][a-z0-9-]*$/)),
  expectedRevision: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  sourceContext: PublicContext,
});
export type SignalReactionInput = typeof SignalReactionInput.Type;
export const SignalReactionReceipt = Schema.Struct({
  input: SignalReactionInput,
  receipt: CommandReceipt,
});

export const SignalOccurrence = Schema.Struct({
  message: TaskMessage,
  delivered: Schema.Boolean,
  error: Schema.optional(Schema.String),
});

const ReactionState = Schema.Struct({
  causal: Schema.optional(CausalChain),
  ...SignalDefinition.fields,
  goal: Schema.optional(Schema.String),
  active: Schema.optional(Schema.Boolean),
  deleted: Schema.optional(Schema.Boolean),
  reactionReceipts: Schema.optional(Schema.Array(SignalReactionReceipt)),
  occurrences: Schema.Array(SignalOccurrence),
});

/** Called only in the Signal mailbox. Occurrence and receipt are one accepted commit. */
export const acceptSignalReaction = Effect.fn("Signal.acceptReaction")(function* (
  registry: ContextRegistry["Service"],
  path: string,
  raw: SignalReactionInput,
): Effect.fn.Return<CommandReceipt, ApplicationError> {
  const input = yield* Schema.decodeUnknownEffect(SignalReactionInput)(raw).pipe(
    Effect.mapError(
      () => new ApplicationError({ kind: "invalid-input", message: "Invalid Signal reaction" }),
    ),
  );
  if (input.target !== path)
    return yield* new ApplicationError({
      kind: "invalid-input",
      message: "Signal reaction target mismatch",
    });
  const current = registry.get(path);
  if (!current)
    return yield* new ApplicationError({ kind: "not-found", message: "Signal unavailable" });
  const state = yield* Schema.decodeUnknownEffect(ReactionState)(current.state).pipe(Effect.orDie);
  const previous = state.reactionReceipts?.find((item) => item.input.requestId === input.requestId);
  if (previous) {
    if (!isDeepStrictEqual(previous.input, input))
      return yield* new ApplicationError({
        kind: "conflict",
        message: "Signal reaction ID belongs to another input",
      });
    return previous.receipt;
  }
  if (!sourceSignalEligible(state, (slug) => registry.get(`/goals/${slug}`)))
    return yield* new ApplicationError({
      kind: "conflict",
      message: "Signal no longer accepts source reactions",
    });
  const receipt = { requestId: input.requestId, revision: (current.revision ?? 0) + 1 };
  const next = {
    ...state,
    reactionReceipts: [...(state.reactionReceipts ?? []), { input, receipt }],
    occurrences: [
      ...(state.occurrences ?? []),
      {
        message: {
          requestId: `${path}:reaction:${input.requestId}`,
          source: path,
          task: state.task,
          createdAt: new Date(yield* Clock.currentTimeMillis).toISOString(),
          evidence: input.sourceContext,
          causal: state.causal ?? { rootRequestId: input.causationId, remainingAgentTurns: 4 },
        },
        delivered: false,
      },
    ],
  };
  yield* registry
    .commit(
      {
        ...current,
        state: {
          ...current.state,
          ...next,
        },
      },
      { expectedRevision: input.expectedRevision },
    )
    .pipe(
      Effect.catchTag("ContextConflict", () =>
        Effect.fail(
          new ApplicationError({
            kind: "conflict",
            message: "Signal changed after screening; evaluate its current revision",
          }),
        ),
      ),
      Effect.catchTag("ContextCommitError", Effect.die),
      Effect.catchTag("ContextValidationError", Effect.die),
    );
  return receipt;
});
