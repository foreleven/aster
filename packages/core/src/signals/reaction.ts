import {
  ApplicationError,
  BusinessNotification,
  CausalChain,
  CommandReceipt,
  PublicContext,
} from "@aster/api-contracts";
import { Clock, Effect, Schema } from "effect";
import { isDeepStrictEqual } from "node:util";
import type { ContextRegistry } from "../context/registry.js";
import { SignalDefinition } from "../config/schema.js";
import { sourceSignalEligible } from "./policy.js";
import { signalNotifications } from "../notifications/signal.js";

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

const ReactionState = Schema.Struct({
  businessOutbox: Schema.optional(Schema.Array(BusinessNotification)),
  causal: Schema.optional(CausalChain),
  ...SignalDefinition.fields,
  goal: Schema.optional(Schema.String),
  active: Schema.optional(Schema.Boolean),
  deleted: Schema.optional(Schema.Boolean),
  reactionReceipts: Schema.optional(Schema.Array(SignalReactionReceipt)),
  occurrences: Schema.optional(
    Schema.Array(
      Schema.Struct({
        causal: Schema.optional(CausalChain),
        id: Schema.String,
        text: Schema.String,
        delivered: Schema.Boolean,
        source: PublicContext,
      }),
    ),
  ),
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
  if (
    !sourceSignalEligible(state, yield* Clock.currentTimeMillis, (slug) =>
      registry.get(`/goals/${slug}`),
    )
  )
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
        id: `${path}:reaction:${input.requestId}`,
        text: `Condition: ${state.when}\nRelated task: ${state.taskId ?? "None"}\nSource: ${input.sourceContext.path}\nRead the matched source snapshot before deciding whether work is needed.`,
        source: input.sourceContext,
        causal: state.causal ?? { rootRequestId: input.causationId, remainingAgentTurns: 4 },
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
          businessOutbox: signalNotifications({
            path,
            revision: receipt.revision,
            at: new Date(yield* Clock.currentTimeMillis).toISOString(),
            previous: state,
            next,
          }),
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
