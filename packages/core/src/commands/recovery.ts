import { isDeepStrictEqual } from "node:util";
import { Effect, Option } from "effect";
import { ApplicationError, type RecoveryInput, type RecoveryReceipt } from "@aster/api-contracts";

/** Mailbox-only preflight. The owner commits the returned authorization with its domain transition. */
export const recoveryReplay = Effect.fn("Recovery.replay")(function* (
  input: RecoveryInput,
  revision: number,
  receipts: readonly RecoveryReceipt[],
) {
  const prior = receipts.find((entry) => entry.input.requestId === input.requestId);
  if (prior) {
    if (!isDeepStrictEqual(prior.input, input))
      return yield* new ApplicationError({
        kind: "conflict",
        message: "Recovery request identity belongs to another payload",
      });
    return Option.some(prior.receipt);
  }
  if (revision !== input.expectedRevision)
    return yield* new ApplicationError({
      kind: "conflict",
      message: "Processing state changed; refresh before authorizing recovery",
    });
  return Option.none();
});
